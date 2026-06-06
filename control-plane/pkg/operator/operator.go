package operator

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type MasterSpec struct {
	GrpcPort int
	HttpPort int
}

type WorkerSpec struct {
	Replicas         int
	FlightPortStart  int
	ControlPortStart int
}

type JobSpec struct {
	SQL           string
	MapSQL        string
	ReduceSQL     string
	InputFiles    []string
	NumPartitions int32
	OutputDir     string
}

type ApplicationSpec struct {
	Master  MasterSpec
	Workers WorkerSpec
	Job     JobSpec
}

type Operator struct {
	spec      *ApplicationSpec
	masterCmd *exec.Cmd
	workers   []*exec.Cmd
	stopChan  chan struct{}
	logDir    string
	mu        sync.Mutex
}

// NewOperator reads and parses the configuration YAML file using a standard-library parser.
func NewOperator(configPath string) (*Operator, error) {
	spec, err := parseYAML(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse operator config: %w", err)
	}

	// Validate configuration spec
	if spec.Master.GrpcPort == 0 {
		spec.Master.GrpcPort = 50050
	}
	if spec.Master.HttpPort == 0 {
		spec.Master.HttpPort = 8080
	}
	if spec.Workers.Replicas == 0 {
		spec.Workers.Replicas = 2
	}
	if spec.Workers.FlightPortStart == 0 {
		spec.Workers.FlightPortStart = 50060
	}
	if spec.Workers.ControlPortStart == 0 {
		spec.Workers.ControlPortStart = 50070
	}

	logDir := "./operator-logs"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}

	return &Operator{
		spec:     spec,
		workers:  make([]*exec.Cmd, spec.Workers.Replicas),
		stopChan: make(chan struct{}),
		logDir:   logDir,
	}, nil
}

func parseYAML(path string) (*ApplicationSpec, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	spec := &ApplicationSpec{}
	scanner := bufio.NewScanner(file)
	inInputFiles := false

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		if strings.HasPrefix(line, "inputFiles:") {
			inInputFiles = true
			continue
		}

		if inInputFiles {
			if strings.HasPrefix(line, "-") {
				val := strings.TrimPrefix(line, "-")
				val = strings.TrimSpace(val)
				val = strings.Trim(val, `"'`)
				spec.Job.InputFiles = append(spec.Job.InputFiles, val)
				continue
			} else if strings.Contains(line, ":") {
				inInputFiles = false
			}
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) < 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		val = strings.Trim(val, `"'`)

		switch key {
		case "grpcPort":
			spec.Master.GrpcPort, _ = strconv.Atoi(val)
		case "httpPort":
			spec.Master.HttpPort, _ = strconv.Atoi(val)
		case "replicas":
			spec.Workers.Replicas, _ = strconv.Atoi(val)
		case "flightPortStart":
			spec.Workers.FlightPortStart, _ = strconv.Atoi(val)
		case "controlPortStart":
			spec.Workers.ControlPortStart, _ = strconv.Atoi(val)
		case "sql":
			spec.Job.SQL = val
		case "mapSql":
			spec.Job.MapSQL = val
		case "reduceSql":
			spec.Job.ReduceSQL = val
		case "numPartitions":
			num, _ := strconv.Atoi(val)
			spec.Job.NumPartitions = int32(num)
		case "outputDir":
			spec.Job.OutputDir = val
		}
	}

	return spec, scanner.Err()
}

func (op *Operator) Run(ctx context.Context) error {
	defer op.Cleanup()

	// 1. Start Master
	if err := op.startMaster(); err != nil {
		return err
	}

	// Give master a brief moment to bind to ports
	time.Sleep(2 * time.Second)

	// 2. Start Workers with Self-Healing Monitoring
	for i := 0; i < op.spec.Workers.Replicas; i++ {
		dataDir := filepath.Join(os.TempDir(), fmt.Sprintf("data-dir-operator-worker-%d", i+1))
		_ = os.RemoveAll(dataDir)
		_ = os.MkdirAll(dataDir, 0755)

		go op.monitorWorker(i, dataDir)
	}

	// Wait for workers to register and heartbeat
	log.Println("[Operator] Cluster starting. Waiting for workers to register...")
	time.Sleep(4 * time.Second)

	// 3. Submit Job
	jobID, err := op.submitJob()
	if err != nil {
		return fmt.Errorf("failed to submit job: %w", err)
	}

	log.Printf("[Operator] Job %s submitted successfully. Monitoring progress...", jobID)

	// 4. Poll Job Status
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			status, err := op.getJobStatus(jobID)
			if err != nil {
				log.Printf("[Operator] Error checking job status: %v", err)
				continue
			}

			// Check queue depth to trigger scale down
			queueDepth, errQD := op.getQueueDepth()
			if errQD == nil && queueDepth == 0 {
				op.scaleDown()
			}

			log.Printf("[Operator] Job %s Status: %s (queue depth: %d)", jobID, status, queueDepth)
			if status == "COMPLETED" {
				log.Println("[Operator] Job completed successfully!")
				return nil
			}
			if status == "FAILED" {
				return fmt.Errorf("job %s failed during execution", jobID)
			}
		}
	}
}

func (op *Operator) startMaster() error {
	log.Printf("[Operator] Spawning Master process on HTTP:%d and gRPC:%d...", op.spec.Master.HttpPort, op.spec.Master.GrpcPort)
	
	cmd := exec.Command("./control-plane-master",
		fmt.Sprintf("--grpc-port=%d", op.spec.Master.GrpcPort),
		fmt.Sprintf("--http-port=%d", op.spec.Master.HttpPort),
	)

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to get stdout pipe: %w", err)
	}
	cmd.Stderr = cmd.Stdout

	logFile, err := os.Create(filepath.Join(op.logDir, "master.log"))
	if err != nil {
		return fmt.Errorf("failed to create master log: %w", err)
	}

	op.masterCmd = cmd
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return fmt.Errorf("failed to start master: %w", err)
	}

	// Read output in background
	go func() {
		defer logFile.Close()
		multiWriter := io.MultiWriter(logFile, os.Stdout)
		
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			line := scanner.Text()
			_, _ = fmt.Fprintln(multiWriter, line)

			if strings.Contains(line, "[SCALE_OUT_WORKERS]") {
				op.scaleUp()
			}
		}
	}()

	return nil
}

func (op *Operator) monitorWorker(idx int, dataDir string) {
	for {
		select {
		case <-op.stopChan:
			return
		default:
		}

		log.Printf("[Operator] Spawning worker-%d process...", idx+1)
		cmd := exec.Command("/home/neutrino/oxidestream/data-plane/target/debug/data-plane",
			fmt.Sprintf("--worker-id=worker-%d", idx+1),
			"--host=127.0.0.1",
			fmt.Sprintf("--control-port=%d", op.spec.Workers.ControlPortStart+idx),
			fmt.Sprintf("--flight-port=%d", op.spec.Workers.FlightPortStart+idx),
			fmt.Sprintf("--master-address=http://127.0.0.1:%d", op.spec.Master.GrpcPort),
			fmt.Sprintf("--data-dir=%s", dataDir),
		)

		logFile, err := os.Create(filepath.Join(op.logDir, fmt.Sprintf("worker-%d.log", idx+1)))
		if err == nil {
			cmd.Stdout = logFile
			cmd.Stderr = logFile
		}

		op.mu.Lock()
		op.workers[idx] = cmd
		op.mu.Unlock()

		err = cmd.Start()
		if err != nil {
			log.Printf("[Operator] Failed to start worker-%d: %v. Retrying in 2s...", idx+1, err)
			if logFile != nil {
				logFile.Close()
			}
			time.Sleep(2 * time.Second)
			continue
		}

		_ = cmd.Wait()
		if logFile != nil {
			logFile.Close()
		}

		select {
		case <-op.stopChan:
			return
		default:
			log.Printf("[Operator] Worker-%d exited unexpectedly. Automatically re-spawning (Self-Healing)...", idx+1)
			time.Sleep(1 * time.Second)
		}
	}
}

func (op *Operator) submitJob() (string, error) {
	payload := map[string]interface{}{
		"sql":            op.spec.Job.SQL,
		"map_sql":        op.spec.Job.MapSQL,
		"reduce_sql":     op.spec.Job.ReduceSQL,
		"input_files":    op.spec.Job.InputFiles,
		"num_partitions": op.spec.Job.NumPartitions,
		"output_dir":     op.spec.Job.OutputDir,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	url := fmt.Sprintf("http://localhost:%d/submit", op.spec.Master.HttpPort)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("http error %d: %s", resp.StatusCode, string(body))
	}

	var submitResp map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&submitResp); err != nil {
		return "", err
	}

	return submitResp["job_id"], nil
}

func (op *Operator) getJobStatus(jobID string) (string, error) {
	url := fmt.Sprintf("http://localhost:%d/status?job_id=%s", op.spec.Master.HttpPort, jobID)
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("http status %d", resp.StatusCode)
	}

	var statusResp map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&statusResp); err != nil {
		return "", err
	}

	return statusResp["status"], nil
}

func (op *Operator) Cleanup() {
	op.mu.Lock()
	defer op.mu.Unlock()

	select {
	case <-op.stopChan:
		return
	default:
		close(op.stopChan)
	}

	log.Println("[Operator] Cleaning up master and worker processes...")

	for i, cmd := range op.workers {
		if cmd != nil && cmd.Process != nil {
			log.Printf("[Operator] Killing worker-%d...", i+1)
			_ = cmd.Process.Kill()
		}
	}

	if op.masterCmd != nil && op.masterCmd.Process != nil {
		log.Println("[Operator] Killing Master...")
		_ = op.masterCmd.Process.Kill()
	}
}

func (op *Operator) scaleUp() {
	op.mu.Lock()
	defer op.mu.Unlock()

	if len(op.workers) > op.spec.Workers.Replicas {
		return // already scaled up
	}

	log.Println("[Operator] SCALE_OUT_WORKERS detected! Scaling up cluster: spawning 2 extra workers...")

	extraReplicas := 2
	totalCount := op.spec.Workers.Replicas + extraReplicas
	if len(op.workers) < totalCount {
		newWorkers := make([]*exec.Cmd, totalCount)
		copy(newWorkers, op.workers)
		op.workers = newWorkers
	}

	for i := 0; i < extraReplicas; i++ {
		idx := op.spec.Workers.Replicas + i
		dataDir := filepath.Join(os.TempDir(), fmt.Sprintf("data-dir-operator-worker-extra-%d", i+1))
		_ = os.RemoveAll(dataDir)
		_ = os.MkdirAll(dataDir, 0755)

		go op.monitorExtraWorker(idx, dataDir)
	}
}

func (op *Operator) monitorExtraWorker(idx int, dataDir string) {
	for {
		select {
		case <-op.stopChan:
			return
		default:
		}

		log.Printf("[Operator] Spawning extra worker-%d process...", idx+1)
		cmd := exec.Command("/home/neutrino/oxidestream/data-plane/target/debug/data-plane",
			fmt.Sprintf("--worker-id=worker-extra-%d", idx+1),
			"--host=127.0.0.1",
			fmt.Sprintf("--control-port=%d", op.spec.Workers.ControlPortStart+idx),
			fmt.Sprintf("--flight-port=%d", op.spec.Workers.FlightPortStart+idx),
			fmt.Sprintf("--master-address=http://127.0.0.1:%d", op.spec.Master.GrpcPort),
			fmt.Sprintf("--data-dir=%s", dataDir),
		)

		logFile, err := os.Create(filepath.Join(op.logDir, fmt.Sprintf("worker-extra-%d.log", idx+1)))
		if err == nil {
			cmd.Stdout = logFile
			cmd.Stderr = logFile
		}

		op.mu.Lock()
		op.workers[idx] = cmd
		op.mu.Unlock()

		err = cmd.Start()
		if err != nil {
			log.Printf("[Operator] Failed to start extra worker-%d: %v", idx+1, err)
			if logFile != nil {
				logFile.Close()
			}
			time.Sleep(2 * time.Second)
			continue
		}

		_ = cmd.Wait()
		if logFile != nil {
			logFile.Close()
		}

		select {
		case <-op.stopChan:
			return
		default:
			op.mu.Lock()
			shouldRestart := idx < len(op.workers) && op.workers[idx] != nil
			op.mu.Unlock()

			if !shouldRestart {
				return
			}
			log.Printf("[Operator] Extra worker-%d exited. Restarting (Self-Healing)...", idx+1)
			time.Sleep(1 * time.Second)
		}
	}
}

func (op *Operator) scaleDown() {
	op.mu.Lock()
	defer op.mu.Unlock()

	if len(op.workers) <= op.spec.Workers.Replicas {
		return
	}

	log.Println("[Operator] Queue empty. Scaling down cluster: terminating extra workers...")

	for i := op.spec.Workers.Replicas; i < len(op.workers); i++ {
		cmd := op.workers[i]
		if cmd != nil && cmd.Process != nil {
			log.Printf("[Operator] Terminating extra worker-%d...", i+1)
			_ = cmd.Process.Kill()
		}
		op.workers[i] = nil
	}

	op.workers = op.workers[:op.spec.Workers.Replicas]
}

func (op *Operator) getQueueDepth() (int, error) {
	url := fmt.Sprintf("http://localhost:%d/queue_depth", op.spec.Master.HttpPort)
	resp, err := http.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var res map[string]int
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return 0, err
	}
	return res["pending_tasks"], nil
}
