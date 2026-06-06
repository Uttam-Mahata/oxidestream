package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"control-plane/pkg/operator"
	pb "control-plane/pkg/proto"
	"control-plane/pkg/raft"
	"control-plane/pkg/scheduler"
	"control-plane/pkg/worker"
	"google.golang.org/grpc"
)

type grpcServer struct {
	pb.UnimplementedControlPlaneServer
	store     *raft.MetadataStore
	scheduler *scheduler.Scheduler
}

func (s *grpcServer) RegisterWorker(ctx context.Context, req *pb.RegisterWorkerRequest) (*pb.RegisterWorkerResponse, error) {
	log.Printf("Registering worker: ID=%s, Host=%s, ControlPort=%d, FlightPort=%d",
		req.WorkerId, req.Host, req.ControlPort, req.FlightPort)

	err := s.store.RegisterWorker(&raft.WorkerConfig{
		WorkerID:      req.WorkerId,
		Host:          req.Host,
		ControlPort:   req.ControlPort,
		FlightPort:    req.FlightPort,
		NumCores:      req.NumCores,
		TotalMemoryMB: req.TotalMemoryMb,
		LastActive:    time.Now(),
		Active:        true,
	})
	if err != nil {
		return &pb.RegisterWorkerResponse{Success: false, Message: err.Error()}, nil
	}
	return &pb.RegisterWorkerResponse{Success: true, Message: "Worker registered successfully"}, nil
}

func (s *grpcServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	err := s.store.UpdateWorkerHeartbeat(req.WorkerId, req.CpuUsagePct, req.MemoryUsedMb)
	if err != nil {
		// Log but return failure response instead of gRPC error
		log.Printf("Heartbeat failed for unregistered worker: %s", req.WorkerId)
		return &pb.HeartbeatResponse{Success: false}, nil
	}
	return &pb.HeartbeatResponse{Success: true}, nil
}

func (s *grpcServer) UpdateTaskStatus(ctx context.Context, req *pb.TaskStatusUpdateRequest) (*pb.TaskStatusUpdateResponse, error) {
	err := s.scheduler.HandleTaskStatusUpdate(
		req.TaskId,
		req.StageId,
		req.WorkerId,
		req.Status,
		req.ErrorMessage,
		req.ProducedPartitions,
		req.TableStats,
	)
	if err != nil {
		log.Printf("Error updating task status: %v", err)
		return &pb.TaskStatusUpdateResponse{Success: false}, nil
	}
	return &pb.TaskStatusUpdateResponse{Success: true}, nil
}

func runServer(grpcPort int, httpPort int) {
	log.Printf("Starting OxideStream Control Plane (Master)...")

	// 1. Initialize Raft Metadata Store
	store := raft.NewMetadataStore("master-node")

	// 2. Initialize Scheduler
	sched := scheduler.NewScheduler(store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go sched.Start(ctx)

	// 3. Initialize Worker Tracker
	// Timeout set to 10 seconds.
	tracker := worker.NewTracker(store, 10*time.Second, func(workerID string) {
		sched.HandleWorkerFailure(workerID)
	})
	go tracker.Start(ctx)
	defer tracker.Stop()

	// 4. Start gRPC server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		log.Fatalf("Failed to listen on gRPC port %d: %v", grpcPort, err)
	}

	grpcServerInstance := grpc.NewServer()
	pb.RegisterControlPlaneServer(grpcServerInstance, &grpcServer{
		store:     store,
		scheduler: sched,
	})

	go func() {
		log.Printf("gRPC Control Plane server listening on port %d", grpcPort)
		if err := grpcServerInstance.Serve(lis); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	// 5. REST HTTP Server configuration
	mux := http.NewServeMux()

	mux.HandleFunc("/submit", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			SQL           string   `json:"sql"`
			MapSQL        string   `json:"map_sql"`
			ReduceSQL     string   `json:"reduce_sql"`
			InputFiles    []string `json:"input_files"`
			NumPartitions int32    `json:"num_partitions"`
			OutputDir     string   `json:"output_dir"`
			// DPP fields
			DppDimFile   string `json:"dpp_dim_file"`
			DppFilterCol string `json:"dpp_filter_col"`
			DppFilterVal string `json:"dpp_filter_val"`
			DppJoinKey   string `json:"dpp_join_key"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if (req.SQL == "" && (req.MapSQL == "" || req.ReduceSQL == "")) || len(req.InputFiles) == 0 || req.NumPartitions <= 0 || req.OutputDir == "" {
			http.Error(w, "Missing required fields: sql (or map_sql and reduce_sql), input_files, num_partitions, output_dir", http.StatusBadRequest)
			return
		}

		// Apply DPP optimization if requested
		if req.DppDimFile != "" && req.DppFilterCol != "" && req.DppFilterVal != "" && req.DppJoinKey != "" {
			if req.MapSQL != "" {
				modified, err := scheduler.ApplyDPP(req.MapSQL, req.DppDimFile, req.DppFilterCol, req.DppFilterVal, req.DppJoinKey)
				if err == nil {
					req.MapSQL = modified
				} else {
					log.Printf("Failed to apply DPP: %v", err)
				}
			} else if req.SQL != "" {
				modified, err := scheduler.ApplyDPP(req.SQL, req.DppDimFile, req.DppFilterCol, req.DppFilterVal, req.DppJoinKey)
				if err == nil {
					req.SQL = modified
				} else {
					log.Printf("Failed to apply DPP: %v", err)
				}
			}
		}

		var job *scheduler.Job
		var err error
		if req.MapSQL != "" && req.ReduceSQL != "" {
			job, err = sched.SubmitMapReduceJob(req.MapSQL, req.ReduceSQL, req.InputFiles, req.NumPartitions, req.OutputDir)
		} else {
			job, err = sched.SubmitSQLJob(req.SQL, req.InputFiles, req.NumPartitions, req.OutputDir)
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"job_id": job.JobID,
			"status": job.Status,
		})
	})

	mux.HandleFunc("/submit_lr", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			InputFiles    []string `json:"input_files"`
			NumPartitions int32    `json:"num_partitions"`
			OutputDir     string   `json:"output_dir"`
			Iterations    int      `json:"iterations"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(req.InputFiles) == 0 || req.NumPartitions <= 0 || req.OutputDir == "" {
			http.Error(w, "Missing required fields", http.StatusBadRequest)
			return
		}
		if req.Iterations == 0 {
			req.Iterations = 2
		}
		go func() {
			err := sched.SubmitLinearRegressionJob(ctx, req.InputFiles, req.NumPartitions, req.OutputDir, req.Iterations)
			if err != nil {
				log.Printf("Linear Regression job failed: %v", err)
			}
		}()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"message": "Linear Regression job started"})
	})

	mux.HandleFunc("/submit_pagerank", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			InputFiles    []string `json:"input_files"`
			NumPartitions int32    `json:"num_partitions"`
			OutputDir     string   `json:"output_dir"`
			Iterations    int      `json:"iterations"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(req.InputFiles) == 0 || req.NumPartitions <= 0 || req.OutputDir == "" {
			http.Error(w, "Missing required fields", http.StatusBadRequest)
			return
		}
		if req.Iterations == 0 {
			req.Iterations = 2
		}
		go func() {
			err := sched.SubmitPageRankJob(ctx, req.InputFiles, req.NumPartitions, req.OutputDir, req.Iterations)
			if err != nil {
				log.Printf("PageRank job failed: %v", err)
			}
		}()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"message": "PageRank job started"})
	})

	mux.HandleFunc("/submit_streaming", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			InputDir       string `json:"input_dir"`
			CheckpointFile string `json:"checkpoint_file"`
			MapSQL         string `json:"map_sql"`
			ReduceSQL      string `json:"reduce_sql"`
			NumPartitions  int32  `json:"num_partitions"`
			OutputDir      string `json:"output_dir"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.InputDir == "" || req.CheckpointFile == "" || req.MapSQL == "" || req.ReduceSQL == "" || req.NumPartitions <= 0 || req.OutputDir == "" {
			http.Error(w, "Missing required fields", http.StatusBadRequest)
			return
		}
		go func() {
			streamSched := scheduler.NewStreamingScheduler(sched, req.InputDir, req.CheckpointFile, "", req.MapSQL, req.ReduceSQL, req.NumPartitions, req.OutputDir)
			streamSched.Start(ctx)
		}()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"message": "Streaming job started"})
	})

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		jobID := r.URL.Query().Get("job_id")
		if jobID == "" {
			http.Error(w, "Missing job_id query parameter", http.StatusBadRequest)
			return
		}
		status, err := sched.GetJobStatus(jobID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"job_id": jobID,
			"status": status,
		})
	})

	mux.HandleFunc("/workers", func(w http.ResponseWriter, r *http.Request) {
		workers := store.GetAllWorkers()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(workers)
	})

	log.Printf("REST HTTP API server listening on port %d", httpPort)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", httpPort), mux); err != nil {
		log.Fatalf("HTTP server failed: %v", err)
	}
}

func runClient(httpPort int, sql string, inputs string, partitions int, output string) {
	inputList := strings.Split(inputs, ",")
	for i := range inputList {
		inputList[i] = strings.TrimSpace(inputList[i])
	}

	reqBody, err := json.Marshal(map[string]interface{}{
		"sql":            sql,
		"input_files":    inputList,
		"num_partitions": partitions,
		"output_dir":     output,
	})
	if err != nil {
		log.Fatalf("Failed to marshal request: %v", err)
	}

	url := fmt.Sprintf("http://localhost:%d/submit", httpPort)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		log.Fatalf("Failed to submit job to Master REST API: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		log.Fatalf("Error submitting job (status %d): %s", resp.StatusCode, string(body))
	}

	var submitResp map[string]string
	if err := json.Unmarshal(body, &submitResp); err != nil {
		log.Fatalf("Failed to decode response: %v", err)
	}

	jobID := submitResp["job_id"]
	fmt.Printf("Job submitted successfully! Job ID: %s\n", jobID)
	fmt.Println("Waiting for job to complete...")

	// Poll status
	statusUrl := fmt.Sprintf("http://localhost:%d/status?job_id=%s", httpPort, jobID)
	for {
		time.Sleep(1 * time.Second)
		sresp, err := http.Get(statusUrl)
		if err != nil {
			log.Printf("Error polling status: %v", err)
			continue
		}

		sbody, err := io.ReadAll(sresp.Body)
		sresp.Body.Close()
		if err != nil {
			log.Printf("Failed to read status response body: %v", err)
			continue
		}

		var statusResp map[string]string
		if err := json.Unmarshal(sbody, &statusResp); err != nil {
			log.Printf("Failed to decode status response: %v", err)
			continue
		}

		status := statusResp["status"]
		fmt.Printf("[%s] Job Status: %s\n", time.Now().Format("15:04:05"), status)
		if status == "COMPLETED" {
			fmt.Println("Job completed successfully!")
			break
		} else if status == "FAILED" {
			log.Fatalf("Job failed!")
		}
	}
}

func main() {
	grpcPort := flag.Int("grpc-port", 50050, "Port for the gRPC Control Plane server")
	httpPort := flag.Int("http-port", 8080, "Port for the REST HTTP API server")
	submitJob := flag.Bool("submit", false, "Submit a SQL job to the control plane")
	sql := flag.String("sql", "", "SQL query to execute (for submit)")
	inputs := flag.String("inputs", "", "Comma-separated list of input CSV files (for submit)")
	partitions := flag.Int("partitions", 1, "Number of partitions for reduce stage (for submit)")
	output := flag.String("output", "", "Output directory (for submit)")
	operatorConfig := flag.String("operator", "", "Path to the OxideStreamApplication YAML configuration for the operator simulator")

	flag.Parse()

	if *operatorConfig != "" {
		op, err := operator.NewOperator(*operatorConfig)
		if err != nil {
			log.Fatalf("Failed to initialize operator: %v", err)
		}
		ctx := context.Background()
		if err := op.Run(ctx); err != nil {
			log.Fatalf("Operator failed: %v", err)
		}
		os.Exit(0)
	}

	if *submitJob {
		if *sql == "" || *inputs == "" || *output == "" {
			fmt.Println("Error: --sql, --inputs, and --output must be specified when using --submit")
			flag.Usage()
			os.Exit(1)
		}
		runClient(*httpPort, *sql, *inputs, *partitions, *output)
	} else {
		runServer(*grpcPort, *httpPort)
	}
}
