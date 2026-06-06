package scheduler

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	pb "control-plane/pkg/proto"
	"control-plane/pkg/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TaskStatus indicates the status of a specific task.
type TaskStatus string

const (
	TaskPending   TaskStatus = "PENDING"
	TaskRunning   TaskStatus = "RUNNING"
	TaskCompleted TaskStatus = "COMPLETED"
	TaskFailed    TaskStatus = "FAILED"
)

// TaskState maintains the execution state of an individual task.
type TaskState struct {
	TaskID              string
	StageID             string
	StageType           pb.StageType
	WorkerID            string
	InputFiles          []string
	OutputPartitionID   int32
	Status              TaskStatus
	ErrorMsg            string
	LastUpdated         time.Time
	BroadcastFiles      []string
	BroadcastTableNames []string
	CoalescedPartitions []int32
	// A5: Speculative execution fields
	StartTime     time.Time
	IsSpeculative bool
	SpeculativeOf string // non-empty only when IsSpeculative; holds the original task's ID
	// A6: Per-task SQL override (used for skew-split reduce tasks)
	ReduceSQLOverride string
}

// Job encapsulates a submitted SQL query and its execution progress.
type Job struct {
	JobID         string
	SQL           string
	MapSQL        string
	ReduceSQL     string
	InputFiles    []string
	NumPartitions int32
	OutputDir     string
	MapTasks      map[string]*TaskState
	ReduceTasks   map[string]*TaskState
	Status        string // PENDING, RUNNING, COMPLETED, FAILED
	mu            sync.Mutex
}

// Scheduler manages job execution DAGs (Map -> Reduce) and worker task scheduling.
type Scheduler struct {
	store     *raft.MetadataStore
	jobs      map[string]*Job
	jobQueue  []*Job
	mu        sync.Mutex
	jobCond   *sync.Cond
	fairSched *FairScheduler
}

// NewScheduler creates a new DAG Scheduler.
func NewScheduler(store *raft.MetadataStore) *Scheduler {
	s := &Scheduler{
		store:     store,
		jobs:      make(map[string]*Job),
		jobQueue:  make([]*Job, 0),
		fairSched: NewFairScheduler(8),
	}
	s.jobCond = sync.NewCond(&s.mu)
	return s
}

// SubmitSQLJob adds a new job to the scheduling queue.
func (s *Scheduler) SubmitSQLJob(sql string, inputFiles []string, numPartitions int32, outputDir string) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	jobID := fmt.Sprintf("job-%d", time.Now().UnixNano())
	job := &Job{
		JobID:         jobID,
		SQL:           sql,
		InputFiles:    inputFiles,
		NumPartitions: numPartitions,
		OutputDir:     outputDir,
		Status:        "PENDING",
	}

	s.jobs[jobID] = job
	s.jobQueue = append(s.jobQueue, job)
	log.Printf("Submitted SQL job %s with %d files. Queue depth: %d", jobID, len(inputFiles), len(s.jobQueue))
	s.jobCond.Signal()
	return job, nil
}

// SubmitMapReduceJob adds a new job with separate map and reduce SQL queries to the scheduling queue.
func (s *Scheduler) SubmitMapReduceJob(mapSQL string, reduceSQL string, inputFiles []string, numPartitions int32, outputDir string) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	jobID := fmt.Sprintf("job-%d", time.Now().UnixNano())
	job := &Job{
		JobID:         jobID,
		SQL:           mapSQL,
		MapSQL:        mapSQL,
		ReduceSQL:     reduceSQL,
		InputFiles:    inputFiles,
		NumPartitions: numPartitions,
		OutputDir:     outputDir,
		Status:        "PENDING",
	}

	s.jobs[jobID] = job
	s.jobQueue = append(s.jobQueue, job)
	log.Printf("Submitted MapReduce job %s with %d files. Queue depth: %d", jobID, len(inputFiles), len(s.jobQueue))
	s.jobCond.Signal()
	return job, nil
}

// GetJobStatus retrieves the current execution status of a job.
func (s *Scheduler) GetJobStatus(jobID string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, exists := s.jobs[jobID]
	if !exists {
		return "", fmt.Errorf("job %s not found", jobID)
	}
	job.mu.Lock()
	status := job.Status
	job.mu.Unlock()
	return status, nil
}

// WaitForJob blocks until the specified job reaches COMPLETED or FAILED.
func (s *Scheduler) WaitForJob(jobID string) (string, error) {
	for {
		status, err := s.GetJobStatus(jobID)
		if err != nil {
			return "", err
		}
		if status == "COMPLETED" || status == "FAILED" {
			return status, nil
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// Start runs the main scheduler loop, consuming submitted jobs.
func (s *Scheduler) Start(ctx context.Context) {
	go s.startSpeculativeMonitor(ctx)
	log.Println("Scheduler loop started.")
	for {
		s.mu.Lock()
		for len(s.jobQueue) == 0 {
			select {
			case <-ctx.Done():
				s.mu.Unlock()
				return
			default:
			}
			s.jobCond.Wait()
		}

		select {
		case <-ctx.Done():
			s.mu.Unlock()
			return
		default:
		}

		job := s.jobQueue[0]
		s.jobQueue = s.jobQueue[1:]
		s.mu.Unlock()

		// Run jobs concurrently using the FairScheduler slots!
		go func(job *Job) {
			s.fairSched.RegisterJob(job.JobID, job)
			defer s.fairSched.UnregisterJob(job.JobID)

			err := s.runJob(ctx, job)

			job.mu.Lock()
			if err != nil {
				log.Printf("Job %s failed: %v", job.JobID, err)
				job.Status = "FAILED"
			} else {
				log.Printf("Job %s completed successfully", job.JobID)
				job.Status = "COMPLETED"
			}
			job.mu.Unlock()
		}(job)
	}
}

func (s *Scheduler) runJob(ctx context.Context, job *Job) error {
	log.Printf("Running job %s, SQL: %q", job.JobID, job.SQL)
	job.mu.Lock()
	job.Status = "RUNNING"
	s.store.ClearPartitions()

	// Check if this is an ANALYZE TABLE query
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(job.SQL)), "ANALYZE TABLE") {
		log.Printf("CBO: Job %s is an ANALYZE TABLE query", job.JobID)
		job.MapTasks = make(map[string]*TaskState)
		taskID := fmt.Sprintf("%s-analyze", job.JobID)
		job.MapTasks[taskID] = &TaskState{
			TaskID:      taskID,
			StageID:     "map-stage",
			StageType:   pb.StageType_STAGE_TYPE_MAP,
			InputFiles:  job.InputFiles,
			Status:      TaskPending,
			LastUpdated: time.Now(),
		}
		job.mu.Unlock()

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			job.mu.Lock()
			allDone := true
			anyFailed := false
			var errMsg string

			for _, t := range job.MapTasks {
				if t.Status != TaskCompleted {
					allDone = false
				}
				if t.Status == TaskFailed {
					anyFailed = true
					errMsg = t.ErrorMsg
				}
			}

			if anyFailed {
				job.mu.Unlock()
				return fmt.Errorf("analyze job failed: %s", errMsg)
			}
			if allDone {
				job.mu.Unlock()
				break
			}

			s.dispatchPendingTasks(ctx, job, job.MapTasks)
			job.mu.Unlock()
			time.Sleep(500 * time.Millisecond)
		}
		return nil
	}

	// 1. Initialize Map Tasks with CBO (Query Planning for Broadcast Joins)
	job.MapTasks = make(map[string]*TaskState)
	var mainDatasets []string
	var broadcastFiles []string
	var broadcastTableNames []string

	for _, file := range job.InputFiles {
		base := filepath.Base(file)
		ext := filepath.Ext(base)
		tableName := strings.TrimSuffix(base, ext)

		size := int64(0)
		stats, errStats := s.store.GetTableStats(tableName)
		if errStats == nil {
			if stats.SizeBytes < 50000 || stats.RowCount < 1000 {
				// Convert to broadcast join (force size below threshold)
				size = 0
			} else {
				size = stats.SizeBytes
			}
			log.Printf("CBO: Using stats for table %s: row_count=%d, size_bytes=%d -> size used=%d", tableName, stats.RowCount, stats.SizeBytes, size)
		} else {
			fi, err := os.Stat(file)
			if err == nil {
				size = fi.Size()
			} else {
				// Fallback: if we cannot stat the file, assume it is a main dataset
				size = 1000000
			}
		}

		if size < 50000 {
			broadcastFiles = append(broadcastFiles, file)
			broadcastTableNames = append(broadcastTableNames, tableName)
		} else {
			mainDatasets = append(mainDatasets, file)
		}
	}

	// Fallback: if all input files are below the threshold, treat the largest as the main dataset
	if len(mainDatasets) == 0 && len(broadcastFiles) > 0 {
		largestIdx := 0
		largestSize := int64(-1)
		for i, file := range broadcastFiles {
			fi, err := os.Stat(file)
			if err == nil && fi.Size() > largestSize {
				largestSize = fi.Size()
				largestIdx = i
			}
		}
		mainFile := broadcastFiles[largestIdx]
		mainDatasets = append(mainDatasets, mainFile)
		broadcastFiles = append(broadcastFiles[:largestIdx], broadcastFiles[largestIdx+1:]...)
		broadcastTableNames = append(broadcastTableNames[:largestIdx], broadcastTableNames[largestIdx+1:]...)
	}

	log.Printf("CBO: Job %s - Main datasets: %v, Broadcast files: %v", job.JobID, mainDatasets, broadcastFiles)

	for i, file := range mainDatasets {
		taskID := fmt.Sprintf("%s-map-%d", job.JobID, i)
		job.MapTasks[taskID] = &TaskState{
			TaskID:              taskID,
			StageID:             "map-stage",
			StageType:           pb.StageType_STAGE_TYPE_MAP,
			InputFiles:          []string{file},
			BroadcastFiles:      broadcastFiles,
			BroadcastTableNames: broadcastTableNames,
			Status:              TaskPending,
			LastUpdated:         time.Now(),
		}
	}
	job.mu.Unlock()

	// 2. Map Stage Loop
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		job.mu.Lock()
		allMapDone := true
		anyMapFailed := false
		var mapError string

		for _, t := range job.MapTasks {
			// A5: tasks cancelled by speculative execution are treated as effectively done.
			speculativeCancelled := t.Status == TaskFailed && strings.HasPrefix(t.ErrorMsg, "cancelled:")
			if t.Status != TaskCompleted && !speculativeCancelled {
				allMapDone = false
			}
			if t.Status == TaskFailed && !speculativeCancelled {
				anyMapFailed = true
				mapError = t.ErrorMsg
			}
		}

		if anyMapFailed {
			job.mu.Unlock()
			return fmt.Errorf("map stage failed: %s", mapError)
		}

		if allMapDone {
			job.mu.Unlock()
			break
		}

		// Dispatch pending Map tasks
		s.dispatchPendingTasks(ctx, job, job.MapTasks)
		job.mu.Unlock()

		time.Sleep(500 * time.Millisecond)
	}

	log.Printf("Map stage for job %s completed. Initializing Reduce stage.", job.JobID)

	// 3. Initialize Reduce Tasks with AQE (Adaptive Query Execution: Partition Coalescing)
	job.mu.Lock()
	job.ReduceTasks = make(map[string]*TaskState)

	// A6: Skew detection — collect per-partition sizes and identify hot partitions.
	var partitionSizes []int64
	for p := int32(0); p < job.NumPartitions; p++ {
		parts := s.store.GetPartitions(p)
		size := int64(0)
		for _, part := range parts {
			size += part.SizeBytes
		}
		partitionSizes = append(partitionSizes, size)
	}
	medianSize := computeMedian(partitionSizes)
	skewedPartitions := make(map[int32]bool)
	for p, size := range partitionSizes {
		if medianSize > 0 && size > 3*medianSize {
			log.Printf("[SKEWED_PARTITION] Partition %d is %.1fx median (size=%d, median=%d) - splitting into 2 reduce tasks",
				p, float64(size)/float64(medianSize), size, medianSize)
			skewedPartitions[int32(p)] = true
		}
	}

	var coalescedGroups [][]int32
	var currentGroup []int32
	currentGroupSize := int64(0)
	const CoalesceThreshold = int64(20000)

	for p := int32(0); p < job.NumPartitions; p++ {
		parts := s.store.GetPartitions(p)
		size := int64(0)
		for _, part := range parts {
			size += part.SizeBytes
		}

		if len(currentGroup) == 0 {
			currentGroup = append(currentGroup, p)
			currentGroupSize = size
		} else {
			if currentGroupSize < CoalesceThreshold {
				currentGroup = append(currentGroup, p)
				currentGroupSize += size
			} else {
				coalescedGroups = append(coalescedGroups, currentGroup)
				currentGroup = []int32{p}
				currentGroupSize = size
			}
		}
	}
	if len(currentGroup) > 0 {
		coalescedGroups = append(coalescedGroups, currentGroup)
	}

	log.Printf("AQE: Partition coalescing groups for job %s: %v", job.JobID, coalescedGroups)

	baseReduceSQL := job.ReduceSQL
	if baseReduceSQL == "" {
		baseReduceSQL = job.SQL
	}

	for _, group := range coalescedGroups {
		outPartitionID := group[0]

		// A6: For single-partition skewed groups, split into two parallel reduce tasks.
		if len(group) == 1 && skewedPartitions[group[0]] {
			p := group[0]
			taskID0 := fmt.Sprintf("%s-reduce-%d", job.JobID, outPartitionID*2)
			taskID1 := fmt.Sprintf("%s-reduce-%d", job.JobID, outPartitionID*2+1)
			job.ReduceTasks[taskID0] = &TaskState{
				TaskID:              taskID0,
				StageID:             "reduce-stage",
				StageType:           pb.StageType_STAGE_TYPE_REDUCE,
				OutputPartitionID:   outPartitionID * 2,
				CoalescedPartitions: []int32{p},
				Status:              TaskPending,
				LastUpdated:         time.Now(),
				ReduceSQLOverride:   baseReduceSQL + " -- skew_split:0",
			}
			job.ReduceTasks[taskID1] = &TaskState{
				TaskID:              taskID1,
				StageID:             "reduce-stage",
				StageType:           pb.StageType_STAGE_TYPE_REDUCE,
				OutputPartitionID:   outPartitionID*2 + 1,
				CoalescedPartitions: []int32{p},
				Status:              TaskPending,
				LastUpdated:         time.Now(),
				ReduceSQLOverride:   baseReduceSQL + " -- skew_split:1",
			}
			continue
		}

		taskID := fmt.Sprintf("%s-reduce-%d", job.JobID, outPartitionID)
		job.ReduceTasks[taskID] = &TaskState{
			TaskID:              taskID,
			StageID:             "reduce-stage",
			StageType:           pb.StageType_STAGE_TYPE_REDUCE,
			OutputPartitionID:   outPartitionID,
			CoalescedPartitions: group,
			Status:              TaskPending,
			LastUpdated:         time.Now(),
		}
	}
	job.mu.Unlock()

	// 4. Reduce Stage Loop
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		job.mu.Lock()
		allReduceDone := true
		anyReduceFailed := false
		var reduceError string

		// If a map task was marked pending again due to worker failure, we must complete Map stage first.
		// A5: tasks cancelled by speculative execution are treated as effectively done.
		anyMapPendingOrRunning := false
		for _, t := range job.MapTasks {
			speculativeCancelled := t.Status == TaskFailed && strings.HasPrefix(t.ErrorMsg, "cancelled:")
			if t.Status != TaskCompleted && !speculativeCancelled {
				anyMapPendingOrRunning = true
				allReduceDone = false
			}
		}

		if anyMapPendingOrRunning {
			// Redo map tasks first
			s.dispatchPendingTasks(ctx, job, job.MapTasks)
			job.mu.Unlock()
			time.Sleep(500 * time.Millisecond)
			continue
		}

		for _, t := range job.ReduceTasks {
			// A5: tasks cancelled by speculative execution are treated as effectively done.
			speculativeCancelled := t.Status == TaskFailed && strings.HasPrefix(t.ErrorMsg, "cancelled:")
			if t.Status != TaskCompleted && !speculativeCancelled {
				allReduceDone = false
			}
			if t.Status == TaskFailed && !speculativeCancelled {
				anyReduceFailed = true
				reduceError = t.ErrorMsg
			}
		}

		if anyReduceFailed {
			job.mu.Unlock()
			return fmt.Errorf("reduce stage failed: %s", reduceError)
		}

		if allReduceDone {
			job.mu.Unlock()
			break
		}

		// Dispatch pending Reduce tasks
		s.dispatchPendingTasks(ctx, job, job.ReduceTasks)
		job.mu.Unlock()

		time.Sleep(500 * time.Millisecond)
	}

	return nil
}

func (s *Scheduler) dispatchPendingTasks(ctx context.Context, job *Job, tasks map[string]*TaskState) {
	activeWorkers := s.store.GetActiveWorkers()
	if len(activeWorkers) == 0 {
		log.Printf("[Warning] No active workers available to schedule tasks.")
		return
	}

	// 1. Throttling using FairScheduler
	runningTasksCount := 0
	for _, task := range tasks {
		if task.Status == TaskRunning {
			runningTasksCount++
		}
	}

	allowedTasks := s.fairSched.GetAllowedTasks(job.JobID)
	slotsAvailable := allowedTasks - runningTasksCount
	if slotsAvailable <= 0 {
		// Log scale-out if we have pending tasks waiting but no slots available
		hasPending := false
		for _, task := range tasks {
			if task.Status == TaskPending && !task.LastUpdated.IsZero() && time.Since(task.LastUpdated) > 2*time.Second {
				hasPending = true
				break
			}
		}
		if hasPending {
			log.Println("[SCALE_OUT_WORKERS]")
		}
		return
	}

	// Log scale-out if there are pending tasks that have been waiting > 2 seconds
	hasOldPending := false
	for _, task := range tasks {
		if task.Status == TaskPending && !task.LastUpdated.IsZero() && time.Since(task.LastUpdated) > 2*time.Second {
			hasOldPending = true
			break
		}
	}
	if hasOldPending {
		log.Println("[SCALE_OUT_WORKERS]")
	}

	// A4: Load-aware worker selection — sort by CPU usage ascending (least loaded first).
	sort.Slice(activeWorkers, func(i, j int) bool {
		return activeWorkers[i].CpuUsagePct < activeWorkers[j].CpuUsagePct
	})

	// Separate workers into preferred (CPU ≤ 80 %) and overloaded (CPU > 80 %) pools.
	var preferredWorkers []*raft.WorkerConfig
	var overloadedWorkers []*raft.WorkerConfig
	for _, w := range activeWorkers {
		if w.CpuUsagePct <= 80.0 {
			preferredWorkers = append(preferredWorkers, w)
		} else {
			overloadedWorkers = append(overloadedWorkers, w)
		}
	}

	// Fall back to overloaded workers only when there is no other option.
	workerPool := preferredWorkers
	if len(workerPool) == 0 {
		workerPool = overloadedWorkers
	}

	workerIdx := 0
	dispatched := 0
	for _, task := range tasks {
		if task.Status != TaskPending {
			continue
		}
		if dispatched >= slotsAvailable {
			break
		}

		worker := workerPool[workerIdx%len(workerPool)]
		workerIdx++

		task.Status = TaskRunning
		task.WorkerID = worker.WorkerID
		task.StartTime = time.Now()
		task.LastUpdated = time.Now()
		dispatched++

		log.Printf("[DISPATCH] Task %s -> worker %s (CPU: %.1f%%)",
			task.TaskID, worker.WorkerID, worker.CpuUsagePct)

		go s.submitTaskToWorker(ctx, job, task, worker)
	}
}

func (s *Scheduler) submitTaskToWorker(ctx context.Context, job *Job, task *TaskState, worker *raft.WorkerConfig) {
	log.Printf("Submitting task %s (%v) to worker %s at %s:%d",
		task.TaskID, task.StageType, worker.WorkerID, worker.Host, worker.ControlPort)

	addr := fmt.Sprintf("%s:%d", worker.Host, worker.ControlPort)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Failed to dial worker %s: %v", worker.WorkerID, err)
		job.mu.Lock()
		task.Status = TaskPending
		task.WorkerID = ""
		job.mu.Unlock()
		return
	}
	defer conn.Close()

	client := pb.NewWorkerControlClient(conn)

	mapSQL := job.MapSQL
	if mapSQL == "" {
		mapSQL = job.SQL
	}
	reduceSQL := job.ReduceSQL
	if reduceSQL == "" {
		reduceSQL = job.SQL
	}
	// A6: Per-task SQL override for skew-split reduce tasks.
	if task.ReduceSQLOverride != "" {
		reduceSQL = task.ReduceSQLOverride
	}

	var req *pb.SubmitTaskRequest
	if task.StageType == pb.StageType_STAGE_TYPE_MAP {
		req = &pb.SubmitTaskRequest{
			TaskId:              task.TaskID,
			StageId:             task.StageID,
			StageType:           task.StageType,
			InputFiles:          task.InputFiles,
			MapSql:              mapSQL,
			NumPartitions:       job.NumPartitions,
			OutputDir:           job.OutputDir,
			BroadcastFiles:      task.BroadcastFiles,
			BroadcastTableNames: task.BroadcastTableNames,
		}
	} else {
		type SourceKey struct {
			WorkerID      string
			FlightAddress string
			TaskID        string
		}
		sourceToPartitions := make(map[SourceKey][]int32)
		for _, pid := range task.CoalescedPartitions {
			parts := s.store.GetPartitions(pid)
			for _, p := range parts {
				key := SourceKey{
					WorkerID:      p.WorkerID,
					FlightAddress: p.FlightAddress,
					TaskID:        p.TaskID,
				}
				sourceToPartitions[key] = append(sourceToPartitions[key], p.PartitionID)
			}
		}

		var shuffleInputs []*pb.ShuffleInput
		for key, pids := range sourceToPartitions {
			primaryPid := int32(-1)
			if len(pids) > 0 {
				primaryPid = pids[0]
			}
			shuffleInputs = append(shuffleInputs, &pb.ShuffleInput{
				PartitionId:   primaryPid,
				WorkerId:      key.WorkerID,
				FlightAddress: key.FlightAddress,
				MapperTaskId:  key.TaskID,
				PartitionIds:  pids,
			})
		}

		req = &pb.SubmitTaskRequest{
			TaskId:              task.TaskID,
			StageId:             task.StageID,
			StageType:           task.StageType,
			ReduceSql:           reduceSQL,
			OutputPartitionId:   task.OutputPartitionID,
			ShuffleInputs:       shuffleInputs,
			NumPartitions:       job.NumPartitions,
			OutputDir:           job.OutputDir,
			CoalescedPartitions: task.CoalescedPartitions,
		}
	}

	resp, err := client.SubmitTask(ctx, req)
	if err != nil || !resp.Success {
		errMsg := "unknown error"
		if err != nil {
			errMsg = err.Error()
		} else if resp != nil {
			errMsg = resp.Message
		}
		log.Printf("Task %s submission failed: %s. Reverting to PENDING.", task.TaskID, errMsg)
		job.mu.Lock()
		task.Status = TaskPending
		task.WorkerID = ""
		job.mu.Unlock()
		return
	}

	log.Printf("Task %s successfully submitted to worker %s", task.TaskID, worker.WorkerID)
}

// HandleWorkerFailure is called when a worker is detected to have failed.
// It rolls back pending, running, or completed tasks that depended on this worker.
func (s *Scheduler) HandleWorkerFailure(workerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("Scheduler handling failure of worker %s", workerID)

	for _, job := range s.jobs {
		job.mu.Lock()
		if job.Status != "RUNNING" {
			job.mu.Unlock()
			continue
		}

		// 1. Reset tasks currently running on the failed worker
		for _, task := range job.MapTasks {
			if task.WorkerID == workerID && task.Status == TaskRunning {
				log.Printf("Rescheduling running map task %s off failed worker %s", task.TaskID, workerID)
				task.Status = TaskPending
				task.WorkerID = ""
			}
		}

		for _, task := range job.ReduceTasks {
			if task.WorkerID == workerID && task.Status == TaskRunning {
				log.Printf("Rescheduling running reduce task %s off failed worker %s", task.TaskID, workerID)
				task.Status = TaskPending
				task.WorkerID = ""
			}
		}

		// 2. Re-run completed Map tasks from the failed worker because intermediate files are now lost
		var mapTasksToReschedule []string
		for _, task := range job.MapTasks {
			if task.WorkerID == workerID && task.Status == TaskCompleted {
				log.Printf("Rescheduling completed map task %s because worker %s containing partition data failed", task.TaskID, workerID)
				task.Status = TaskPending
				task.WorkerID = ""
				mapTasksToReschedule = append(mapTasksToReschedule, task.TaskID)
			}
		}

		if len(mapTasksToReschedule) > 0 {
			s.store.RemovePartitionsForTasks(mapTasksToReschedule)
		}

		job.mu.Unlock()
	}

	s.store.RemovePartitionsForWorker(workerID)
}

// HandleTaskStatusUpdate handles gRPC callback updates from workers.
func (s *Scheduler) HandleTaskStatusUpdate(taskID string, stageID string, workerID string, status pb.TaskStatus, errorMsg string, partitions []*pb.PartitionMetadata, tableStats *pb.TableStats) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var targetJob *Job
	for _, job := range s.jobs {
		job.mu.Lock()
		if _, exists := job.MapTasks[taskID]; exists {
			targetJob = job
			job.mu.Unlock()
			break
		}
		if _, exists := job.ReduceTasks[taskID]; exists {
			targetJob = job
			job.mu.Unlock()
			break
		}
		job.mu.Unlock()
	}

	if targetJob == nil {
		return fmt.Errorf("task %s not found in any job", taskID)
	}

	targetJob.mu.Lock()
	defer targetJob.mu.Unlock()

	var localStatus TaskStatus
	switch status {
	case pb.TaskStatus_TASK_STATUS_COMPLETED:
		localStatus = TaskCompleted
	case pb.TaskStatus_TASK_STATUS_FAILED:
		localStatus = TaskFailed
	case pb.TaskStatus_TASK_STATUS_RUNNING:
		localStatus = TaskRunning
	case pb.TaskStatus_TASK_STATUS_PENDING:
		localStatus = TaskPending
	default:
		localStatus = TaskPending
	}

	var tState *TaskState
	if stageID == "map-stage" {
		tState = targetJob.MapTasks[taskID]
	} else if stageID == "reduce-stage" {
		tState = targetJob.ReduceTasks[taskID]
	}

	if tState == nil {
		return fmt.Errorf("task %s not found in stage %s", taskID, stageID)
	}

	// Avoid updating if already completed (unless reset by failure)
	if tState.Status == TaskCompleted && localStatus != TaskPending {
		return nil
	}

	tState.Status = localStatus
	tState.ErrorMsg = errorMsg
	tState.LastUpdated = time.Now()

	log.Printf("Task %s (%s) status updated to %v by worker %s", taskID, stageID, localStatus, workerID)

	if localStatus == TaskCompleted {
		if stageID == "map-stage" {
			for _, p := range partitions {
				err := s.store.AddPartition(raft.PartitionInfo{
					PartitionID:   p.PartitionId,
					WorkerID:      p.WorkerId,
					FlightAddress: p.FlightAddress,
					TaskID:        p.TaskId,
					SizeBytes:     p.SizeBytes,
				})
				if err != nil {
					log.Printf("Failed to register partition %d: %v", p.PartitionId, err)
				}
			}
		}

		// Save table stats if returned in the status update
		if tableStats != nil {
			var tableName string
			sqlUpper := strings.ToUpper(strings.TrimSpace(targetJob.SQL))
			if strings.HasPrefix(sqlUpper, "ANALYZE TABLE") {
				parts := strings.Fields(targetJob.SQL)
				if len(parts) >= 3 {
					tableName = parts[2]
				}
			} else {
				tableName = "input"
			}

			if tableName != "" {
				colStatsMap := make(map[string]*raft.ColumnStats)
				for _, cs := range tableStats.ColStats {
					colStatsMap[cs.ColumnName] = &raft.ColumnStats{
						ColumnName:  cs.ColumnName,
						Cardinality: cs.Cardinality,
						NullCount:   cs.NullCount,
					}
				}

				err := s.store.UpdateTableStats(tableName, &raft.TableStats{
					RowCount:  tableStats.RowCount,
					SizeBytes: tableStats.SizeBytes,
					ColStats:  colStatsMap,
				})
				if err != nil {
					log.Printf("Failed to update table stats for %s: %v", tableName, err)
				} else {
					log.Printf("CBO Stats: Table %s updated with row_count=%d size_bytes=%d", tableName, tableStats.RowCount, tableStats.SizeBytes)
				}
			}
		}

		// A5: Speculative execution — cancel the twin task now that one side has won.
		var taskMap map[string]*TaskState
		if stageID == "map-stage" {
			taskMap = targetJob.MapTasks
		} else {
			taskMap = targetJob.ReduceTasks
		}

		var twinTask *TaskState
		if tState.IsSpeculative {
			// This speculative copy finished first — cancel the original task.
			if orig, ok := taskMap[tState.SpeculativeOf]; ok {
				if orig.Status != TaskCompleted && orig.Status != TaskFailed {
					orig.Status = TaskFailed
					orig.ErrorMsg = "cancelled: speculative copy won"
					twinTask = orig
					log.Printf("[SPECULATIVE] Speculative copy %s won; cancelling original %s", tState.TaskID, orig.TaskID)
				}
			}
		} else {
			// The original finished first — cancel any speculative copy.
			for _, t := range taskMap {
				if t.SpeculativeOf == tState.TaskID && t.Status != TaskCompleted && t.Status != TaskFailed {
					t.Status = TaskFailed
					t.ErrorMsg = "cancelled: original won"
					twinTask = t
					log.Printf("[SPECULATIVE] Original %s won; cancelling speculative copy %s", tState.TaskID, t.TaskID)
					break
				}
			}
		}

		if twinTask != nil {
			go s.cancelTaskOnWorker(context.Background(), twinTask)
		}
	}

	return nil
}

// computeMedian returns the median value from a slice of int64 sizes.
// It sorts a copy of the slice so the original is not modified.
func computeMedian(sizes []int64) int64 {
	if len(sizes) == 0 {
		return 0
	}
	cp := make([]int64, len(sizes))
	copy(cp, sizes)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	return cp[len(cp)/2]
}

// startSpeculativeMonitor runs a background goroutine that periodically checks for
// stragglers and launches speculative duplicate tasks to reduce tail latency.
func (s *Scheduler) startSpeculativeMonitor(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Snapshot the job list under s.mu, then release the lock before
			// touching individual jobs so we avoid nested lock ordering issues.
			s.mu.Lock()
			jobList := make([]*Job, 0, len(s.jobs))
			for _, j := range s.jobs {
				jobList = append(jobList, j)
			}
			s.mu.Unlock()

			for _, job := range jobList {
				s.checkSpeculativeForJob(ctx, job)
			}
		}
	}
}

// checkSpeculativeForJob inspects running tasks in a single job and launches
// speculative copies for tasks that are running significantly slower than the median.
func (s *Scheduler) checkSpeculativeForJob(ctx context.Context, job *Job) {
	job.mu.Lock()
	defer job.mu.Unlock()

	if job.Status != "RUNNING" {
		return
	}

	// Build a combined view of all tasks so we can compute a cross-stage median.
	allTasks := make(map[string]*TaskState, len(job.MapTasks)+len(job.ReduceTasks))
	for id, t := range job.MapTasks {
		allTasks[id] = t
	}
	for id, t := range job.ReduceTasks {
		allTasks[id] = t
	}

	// Compute the median duration of completed tasks.
	var completedDurations []time.Duration
	for _, t := range allTasks {
		if t.Status == TaskCompleted && !t.StartTime.IsZero() {
			completedDurations = append(completedDurations, t.LastUpdated.Sub(t.StartTime))
		}
	}
	if len(completedDurations) == 0 {
		return
	}
	sort.Slice(completedDurations, func(i, j int) bool { return completedDurations[i] < completedDurations[j] })
	medianDuration := completedDurations[len(completedDurations)/2]

	// Do not speculate when the median is very small — the overhead would dominate.
	if medianDuration <= 5*time.Second {
		return
	}
	threshold := time.Duration(float64(medianDuration) * 1.5)

	// Look up available workers once; we release job.mu during the store call later,
	// but here we only call GetActiveWorkers (store-level lock, no conflict).
	activeWorkers := s.store.GetActiveWorkers()

	for _, t := range allTasks {
		if t.Status != TaskRunning || t.StartTime.IsZero() || t.IsSpeculative {
			continue
		}
		if time.Since(t.StartTime) <= threshold {
			continue
		}

		// Check whether a speculative copy already exists.
		alreadySpeculated := false
		for _, other := range allTasks {
			if other.SpeculativeOf == t.TaskID {
				alreadySpeculated = true
				break
			}
		}
		if alreadySpeculated {
			continue
		}

		// Find a different worker to run the speculative copy on.
		var speculativeWorker *raft.WorkerConfig
		for _, w := range activeWorkers {
			if w.WorkerID != t.WorkerID {
				speculativeWorker = w
				break
			}
		}
		if speculativeWorker == nil {
			continue // No alternative worker available.
		}

		specTaskID := t.TaskID + "-spec"
		specTask := &TaskState{
			TaskID:              specTaskID,
			StageID:             t.StageID,
			StageType:           t.StageType,
			InputFiles:          t.InputFiles,
			OutputPartitionID:   t.OutputPartitionID,
			Status:              TaskPending,
			LastUpdated:         time.Now(),
			BroadcastFiles:      t.BroadcastFiles,
			BroadcastTableNames: t.BroadcastTableNames,
			CoalescedPartitions: t.CoalescedPartitions,
			ReduceSQLOverride:   t.ReduceSQLOverride,
			IsSpeculative:       true,
			SpeculativeOf:       t.TaskID,
		}

		if t.StageID == "map-stage" {
			job.MapTasks[specTaskID] = specTask
		} else {
			job.ReduceTasks[specTaskID] = specTask
		}

		log.Printf("[SPECULATIVE] Launching speculative copy of task %s on different worker", t.TaskID)
	}
}

// cancelTaskOnWorker dials the worker that owns the given task and sends a CancelTask RPC.
// Errors are intentionally ignored — the worker may have already finished the task.
func (s *Scheduler) cancelTaskOnWorker(ctx context.Context, task *TaskState) {
	if task.WorkerID == "" {
		return
	}

	allWorkers := s.store.GetAllWorkers()
	var targetWorker *raft.WorkerConfig
	for _, w := range allWorkers {
		if w.WorkerID == task.WorkerID {
			targetWorker = w
			break
		}
	}
	if targetWorker == nil {
		return
	}

	addr := fmt.Sprintf("%s:%d", targetWorker.Host, targetWorker.ControlPort)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return
	}
	defer conn.Close()

	client := pb.NewWorkerControlClient(conn)
	_, _ = client.CancelTask(ctx, &pb.CancelTaskRequest{TaskId: task.TaskID})
}

// GetPendingTasksCount returns the total number of tasks currently in PENDING status.
func (s *Scheduler) GetPendingTasksCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, job := range s.jobs {
		job.mu.Lock()
		if job.Status == "RUNNING" || job.Status == "PENDING" {
			for _, t := range job.MapTasks {
				if t.Status == TaskPending {
					count++
				}
			}
			for _, t := range job.ReduceTasks {
				if t.Status == TaskPending {
					count++
				}
			}
		}
		job.mu.Unlock()
	}
	return count
}
