package scheduler

import (
	"context"
	"fmt"
	"log"
	"time"
)

// SubmitLinearRegressionJob schedules a distributed closed-form OLS linear regression job.
func (s *Scheduler) SubmitLinearRegressionJob(ctx context.Context, inputFiles []string, numPartitions int32, outputDir string, iterations int) error {
	log.Printf("Starting Distributed Linear Regression Job (OLS closed-form) on inputs: %v", inputFiles)

	mapSQL := "SELECT * FROM input -- linear_regression"
	reduceSQL := "SELECT * FROM input -- linear_regression"

	s.mu.Lock()
	jobID := fmt.Sprintf("job-linear_regression-%d", time.Now().UnixNano())
	job := &Job{
		JobID:         jobID,
		SQL:           mapSQL,
		MapSQL:        mapSQL,
		ReduceSQL:     reduceSQL,
		InputFiles:    inputFiles,
		NumPartitions: numPartitions,
		OutputDir:     outputDir,
		Status:        "PENDING",
		MapTasks:      make(map[string]*TaskState),
		ReduceTasks:   make(map[string]*TaskState),
	}
	s.jobs[jobID] = job
	s.jobQueue = append(s.jobQueue, job)
	s.mu.Unlock()
	s.jobCond.Signal()

	log.Printf("Submitted Linear Regression Job: %s", jobID)
	status, err := s.WaitForJob(jobID)
	if err != nil || status != "COMPLETED" {
		return fmt.Errorf("linear regression job failed: status=%s, err=%v", status, err)
	}

	log.Println("Distributed Linear Regression Job completed successfully!")
	return nil
}

// SubmitPageRankJob schedules iterative distributed PageRank graph tasks.
func (s *Scheduler) SubmitPageRankJob(ctx context.Context, inputFiles []string, numPartitions int32, outputDir string, iterations int) error {
	log.Printf("Starting Distributed PageRank Graph Job with %d iterations...", iterations)

	for iter := 1; iter <= iterations; iter++ {
		log.Printf("PageRank: Iteration %d/%d...", iter, iterations)

		// Map Stage: Map nodes to outgoing link ranks
		mapSQL := fmt.Sprintf("SELECT rank / out_degree AS rank_contrib, target FROM input")
		// Reduce Stage: Aggregate ranks per target node
		reduceSQL := fmt.Sprintf("SELECT target, 0.15 + 0.85 * SUM(rank_contrib) AS rank FROM input GROUP BY target")

		job, err := s.SubmitMapReduceJob(mapSQL, reduceSQL, inputFiles, numPartitions, outputDir)
		if err != nil {
			return fmt.Errorf("failed to submit PageRank iteration %d: %w", iter, err)
		}

		status, err := s.WaitForJob(job.JobID)
		if err != nil || status != "COMPLETED" {
			return fmt.Errorf("iteration %d failed with status %s: %v", iter, status, err)
		}

		log.Printf("PageRank: Iteration %d complete.", iter)
		time.Sleep(100 * time.Millisecond)
	}

	log.Println("Distributed PageRank Graph Job completed successfully!")
	return nil
}
