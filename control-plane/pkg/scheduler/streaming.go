package scheduler

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// StreamingScheduler orchestrates structured streaming over new files in a micro-batch loop.
type StreamingScheduler struct {
	scheduler      *Scheduler
	inputDir       string
	checkpointFile string
	processedFiles map[string]bool
	mu             sync.Mutex
	sql            string
	mapSql         string
	reduceSql      string
	numPartitions  int32
	outputDir      string
	watermarkDelay time.Duration
}

// NewStreamingScheduler creates a new streaming orchestrator.
func NewStreamingScheduler(s *Scheduler, inputDir string, checkpointFile string, sql string, mapSql string, reduceSql string, numPartitions int32, outputDir string) *StreamingScheduler {
	return &StreamingScheduler{
		scheduler:      s,
		inputDir:       inputDir,
		checkpointFile: checkpointFile,
		processedFiles: make(map[string]bool),
		sql:            sql,
		mapSql:         mapSql,
		reduceSql:      reduceSql,
		numPartitions:  numPartitions,
		outputDir:      outputDir,
		watermarkDelay: 5 * time.Second, // watermark delay offset
	}
}

// Start runs the periodic micro-batch checking loop.
func (ss *StreamingScheduler) Start(ctx context.Context) {
	_ = os.MkdirAll(ss.inputDir, 0755)
	ss.loadCheckpoint()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	log.Printf("Streaming Scheduler scanning %s every 2 seconds with checkpoint file %s", ss.inputDir, ss.checkpointFile)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ss.processMicroBatch(ctx)
		}
	}
}

func (ss *StreamingScheduler) processMicroBatch(ctx context.Context) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	// 1. Scan input directory for CSV files
	files, err := filepath.Glob(filepath.Join(ss.inputDir, "*.csv"))
	if err != nil {
		log.Printf("Streaming scan error: %v", err)
		return
	}

	var newFiles []string
	var maxFileModTime time.Time

	for _, file := range files {
		if !ss.processedFiles[file] {
			newFiles = append(newFiles, file)

			fi, err := os.Stat(file)
			if err == nil && fi.ModTime().After(maxFileModTime) {
				maxFileModTime = fi.ModTime()
			}
		}
	}

	if len(newFiles) == 0 {
		return
	}

	log.Printf("Streaming Scheduler: Found %d new files to process: %v", len(newFiles), newFiles)

	// 2. Set up Watermarking (event time delay)
	maxDataTS := ss.getMaxTimestamp(newFiles)
	if maxDataTS == 0 {
		maxDataTS = time.Now().Unix()
	}
	watermarkCutoff := maxDataTS - int64(ss.watermarkDelay.Seconds())
	log.Printf("Streaming Scheduler: Max data timestamp found: %d. Watermark threshold cutoff set to: %d (delay: %v)", maxDataTS, watermarkCutoff, ss.watermarkDelay)

	// Inject watermark filter constraint into the SQL queries (referencing valid timestamp column)
	mapSQLWithWatermark := ss.mapSql
	if mapSQLWithWatermark != "" {
		if strings.Contains(strings.ToUpper(mapSQLWithWatermark), "WHERE") {
			mapSQLWithWatermark = fmt.Sprintf("%s AND timestamp >= %d", mapSQLWithWatermark, watermarkCutoff)
		} else if strings.Contains(strings.ToUpper(mapSQLWithWatermark), "GROUP BY") {
			parts := strings.SplitN(mapSQLWithWatermark, "GROUP BY", 2)
			mapSQLWithWatermark = fmt.Sprintf("%s WHERE timestamp >= %d GROUP BY%s", parts[0], watermarkCutoff, parts[1])
		} else {
			mapSQLWithWatermark = fmt.Sprintf("%s WHERE timestamp >= %d", mapSQLWithWatermark, watermarkCutoff)
		}
	}

	log.Printf("Streaming Scheduler: Submitting micro-batch with map SQL: %q", mapSQLWithWatermark)

	// 3. Submit micro-batch job
	var job *Job
	if ss.mapSql != "" && ss.reduceSql != "" {
		job, err = ss.scheduler.SubmitMapReduceJob(mapSQLWithWatermark, ss.reduceSql, newFiles, ss.numPartitions, ss.outputDir)
	} else {
		sqlWithWatermark := ss.sql
		if strings.Contains(strings.ToUpper(sqlWithWatermark), "WHERE") {
			sqlWithWatermark = fmt.Sprintf("%s AND timestamp >= %d", sqlWithWatermark, watermarkCutoff)
		} else {
			sqlWithWatermark = fmt.Sprintf("%s WHERE timestamp >= %d", sqlWithWatermark, watermarkCutoff)
		}
		job, err = ss.scheduler.SubmitSQLJob(sqlWithWatermark, newFiles, ss.numPartitions, ss.outputDir)
	}

	if err != nil {
		log.Printf("Streaming Scheduler failed to submit job: %v", err)
		return
	}

	// 4. Wait for micro-batch execution to finish
	status, err := ss.scheduler.WaitForJob(job.JobID)
	if err != nil || status != "COMPLETED" {
		log.Printf("Streaming Scheduler micro-batch job %s failed: status=%s, err=%v", job.JobID, status, err)
		return
	}

	log.Printf("Streaming Scheduler: Micro-batch job %s completed successfully", job.JobID)

	// 5. Checkpoint processed state
	for _, file := range newFiles {
		ss.processedFiles[file] = true
	}
	ss.saveCheckpoint()
}

func (ss *StreamingScheduler) saveCheckpoint() {
	data, err := json.Marshal(ss.processedFiles)
	if err != nil {
		log.Printf("Failed to marshal streaming checkpoint: %v", err)
		return
	}
	_ = os.WriteFile(ss.checkpointFile, data, 0644)
}

func (ss *StreamingScheduler) loadCheckpoint() {
	data, err := os.ReadFile(ss.checkpointFile)
	if err != nil {
		return
	}
	_ = json.Unmarshal(data, &ss.processedFiles)
}

// getMaxTimestamp reads files to find the maximum integer in the 'timestamp' column.
func (ss *StreamingScheduler) getMaxTimestamp(files []string) int64 {
	var maxTS int64 = 0
	for _, path := range files {
		if strings.Contains(path, "category_lookup") {
			continue // Skip lookup dimension tables
		}
		file, err := os.Open(path)
		if err != nil {
			continue
		}
		r := csv.NewReader(file)
		records, err := r.ReadAll()
		file.Close()
		if err != nil || len(records) < 2 {
			continue
		}
		header := records[0]
		tsIdx := -1
		for i, col := range header {
			if strings.TrimSpace(strings.ToLower(col)) == "timestamp" {
				tsIdx = i
				break
			}
		}
		if tsIdx == -1 {
			continue
		}
		for _, row := range records[1:] {
			if len(row) > tsIdx {
				val, err := strconv.ParseInt(strings.TrimSpace(row[tsIdx]), 10, 64)
				if err == nil && val > maxTS {
					maxTS = val
				}
			}
		}
	}
	return maxTS
}
