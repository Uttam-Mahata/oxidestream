package raft

import (
	"fmt"
	"sync"
	"time"
)

// WorkerConfig represents the configuration of a registered worker.
type WorkerConfig struct {
	WorkerID      string    `json:"worker_id"`
	Host          string    `json:"host"`
	ControlPort   int32     `json:"control_port"`
	FlightPort    int32     `json:"flight_port"`
	NumCores      int32     `json:"num_cores"`
	TotalMemoryMB int64     `json:"total_memory_mb"`
	LastActive    time.Time `json:"last_active"`
	Active        bool      `json:"active"`
	CpuUsagePct   float32   `json:"cpu_usage_pct"`
	MemoryUsedMB  int64     `json:"memory_used_mb"`
}

// ColumnStats represents table stats for a column.
type ColumnStats struct {
	ColumnName  string `json:"column_name"`
	Cardinality int64  `json:"cardinality"`
	NullCount   int64  `json:"null_count"`
}

// TableStats represents stats for a table.
type TableStats struct {
	RowCount  int64                  `json:"row_count"`
	SizeBytes int64                  `json:"size_bytes"`
	ColStats  map[string]*ColumnStats `json:"col_stats"`
}

// TableMetadata holds catalog details.
type TableMetadata struct {
	TableName string      `json:"table_name"`
	Schema    string      `json:"schema"`
	Stats     *TableStats `json:"stats"`
}

// PartitionInfo details where a given partition is stored.
type PartitionInfo struct {
	PartitionID   int32  `json:"partition_id"`
	WorkerID      string `json:"worker_id"`
	FlightAddress string `json:"flight_address"`
	TaskID        string `json:"task_id"`
	SizeBytes     int64  `json:"size_bytes"`
}

// MetadataStore implements an in-memory replicated state machine / mock Raft metadata store.
type MetadataStore struct {
	mu         sync.RWMutex
	workers    map[string]*WorkerConfig
	catalog    map[string]*TableMetadata
	partitions map[int32][]PartitionInfo
	logs       []string // Mock replicated log
	term       uint64
	leaderID   string
	isLeader   bool
}

// NewMetadataStore creates a new consensus metadata store.
func NewMetadataStore(leaderID string) *MetadataStore {
	return &MetadataStore{
		workers:    make(map[string]*WorkerConfig),
		catalog:    make(map[string]*TableMetadata),
		partitions: make(map[int32][]PartitionInfo),
		logs:       make([]string, 0),
		term:       1,
		leaderID:   leaderID,
		isLeader:   true, // Master is leader by default
	}
}

// IsLeaderStatus returns if the current node is the consensus leader.
func (s *MetadataStore) IsLeaderStatus() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.isLeader
}

// GetLeaderID returns the ID of the current leader.
func (s *MetadataStore) GetLeaderID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.leaderID
}

// RegisterWorker records a new worker configuration.
func (s *MetadataStore) RegisterWorker(w *WorkerConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logs = append(s.logs, fmt.Sprintf("TERM:%d REGISTER_WORKER:%s", s.term, w.WorkerID))
	s.workers[w.WorkerID] = w
	return nil
}

// UpdateWorkerHeartbeat updates the worker's last active timestamp, sets active status,
// and records the latest CPU and memory metrics reported by the worker.
func (s *MetadataStore) UpdateWorkerHeartbeat(workerID string, cpuPct float32, memMB int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	w, exists := s.workers[workerID]
	if !exists {
		return fmt.Errorf("worker %s not registered", workerID)
	}
	w.LastActive = time.Now()
	w.Active = true
	w.CpuUsagePct = cpuPct
	w.MemoryUsedMB = memMB
	return nil
}

// SetWorkerActiveStatus marks a worker as active/inactive.
func (s *MetadataStore) SetWorkerActiveStatus(workerID string, active bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if w, exists := s.workers[workerID]; exists {
		w.Active = active
		s.logs = append(s.logs, fmt.Sprintf("TERM:%d SET_WORKER_ACTIVE:%s:%t", s.term, workerID, active))
	}
}

// GetActiveWorkers returns a snapshot of all active workers.
func (s *MetadataStore) GetActiveWorkers() []*WorkerConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var active []*WorkerConfig
	for _, w := range s.workers {
		if w.Active {
			wCopy := *w
			active = append(active, &wCopy)
		}
	}
	return active
}

// GetAllWorkers returns a snapshot of all registered workers.
func (s *MetadataStore) GetAllWorkers() []*WorkerConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var all []*WorkerConfig
	for _, w := range s.workers {
		wCopy := *w
		all = append(all, &wCopy)
	}
	return all
}


// AddTable adds a table to the metadata catalog.
func (s *MetadataStore) AddTable(table string, schema string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logs = append(s.logs, fmt.Sprintf("TERM:%d ADD_TABLE:%s", s.term, table))
	s.catalog[table] = &TableMetadata{TableName: table, Schema: schema}
	return nil
}

// UpdateTableStats updates statistics for a table.
func (s *MetadataStore) UpdateTableStats(tableName string, stats *TableStats) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tbl, exists := s.catalog[tableName]
	if !exists {
		tbl = &TableMetadata{TableName: tableName}
		s.catalog[tableName] = tbl
	}
	tbl.Stats = stats
	s.logs = append(s.logs, fmt.Sprintf("TERM:%d UPDATE_TABLE_STATS:%s", s.term, tableName))
	return nil
}

// GetTableStats retrieves statistics for a table.
func (s *MetadataStore) GetTableStats(tableName string) (*TableStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tbl, exists := s.catalog[tableName]
	if !exists || tbl.Stats == nil {
		return nil, fmt.Errorf("stats not found for table %s", tableName)
	}
	return tbl.Stats, nil
}

// AddPartition registers a produced partition.
func (s *MetadataStore) AddPartition(p PartitionInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logs = append(s.logs, fmt.Sprintf("TERM:%d ADD_PARTITION:%d:%s:%d", s.term, p.PartitionID, p.WorkerID, p.SizeBytes))
	s.partitions[p.PartitionID] = append(s.partitions[p.PartitionID], p)
	return nil
}

// ClearPartitions resets all partition mappings (useful at job starts).
func (s *MetadataStore) ClearPartitions() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.partitions = make(map[int32][]PartitionInfo)
}

// GetPartitions retrieves partition metadata for a specific partition ID.
func (s *MetadataStore) GetPartitions(partitionID int32) []PartitionInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	parts, exists := s.partitions[partitionID]
	if !exists {
		return nil
	}
	res := make([]PartitionInfo, len(parts))
	copy(res, parts)
	return res
}

// GetAllPartitions retrieves all stored partition metadata.
func (s *MetadataStore) GetAllPartitions() map[int32][]PartitionInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	res := make(map[int32][]PartitionInfo)
	for k, v := range s.partitions {
		resCopy := make([]PartitionInfo, len(v))
		copy(resCopy, v)
		res[k] = resCopy
	}
	return res
}

// RemovePartitionsForTasks filters out partition metadata matching the specified task IDs.
func (s *MetadataStore) RemovePartitionsForTasks(taskIDs []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	taskSet := make(map[string]bool)
	for _, tid := range taskIDs {
		taskSet[tid] = true
	}

	for pid, infos := range s.partitions {
		var kept []PartitionInfo
		for _, info := range infos {
			if !taskSet[info.TaskID] {
				kept = append(kept, info)
			}
		}
		if len(kept) == 0 {
			delete(s.partitions, pid)
		} else {
			s.partitions[pid] = kept
		}
	}
}

// RemovePartitionsForWorker filters out partition metadata matching a given worker.
func (s *MetadataStore) RemovePartitionsForWorker(workerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for pid, infos := range s.partitions {
		var kept []PartitionInfo
		for _, info := range infos {
			if info.WorkerID != workerID {
				kept = append(kept, info)
			}
		}
		if len(kept) == 0 {
			delete(s.partitions, pid)
		} else {
			s.partitions[pid] = kept
		}
	}
}

