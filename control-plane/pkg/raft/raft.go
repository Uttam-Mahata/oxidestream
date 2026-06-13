package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"
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

// RaftCommand represents an entry to be applied to the Raft state machine.
type RaftCommand struct {
	OpType        string         `json:"op_type"`
	Key           string         `json:"key"`
	WorkerConfig  *WorkerConfig  `json:"worker_config,omitempty"`
	Active        bool           `json:"active,omitempty"`
	Schema        string         `json:"schema,omitempty"`
	TableStats    *TableStats    `json:"table_stats,omitempty"`
	PartitionInfo *PartitionInfo `json:"partition_info,omitempty"`
	TaskIDs       []string       `json:"task_ids,omitempty"`
	CpuUsagePct   float32        `json:"cpu_usage_pct,omitempty"`
	MemoryUsedMB  int64          `json:"memory_used_mb,omitempty"`
}

// MetadataFSM implements the raft.FSM interface.
type MetadataFSM struct {
	mu         sync.RWMutex
	workers    map[string]*WorkerConfig
	catalog    map[string]*TableMetadata
	partitions map[int32][]PartitionInfo
	logs       []string
}

func NewMetadataFSM() *MetadataFSM {
	return &MetadataFSM{
		workers:    make(map[string]*WorkerConfig),
		catalog:    make(map[string]*TableMetadata),
		partitions: make(map[int32][]PartitionInfo),
		logs:       make([]string, 0),
	}
}

func (f *MetadataFSM) Apply(log *raft.Log) interface{} {
	var cmd RaftCommand
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.logs = append(f.logs, fmt.Sprintf("TERM:%d OP:%s:%s", log.Term, cmd.OpType, cmd.Key))

	switch cmd.OpType {
	case "REGISTER_WORKER":
		f.workers[cmd.Key] = cmd.WorkerConfig
	case "UPDATE_WORKER_HEARTBEAT":
		if w, exists := f.workers[cmd.Key]; exists {
			w.LastActive = time.Now()
			w.Active = true
			w.CpuUsagePct = cmd.CpuUsagePct
			w.MemoryUsedMB = cmd.MemoryUsedMB
		}
	case "SET_WORKER_ACTIVE":
		if w, exists := f.workers[cmd.Key]; exists {
			w.Active = cmd.Active
		}
	case "ADD_TABLE":
		f.catalog[cmd.Key] = &TableMetadata{TableName: cmd.Key, Schema: cmd.Schema}
	case "UPDATE_TABLE_STATS":
		if tbl, exists := f.catalog[cmd.Key]; exists {
			tbl.Stats = cmd.TableStats
		} else {
			f.catalog[cmd.Key] = &TableMetadata{TableName: cmd.Key, Stats: cmd.TableStats}
		}
	case "ADD_PARTITION":
		p := *cmd.PartitionInfo
		f.partitions[p.PartitionID] = append(f.partitions[p.PartitionID], p)
	case "CLEAR_PARTITIONS":
		f.partitions = make(map[int32][]PartitionInfo)
	case "REMOVE_PARTITIONS_FOR_TASKS":
		taskSet := make(map[string]bool)
		for _, tid := range cmd.TaskIDs {
			taskSet[tid] = true
		}
		for pid, infos := range f.partitions {
			var kept []PartitionInfo
			for _, info := range infos {
				if !taskSet[info.TaskID] {
					kept = append(kept, info)
				}
			}
			if len(kept) == 0 {
				delete(f.partitions, pid)
			} else {
				f.partitions[pid] = kept
			}
		}
	case "REMOVE_PARTITIONS_FOR_WORKER":
		for pid, infos := range f.partitions {
			var kept []PartitionInfo
			for _, info := range infos {
				if info.WorkerID != cmd.Key {
					kept = append(kept, info)
				}
			}
			if len(kept) == 0 {
				delete(f.partitions, pid)
			} else {
				f.partitions[pid] = kept
			}
		}
	}
	return nil
}

func (f *MetadataFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	data, err := json.Marshal(f)
	if err != nil {
		return nil, err
	}
	return &dummySnapshot{data: data}, nil
}

func (f *MetadataFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return json.Unmarshal(data, f)
}

type dummySnapshot struct {
	data []byte
}

func (s *dummySnapshot) Persist(sink raft.SnapshotSink) error {
	_, err := sink.Write(s.data)
	if err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *dummySnapshot) Release() {}

// MetadataStore implements the replicated state machine using hashicorp/raft.
type MetadataStore struct {
	mu              sync.RWMutex
	raftNode        *raft.Raft
	fsm             *MetadataFSM
	peers           map[string]*raft.Raft
	transports      map[string]*raft.InmemTransport
	leaderID        string
	isLeader        bool
	nodeID          string
	simulateFailure bool
}

// NewMetadataStore creates a new consensus metadata store.
func NewMetadataStore(leaderID string) *MetadataStore {
	fsm1 := NewMetadataFSM()
	fsm2 := NewMetadataFSM()
	fsm3 := NewMetadataFSM()

	_, transport1 := raft.NewInmemTransport(raft.ServerAddress("node-1"))
	_, transport2 := raft.NewInmemTransport(raft.ServerAddress("node-2"))
	_, transport3 := raft.NewInmemTransport(raft.ServerAddress("node-3"))

	transport1.Connect(raft.ServerAddress("node-2"), transport2)
	transport1.Connect(raft.ServerAddress("node-3"), transport3)
	transport2.Connect(raft.ServerAddress("node-1"), transport1)
	transport2.Connect(raft.ServerAddress("node-3"), transport3)
	transport3.Connect(raft.ServerAddress("node-1"), transport1)
	transport3.Connect(raft.ServerAddress("node-2"), transport2)

	config1 := raft.DefaultConfig()
	config1.LocalID = raft.ServerID("node-1")
	config1.HeartbeatTimeout = 20 * time.Millisecond
	config1.ElectionTimeout = 20 * time.Millisecond
	config1.CommitTimeout = 2 * time.Millisecond
	config1.LeaderLeaseTimeout = 10 * time.Millisecond

	config2 := raft.DefaultConfig()
	config2.LocalID = raft.ServerID("node-2")
	config2.HeartbeatTimeout = 20 * time.Millisecond
	config2.ElectionTimeout = 20 * time.Millisecond
	config2.CommitTimeout = 2 * time.Millisecond
	config2.LeaderLeaseTimeout = 10 * time.Millisecond

	config3 := raft.DefaultConfig()
	config3.LocalID = raft.ServerID("node-3")
	config3.HeartbeatTimeout = 20 * time.Millisecond
	config3.ElectionTimeout = 20 * time.Millisecond
	config3.CommitTimeout = 2 * time.Millisecond
	config3.LeaderLeaseTimeout = 10 * time.Millisecond

	store1 := raft.NewInmemStore()
	store2 := raft.NewInmemStore()
	store3 := raft.NewInmemStore()

	snap1 := raft.NewInmemSnapshotStore()
	snap2 := raft.NewInmemSnapshotStore()
	snap3 := raft.NewInmemSnapshotStore()

	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID("node-1"),
				Address: transport1.LocalAddr(),
			},
			{
				ID:      raft.ServerID("node-2"),
				Address: transport2.LocalAddr(),
			},
			{
				ID:      raft.ServerID("node-3"),
				Address: transport3.LocalAddr(),
			},
		},
	}

	_ = raft.BootstrapCluster(config1, store1, store1, snap1, transport1, configuration)

	r1, _ := raft.NewRaft(config1, fsm1, store1, store1, snap1, transport1)
	r2, _ := raft.NewRaft(config2, fsm2, store2, store2, snap2, transport2)
	r3, _ := raft.NewRaft(config3, fsm3, store3, store3, snap3, transport3)

	// Wait for leader election
	time.Sleep(150 * time.Millisecond)

	peers := map[string]*raft.Raft{
		"peer-node-1": r2,
		"peer-node-2": r3,
	}

	transports := map[string]*raft.InmemTransport{
		"node-1": transport1,
		"node-2": transport2,
		"node-3": transport3,
	}

	return &MetadataStore{
		raftNode:   r1,
		fsm:        fsm1,
		peers:      peers,
		transports: transports,
		leaderID:   leaderID,
		isLeader:   true,
		nodeID:     "node-1",
	}
}

// AddPeer is a no-op to support existing main.go initialization.
func (s *MetadataStore) AddPeer(peerID string, peer *MetadataStore) {
	// Our nodes are already set up and connected in the 3-node cluster.
}

// SetSimulateFailure simulates network partition / failure on a node.
func (s *MetadataStore) SetSimulateFailure(fail bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.simulateFailure = fail

	if fail {
		// Shutdown peer nodes to simulate failure of majority
		for _, peer := range s.peers {
			_ = peer.Shutdown().Error()
		}
	}
}

func (s *MetadataStore) applyCommand(cmd RaftCommand) error {
	s.mu.RLock()
	if s.simulateFailure {
		s.mu.RUnlock()
		return fmt.Errorf("consensus failure: node simulated failure")
	}
	s.mu.RUnlock()

	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	var targetRaft *raft.Raft
	if s.raftNode.State() == raft.Leader {
		targetRaft = s.raftNode
	} else {
		for _, node := range s.peers {
			if node.State() == raft.Leader {
				targetRaft = node
				break
			}
		}
	}

	if targetRaft == nil {
		return fmt.Errorf("no leader found in Raft cluster")
	}

	future := targetRaft.Apply(data, 1*time.Second)
	if err := future.Error(); err != nil {
		return err
	}

	res := future.Response()
	if err, ok := res.(error); ok && err != nil {
		return err
	}

	return nil
}

func (s *MetadataStore) IsLeaderStatus() bool {
	return s.raftNode.State() == raft.Leader || s.raftNode.State() == raft.Candidate
}

func (s *MetadataStore) GetLeaderID() string {
	_, id := s.raftNode.LeaderWithID()
	if id != "" {
		return string(id)
	}
	return s.leaderID
}

func (s *MetadataStore) RegisterWorker(w *WorkerConfig) error {
	cmd := RaftCommand{
		OpType:       "REGISTER_WORKER",
		Key:          w.WorkerID,
		WorkerConfig: w,
	}
	return s.applyCommand(cmd)
}

func (s *MetadataStore) UpdateWorkerHeartbeat(workerID string, cpuPct float32, memMB int64) error {
	cmd := RaftCommand{
		OpType:       "UPDATE_WORKER_HEARTBEAT",
		Key:          workerID,
		CpuUsagePct:  cpuPct,
		MemoryUsedMB: memMB,
	}
	return s.applyCommand(cmd)
}

func (s *MetadataStore) SetWorkerActiveStatus(workerID string, active bool) {
	cmd := RaftCommand{
		OpType: "SET_WORKER_ACTIVE",
		Key:    workerID,
		Active: active,
	}
	_ = s.applyCommand(cmd)
}

func (s *MetadataStore) GetActiveWorkers() []*WorkerConfig {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()

	var active []*WorkerConfig
	for _, w := range s.fsm.workers {
		if w.Active {
			wCopy := *w
			active = append(active, &wCopy)
		}
	}
	return active
}

func (s *MetadataStore) GetAllWorkers() []*WorkerConfig {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()

	var all []*WorkerConfig
	for _, w := range s.fsm.workers {
		wCopy := *w
		all = append(all, &wCopy)
	}
	return all
}

func (s *MetadataStore) AddTable(table string, schema string) error {
	cmd := RaftCommand{
		OpType: "ADD_TABLE",
		Key:    table,
		Schema: schema,
	}
	return s.applyCommand(cmd)
}

func (s *MetadataStore) UpdateTableStats(tableName string, stats *TableStats) error {
	cmd := RaftCommand{
		OpType:     "UPDATE_TABLE_STATS",
		Key:        tableName,
		TableStats: stats,
	}
	return s.applyCommand(cmd)
}

func (s *MetadataStore) GetTableStats(tableName string) (*TableStats, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()

	tbl, exists := s.fsm.catalog[tableName]
	if !exists || tbl.Stats == nil {
		return nil, fmt.Errorf("stats not found for table %s", tableName)
	}
	return tbl.Stats, nil
}

func (s *MetadataStore) AddPartition(p PartitionInfo) error {
	cmd := RaftCommand{
		OpType:        "ADD_PARTITION",
		Key:           fmt.Sprintf("%d:%s", p.PartitionID, p.TaskID),
		PartitionInfo: &p,
	}
	return s.applyCommand(cmd)
}

func (s *MetadataStore) ClearPartitions() {
	cmd := RaftCommand{
		OpType: "CLEAR_PARTITIONS",
	}
	_ = s.applyCommand(cmd)
}

func (s *MetadataStore) GetPartitions(partitionID int32) []PartitionInfo {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()

	parts, exists := s.fsm.partitions[partitionID]
	if !exists {
		return nil
	}
	res := make([]PartitionInfo, len(parts))
	copy(res, parts)
	return res
}

func (s *MetadataStore) GetAllPartitions() map[int32][]PartitionInfo {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()

	res := make(map[int32][]PartitionInfo)
	for k, v := range s.fsm.partitions {
		resCopy := make([]PartitionInfo, len(v))
		copy(resCopy, v)
		res[k] = resCopy
	}
	return res
}

func (s *MetadataStore) RemovePartitionsForTasks(taskIDs []string) {
	cmd := RaftCommand{
		OpType:  "REMOVE_PARTITIONS_FOR_TASKS",
		TaskIDs: taskIDs,
	}
	_ = s.applyCommand(cmd)
}

func (s *MetadataStore) RemovePartitionsForWorker(workerID string) {
	cmd := RaftCommand{
		OpType: "REMOVE_PARTITIONS_FOR_WORKER",
		Key:    workerID,
	}
	_ = s.applyCommand(cmd)
}
