package raft

import (
	"testing"
)

func TestRaftReplicationSuccess(t *testing.T) {
	store := NewMetadataStore("master-node")

	// Verify table addition propagates to FSM
	err := store.AddTable("test_table", "id INT")
	if err != nil {
		t.Fatalf("Expected AddTable to succeed, got %v", err)
	}

	// Read table schema from store FSM
	store.fsm.mu.RLock()
	tbl, exists := store.fsm.catalog["test_table"]
	store.fsm.mu.RUnlock()
	if !exists || tbl.Schema != "id INT" {
		t.Errorf("Expected test_table with schema 'id INT' in FSM")
	}
}

func TestRaftReplicationRollbackOnFailure(t *testing.T) {
	store := NewMetadataStore("master-node")

	// Trigger simulated failure (which shuts down peer nodes)
	store.SetSimulateFailure(true)

	// Since peer nodes are shut down, consensus cannot be reached
	err := store.AddTable("failed_table", "id INT")
	if err == nil {
		t.Fatalf("Expected AddTable to fail due to simulated lack of majority consensus")
	}

	// Verify table does not exist in FSM
	store.fsm.mu.RLock()
	_, exists := store.fsm.catalog["failed_table"]
	store.fsm.mu.RUnlock()
	if exists {
		t.Errorf("Expected failed_table to not exist in FSM after failure")
	}
}
