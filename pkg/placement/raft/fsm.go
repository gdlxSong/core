package raft

import (
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

// CommandType is the type of raft command in log entry.
type CommandType uint8

const (
	// MemberUpsert is the command to update or insert new or existing member info.
	MemberUpsert CommandType = 0
	// MemberRemove is the command to remove member from actor host member state.
	MemberRemove CommandType = 1

	// TableDisseminate is the reserved command for dissemination loop.
	TableDisseminate CommandType = 100
)

type FSM struct {
	lock  sync.RWMutex
	state *StateTable
}

func newFSM() *FSM {
	return &FSM{
		lock:  sync.RWMutex{},
		state: NewStateTable(),
	}
}

// Apply log is invoked once a log entry is committed.
// It returns a value which will be made available in the
// ApplyFuture returned by Raft.Apply method if that
// method was called on the same Raft node as the FSM.
func (fsm *FSM) Apply(*raft.Log) interface{} {
	panic("implement me.")
}

// Snapshot is used to support log compaction. This call should
// return an FSMSnapshot which can be used to save a point-in-time
// snapshot of the FSM. Apply and Snapshot are not called in multiple
// threads, but Apply will be called concurrently with Persist. This means
// the FSM should be implemented in a fashion that allows for concurrent
// updates while a snapshot is happening.
func (fsm *FSM) Snapshot() (raft.FSMSnapshot, error) {
	panic("implement me.")
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state.
func (fsm *FSM) Restore(io.ReadCloser) error {
	panic("implement me.")
}
