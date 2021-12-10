package raft

import (
	"context"
	"io"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"github.com/tkeel-io/core/pkg/placement/hashing"
)

// CommandType is the type of raft command in log entry.
type CommandType uint8

const (
	StateUpsert      CommandType = 0
	StateRemove      CommandType = 1
	MemberUpsert     CommandType = 2
	MasterUpsert     CommandType = 3
	MemberRemove     CommandType = 4
	MasterRemove     CommandType = 5
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

func (fsm *FSM) upsertMember(cmdData []byte) (bool, error) {
	var host hashing.Host
	if err := unmarshalMsgPack(cmdData, &host); err != nil {
		return false, err
	}
	log.Infof("apply member log, id: %s, name: %s, port: %d", host.AppID, host.Name, host.Port)

	fsm.lock.RLock()
	defer fsm.lock.RUnlock()

	return fsm.state.Consistent.Add(host.Name, host.AppID, host.Port), nil
}

func (fsm *FSM) removeMember(cmdData []byte) (bool, error) {
	var host hashing.Host
	if err := unmarshalMsgPack(cmdData, &host); err != nil {
		return false, err
	}

	fsm.lock.RLock()
	defer fsm.lock.RUnlock()

	return fsm.state.Consistent.Remove(host.Name), nil
}

func (fsm *FSM) upsertMaster(cmdData []byte) (bool, error) {
	var host hashing.Host
	if err := unmarshalMsgPack(cmdData, &host); err != nil {
		return false, err
	}

	log.Infof("apply master log, id: %s, name: %s, port: %d", host.AppID, host.Name, host.Port)

	fsm.lock.RLock()
	defer fsm.lock.RUnlock()
	fsm.state.MasterHost = &host
	return fsm.state.Consistent.Add(host.Name, host.AppID, host.Port), nil
}

// Apply log is invoked once a log entry is committed.
// It returns a value which will be made available in the
// ApplyFuture returned by Raft.Apply method if that
// method was called on the same Raft node as the FSM.
func (fsm *FSM) Apply(logData *raft.Log) interface{} {
	var (
		err     error
		updated bool
	)

	switch CommandType(logData.Data[0]) {
	case MemberUpsert:
		updated, err = fsm.upsertMember(logData.Data[1:])
	case MemberRemove:
		updated, err = fsm.removeMember(logData.Data[1:])
	case MasterUpsert:
		updated, err = fsm.upsertMaster(logData.Data[1:])
	default:
		err = errors.New("unimplemented command")
	}

	if err != nil {
		log.Errorf("fsm apply entry log failed. data: %s, error: %s",
			string(logData.Data), err.Error())
		return false
	}

	return updated
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

func (fsm *FSM) LookFor(ctx context.Context, id string) *State {
	return fsm.state.States[id]
}

func (fsm *FSM) GetHost(ctx context.Context, id string) (*hashing.Host, error) {
	host, err := fsm.state.Consistent.GetHost(id)
	return host, errors.Wrap(err, "GetHost failed")
}

func (fsm *FSM) GetMaster(ctx context.Context) *hashing.Host {
	return fsm.state.MasterHost
}
