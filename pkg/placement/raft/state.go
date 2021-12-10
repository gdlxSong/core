package raft

import (
	"github.com/tkeel-io/core/pkg/placement/hashing"
	pb "github.com/tkeel-io/core/pkg/placement/proto/v1"
)

type State struct {
	ID      string
	Host    string
	Version int64
}

// StateTable 应该保证这个的全局唯一。
type StateTable struct {
	States     map[string]*State
	MasterHost *hashing.Host
	Consistent *hashing.Consistent
}

func NewStateTable() *StateTable {
	return &StateTable{
		States:     make(map[string]*State),
		Consistent: hashing.NewConsistentHash(),
	}
}

func (s *State) ConvertTo() *pb.State {
	return &pb.State{
		Id:      s.ID,
		Host:    s.Host,
		Version: s.Version,
	}
}
