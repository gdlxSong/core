package raft

import (
	"github.com/tkeel-io/core/pkg/placement/hashing"
)

type State struct {
	Id      string
	Host    string
	Version int64
}

// StateTable 应该保证这个的全局唯一。
type StateTable struct {
	Consiste *hashing.Consistent
	States   map[string]State
}

func NewStateTable() *StateTable {
	return &StateTable{
		Consiste: hashing.NewConsistentHash(),
		States:   make(map[string]State),
	}
}
