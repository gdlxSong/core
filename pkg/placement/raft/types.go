package raft

import "github.com/pkg/errors"

var (
	ErrHostNotFound     = errors.New("host not found")
	ErrEmptyRaftAddress = errors.New("empty raft address")
)
