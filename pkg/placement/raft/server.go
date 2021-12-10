package raft

import (
	"net"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/pkg/errors"
)

const (
	logStorePrefix    = "log-"
	snapshotsRetained = 2

	raftLogCacheSize = 512

	commandTimeout = 1 * time.Second

	nameResolveRetryInterval = 2 * time.Second
	nameResolveMaxRetry      = 120
)

// PeerInfo represents raft peer node information.
type PeerInfo struct {
	ID      string
	Address string
}

// Server is Raft server implementation.
type Server struct {
	id  string
	fsm *FSM

	inMem    bool
	raftBind string
	peers    []PeerInfo

	config        *raft.Config
	raft          *raft.Raft
	raftStore     *raftboltdb.BoltStore
	raftTransport *raft.NetworkTransport

	logStore    raft.LogStore
	stableStore raft.StableStore
	snapStore   raft.SnapshotStore

	raftLogStorePath string
}

// New creates Raft server node.
func New(id string, inMem bool, peers []PeerInfo, logStorePath string) *Server {
	return &Server{
		id:               id,
		inMem:            inMem,
		peers:            peers,
		raftLogStorePath: logStorePath,
	}
}

func (s *Server) init() error {
	for _, peer := range s.peers {
		if s.id == peer.ID {
			s.raftBind = peer.Address
		}
	}

	if s.raftBind == "" {
		return ErrEmptyRaftAddress
	}
	return nil
}

// StartRaft starts Raft node with Raft protocol configuration. if config is nil,
// the default config will be used.
func (s *Server) StartRaft(config *raft.Config) error { //nolint
	// If we have an unclean exit then attempt to close the Raft store.
	defer func() {
		if s.raft == nil && s.raftStore != nil {
			if err := s.raftStore.Close(); err != nil {
				log.Errorf("failed to close log storage: %v", err)
			}
		}
	}()

	var err error
	var addr *net.TCPAddr

	s.fsm = newFSM()
	loggerAdapter := newLoggerAdapter()
	if err = s.init(); nil != err {
		return errors.Wrap(err, "raft server start failed")
	} else if addr, err = tryResolveRaftAdvertiseAddr(s.raftBind); nil != err {
		return errors.Wrap(err, "raft server start failed")
	} else if s.raftTransport, err = raft.NewTCPTransportWithLogger(s.raftBind, addr, 3, 10*time.Second, loggerAdapter); nil != err {
		return errors.Wrap(err, "raft server start failed")
	}

	// Build an all in-memory setup for dev mode, otherwise prepare a full
	// disk-based setup.
	if s.inMem {
		raftInmem := raft.NewInmemStore()
		s.stableStore = raftInmem
		s.logStore = raftInmem
		s.snapStore = raft.NewInmemSnapshotStore()
	} else {
		if err = ensureDir(s.raftStorePath()); err != nil {
			return errors.Wrap(err, "failed to create log store directory")
		}

		// Create the backend raft store for logs and stable storage.
		s.raftStore, err = raftboltdb.NewBoltStore(filepath.Join(s.raftStorePath(), "raft.db"))
		if err != nil {
			return errors.Wrap(err, "start raft server failed")
		}

		s.stableStore = s.raftStore

		// Wrap the store in a LogCache to improve performance.
		s.logStore, err = raft.NewLogCache(raftLogCacheSize, s.raftStore)
		if err != nil {
			return errors.Wrap(err, "start raft server failed")
		}

		// Create the snapshot store.
		s.snapStore, err = raft.NewFileSnapshotStoreWithLogger(s.raftStorePath(), snapshotsRetained, loggerAdapter)
		if err != nil {
			return errors.Wrap(err, "start raft server failed")
		}
	}

	// Setup Raft configuration.
	if config == nil {
		// Set default configuration for raft
		s.config = &raft.Config{
			ProtocolVersion:    raft.ProtocolVersionMax,
			HeartbeatTimeout:   1000 * time.Millisecond,
			ElectionTimeout:    1000 * time.Millisecond,
			CommitTimeout:      50 * time.Millisecond,
			MaxAppendEntries:   64,
			ShutdownOnRemove:   true,
			TrailingLogs:       10240,
			SnapshotInterval:   120 * time.Second,
			SnapshotThreshold:  8192,
			LeaderLeaseTimeout: 500 * time.Millisecond,
		}
	} else {
		s.config = config
	}

	s.config.Logger = loggerAdapter
	s.config.LocalID = raft.ServerID(s.id)

	// If we are in bootstrap or dev mode and the state is clean then we can
	// bootstrap now.
	bootstrapConf, err := s.bootstrapConfig(s.peers)
	if err != nil {
		return errors.Wrap(err, "start raft server failed")
	}

	if bootstrapConf != nil {
		if err = raft.BootstrapCluster(
			s.config, s.logStore, s.stableStore,
			s.snapStore, s.raftTransport, *bootstrapConf); err != nil {
			return errors.Wrap(err, "start raft server failed")
		}
	}

	s.raft, err = raft.NewRaft(s.config, s.fsm, s.logStore, s.stableStore, s.snapStore, s.raftTransport)

	log.Infof("Raft server is starting on %s...", s.raftBind)
	return errors.Wrap(err, "start raft server failed")
}

func (s *Server) bootstrapConfig(peers []PeerInfo) (*raft.Configuration, error) {
	hasState, err := raft.HasExistingState(s.logStore, s.stableStore, s.snapStore)
	if err != nil {
		return nil, errors.Wrap(err, "bootstrap raft server failed")
	}

	if !hasState {
		raftConfig := &raft.Configuration{
			Servers: make([]raft.Server, len(peers)),
		}

		for i, p := range peers {
			raftConfig.Servers[i] = raft.Server{
				ID:      raft.ServerID(p.ID),
				Address: raft.ServerAddress(p.Address),
			}
		}

		return raftConfig, nil
	}

	// return nil for raft.Configuration to use the existing log store files.
	return nil, nil
}

func (s *Server) raftStorePath() string {
	if s.raftLogStorePath == "" {
		return logStorePrefix + s.id
	}
	return s.raftLogStorePath
}

// FSM returns fsm.
func (s *Server) FSM() *FSM {
	return s.fsm
}

// Raft returns raft node.
func (s *Server) Raft() *raft.Raft {
	return s.raft
}

// IsLeader returns true if the current node is leader.
func (s *Server) IsLeader() bool {
	return s.raft.State() == raft.Leader
}

// ApplyCommand applies command log to state machine to upsert or remove members.
func (s *Server) ApplyCommand(cmdType CommandType, data interface{}) (bool, error) {
	if !s.IsLeader() {
		return false, errors.New("this is not the leader node")
	}

	cmdLog, err := makeRaftLogCommand(cmdType, data)
	if err != nil {
		return false, err
	}

	future := s.raft.Apply(cmdLog, commandTimeout)
	if err := future.Error(); err != nil {
		return false, errors.Wrap(err, "apply log failed")
	}

	resp := future.Response()
	return resp.(bool), nil //nolint
}

func (s *Server) Address() string {
	return s.raftBind
}

// Shutdown shutdown raft server gracefully.
func (s *Server) Shutdown() {
	if s.raft != nil {
		s.raftTransport.Close()
		future := s.raft.Shutdown()
		if err := future.Error(); err != nil {
			log.Warnf("error shutting down raft: %v", err)
		}
		if s.raftStore != nil {
			s.raftStore.Close()
		}
	}
}
