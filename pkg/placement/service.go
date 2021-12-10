package placement

import (
	"context"
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/tkeel-io/core/pkg/placement/hashing"
	pb "github.com/tkeel-io/core/pkg/placement/proto/v1"
	"github.com/tkeel-io/core/pkg/placement/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (ps *PlacementService) LookFor(ctx context.Context, in *pb.LookForReq) (*pb.LookForResp, error) {
	if state := ps.raftNode.FSM().LookFor(ctx, in.Id); nil != state {
		return &pb.LookForResp{State: state.ConvertTo()}, nil
	}
	log.Infof("LookFor..., leader address {%s}, memberships %v",
		ps.raftNode.Raft().Leader(), ps.raftNode.Raft().GetConfiguration().Configuration())
	if ps.hasLeaderShip.Load() {
		host, err := ps.raftNode.FSM().GetHost(ctx, in.Id)
		if nil != err {
			log.Errorf("look for state failed, %s", err.Error())
			return nil, errors.Wrap(err, "look for state failed")
		}
		return &pb.LookForResp{State: &pb.State{Id: in.Id, Host: host.Name}}, nil
	}

	// dispatch call-package to master node.
	client, err := ps.masterConn.GetClient()
	if nil != err {
		log.Errorf("look for state failed, %s", err.Error())
		return nil, errors.Wrap(err, "look for state failed")
	}

	res, err := client.LookFor(ctx, in)
	return res, errors.Wrap(err, "slave look for state failed")
}

func (ps *PlacementService) RepotStatus(stream pb.Placement_RepotStatusServer) error {
	var hostName string
	ps.streamConnGroup.Add(1)
	defer func() {
		ps.streamConnGroup.Done()
		ps.deleteStreamConn(stream)
	}()

	for ps.hasLeaderShip.Load() {
		req, err := stream.Recv()
		switch err { //nolint
		case nil:
			if hostName == "" {
				hostName = req.Name
				ps.addStreamConn(stream)
				ps.raftNode.ApplyCommand(raft.MemberUpsert, hashing.Host{
					Name:  req.Name,
					Port:  req.Port,
					AppID: req.Id,
				})
			}

			// Record the heartbeat timestamp. This timestamp will be used to check if the member
			// state maintained by raft is valid or not. If the member is outdated based the timestamp
			// the member will be marked as faulty node and removed.
			ps.lastHeartBeat.Store(req.Name, time.Now().UnixNano())

			for _, state := range req.States {
				ps.raftNode.ApplyCommand(raft.StateUpsert, raft.State{
					ID:   state.Id,
					Host: state.Host,
				})
			}
		default:
			if hostName == "" {
				log.Errorf("stream is disconnected before member is added, %s", err.Error())
				return nil
			}

			if errors.Is(err, io.EOF) {
				log.Debugf("Stream connection is disconnected gracefully: %s", hostName)
				ps.raftNode.ApplyCommand(raft.MemberRemove, hashing.Host{Name: hostName})
			} else {
				// no actions for hashing table. Instead, MembershipChangeWorker will check
				// host updatedAt and if now - updatedAt > p.faultyHostDetectDuration, remove hosts.
				log.Debugf("Stream connection is disconnected with the error: %v", err)
			}

			return nil
		}
	}

	return errors.Wrap(status.Error(codes.FailedPrecondition,
		"only leader can serve the request"), "server not leader")
}

// addStreamConn adds stream connection between runtime and placement to the dissemination pool.
func (ps *PlacementService) addStreamConn(conn placementStream) {
	ps.streamConnPoolLock.Lock()
	ps.streamConnPool = append(ps.streamConnPool, conn)
	ps.streamConnPoolLock.Unlock()
}

func (ps *PlacementService) deleteStreamConn(conn placementStream) {
	ps.streamConnPoolLock.Lock()
	for i, c := range ps.streamConnPool {
		if c == conn {
			ps.streamConnPool = append(ps.streamConnPool[:i], ps.streamConnPool[i+1:]...)
			break
		}
	}
	ps.streamConnPoolLock.Unlock()
}
