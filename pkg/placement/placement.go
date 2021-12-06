package placement

import (
	"context"
	"fmt"
	"net"

	"github.com/pkg/errors"
	"github.com/tkeel-io/core/pkg/logger"
	pb "github.com/tkeel-io/core/pkg/placement/proto/v1"
	"github.com/tkeel-io/core/pkg/placement/raft"
	"google.golang.org/grpc"
)

var log = logger.NewLogger("core.placement")

type placementStream = pb.Placement_RepotStatusServer

type PlacementService struct {
	pb.UnimplementedPlacementServer
	serverListener net.Listener
	grpcServer     *grpc.Server
	raftNode       *raft.Server
	streamConnPool []placementStream

	ctx    context.Context
	cancel context.CancelFunc
}

func NewPlacementService(ctx context.Context, raftNode *raft.Server) *PlacementService {
	ctx, cancel := context.WithCancel(ctx)
	return &PlacementService{
		ctx:            ctx,
		cancel:         cancel,
		raftNode:       raftNode,
		streamConnPool: make([]placementStream, 0),
	}
}

// Run starts the placement service gRPC server.
func (ps *PlacementService) Start(port string) error {
	var err error
	ps.serverListener, err = net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer([]grpc.ServerOption{}...)
	pb.RegisterPlacementServer(grpcServer, ps)
	ps.grpcServer = grpcServer
	log.Infof("Placement server is starting on %s...", ps.serverListener.Addr().String())

	if err := grpcServer.Serve(ps.serverListener); err != nil {
		log.Errorf("failed to serve: %v", err)
		return errors.Wrap(err, "start placement service failed")
	}

	log.Infof("Placement server is starting on %s...", ps.serverListener.Addr().String())

	return nil
}

func (ps *PlacementService) Register(context.Context, *pb.RegisterStatemReq) (*pb.RegisterStatemResp, error) {
	panic("implement me.")
}

func (ps *PlacementService) RepotStatus(pb.Placement_RepotStatusServer) error {
	panic("implement me.")
}
