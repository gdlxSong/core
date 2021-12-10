package placement

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/tkeel-io/core/pkg/config"
	"github.com/tkeel-io/core/pkg/logger"
	"github.com/tkeel-io/core/pkg/placement/hashing"
	pb "github.com/tkeel-io/core/pkg/placement/proto/v1"
	"github.com/tkeel-io/core/pkg/placement/raft"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
)

var log = logger.NewLogger("core.placement")

type placementStream pb.Placement_RepotStatusServer
type placementRecvStream pb.Placement_RepotStatusClient

type PlacementService struct { //nolint
	pb.UnimplementedPlacementServer
	serverListener net.Listener
	grpcServer     *grpc.Server
	raftNode       *raft.Server
	shutdown       chan struct{}
	// slave Grpc connections for master node.
	streamConnPool     []placementStream
	streamConnGroup    sync.WaitGroup
	streamConnPoolLock sync.RWMutex
	// master Grpc connection for slave node.
	masterConn    *placementGRPCConn
	lastHeartBeat sync.Map
	hasLeaderShip atomic.Bool

	ctx    context.Context
	cancel context.CancelFunc
}

func NewPlacementService(ctx context.Context, raftNode *raft.Server) *PlacementService {
	ctx, cancel := context.WithCancel(ctx)
	return &PlacementService{
		ctx:            ctx,
		cancel:         cancel,
		raftNode:       raftNode,
		shutdown:       make(chan struct{}),
		streamConnPool: make([]placementStream, 0),
		masterConn:     newPlacementGRPCConn(""),
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

	return nil
}

func (ps *PlacementService) MonitorLeadership() { //nolint
	log.Infof("Placement server is moniting...")

	for {
		select {
		case isLeader := <-ps.raftNode.Raft().LeaderCh():
			if isLeader {
				ps.hasLeaderShip.Store(true)
				log.Infof("cluster leadership acquired, leader address {%s}, memberships %v",
					ps.raftNode.Raft().Leader(), ps.raftNode.Raft().GetConfiguration().Configuration())

				if _, err := ps.raftNode.ApplyCommand(raft.MasterUpsert, hashing.Host{
					Name:  config.GetConfig().Server.Name,
					Port:  int64(config.GetConfig().Placement.Port),
					AppID: config.GetConfig().Server.AppID,
				}); nil != err {
					log.Errorf("apply log failed, leader address {%s}, memberships %v",
						ps.raftNode.Raft().Leader(), ps.raftNode.Raft().GetConfiguration().Configuration())
				}
			} else {
				ps.hasLeaderShip.Store(false)
				for _, stream := range ps.streamConnPool {
					stream.SendAndClose(&pb.ReportStatusResp{})
				}
				log.Infof("cluster leadership{%s} lost", ps.raftNode.Address())
			}
		case <-ps.shutdown:
			return
		case <-time.NewTicker(3 * time.Second).C:
			if ps.raftNode.Raft().Leader() == "" {
				log.Errorf("cluster lost leader, leader address {%s}, memberships %v",
					ps.raftNode.Raft().Leader(), ps.raftNode.Raft().GetConfiguration().Configuration())
			} else if !ps.hasLeaderShip.Load() {
				for n := 0; n < 3; n++ {
					stream, err := ps.masterConn.GetStream()
					if nil != err {
						log.Errorf("get stream failed, %s", err.Error())
						ps.masterConn.Reset().Adapt(ps.getPlacementLeaderAddress())
						continue
					}

					if err = stream.Send(&pb.ReportStatusReq{
						Id:   config.GetConfig().Server.AppID,
						Name: config.GetConfig().Server.Name,
						Port: int64(config.GetConfig().Placement.Port),
					}); nil != err {
						log.Errorf("send heart beat failed, %s", err.Error())
					} else {
						log.Infof("heart beat at %s", time.Now().UTC().String())
					}

					break
				}
			}
		}
	}
}

func (ps *PlacementService) getPlacementLeaderAddress() string {
	if ps.raftNode.Raft().Leader() == "" {
		return ""
	} else if masterHost := ps.raftNode.FSM().GetMaster(context.Background()); nil == masterHost {
		return ""
	} else {
		ip := strings.Split(string(ps.raftNode.Raft().Leader()), ":")[0]
		return fmt.Sprintf("%s:%d", ip, masterHost.Port)
	}
}

type placementGRPCConn struct {
	masterAddr       string
	waitCh           chan struct{}
	grpcConnection   *grpc.ClientConn
	placementClient  pb.PlacementClient
	masterRecvStream placementRecvStream
}

func newPlacementGRPCConn(addr string) *placementGRPCConn {
	return &placementGRPCConn{
		masterAddr: addr,
		waitCh:     make(chan struct{}, 1),
	}
}

func (p *placementGRPCConn) Adapt(addr string) *placementGRPCConn {
	if p.masterAddr != addr {
		p.Close()
		p.masterAddr = addr
	}
	return p
}

func (p *placementGRPCConn) Reset() *placementGRPCConn {
	p.Close()
	return p
}

func (p *placementGRPCConn) GetClient() (pb.PlacementClient, error) {
	if p.masterAddr == "" {
		return nil, ErrMissingAddress
	} else if nil == p.placementClient {
		p.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		conn, err := grpc.DialContext(ctx, p.masterAddr, grpc.WithInsecure())
		if nil != err {
			return nil, errors.Wrap(err, "create Grpc connection failed")
		}

		p.grpcConnection = conn
		p.placementClient = pb.NewPlacementClient(conn)
	}
	return p.placementClient, nil
}

func (p *placementGRPCConn) GetStream() (placementRecvStream, error) {
	if nil == p.masterRecvStream {
		client, err := p.GetClient()
		if nil != err {
			return nil, errors.Wrap(err, "create placement stream failed")
		}
		stream, err := client.RepotStatus(context.Background())
		if nil != err {
			return nil, errors.Wrap(err, "create placement stream failed")
		}

		// wait master reply.
		// go func() {
		// 	if _, err := p.masterRecvStream.CloseAndRecv(); nil != err {
		// 		log.Errorf("cluster leadership reply, %s", err.Error())
		// 	}
		// 	p.waitCh <- struct{}{}
		// }()

		p.masterRecvStream = stream
	}

	return p.masterRecvStream, nil
}

func (p *placementGRPCConn) Close() {
	if nil != p.grpcConnection {
		p.grpcConnection.Close()
	}

	p.grpcConnection = nil
	p.masterRecvStream = nil
	p.placementClient = nil
	p.waitCh = make(chan struct{}, 1)
}
