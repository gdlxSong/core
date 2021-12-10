package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	Core_v1 "github.com/tkeel-io/core/api/core/v1"
	"github.com/tkeel-io/core/pkg/config"
	"github.com/tkeel-io/core/pkg/entities"
	"github.com/tkeel-io/core/pkg/placement"
	"github.com/tkeel-io/core/pkg/placement/raft"
	"github.com/tkeel-io/core/pkg/search"
	"github.com/tkeel-io/core/pkg/server"
	"github.com/tkeel-io/core/pkg/service"

	"github.com/panjf2000/ants/v2"
	"github.com/tkeel-io/kit/app"
	"github.com/tkeel-io/kit/log"
	"github.com/tkeel-io/kit/transport"
)

var (
	CfgFile       string
	SearchBrokers string
)

func init() {
	flag.StringVar(&CfgFile, "conf", "config.yml", "core configuration file.")
	flag.StringVar(&SearchBrokers, "search_brokers", "http://localhost:9200", "search brokers address.")
}

func main() {
	flag.Parse()
	config.InitConfig(CfgFile)

	cfg := config.GetConfig()

	httpSrv := server.NewHTTPServer(fmt.Sprintf(":%d", cfg.Server.HTTPPort))
	grpcSrv := server.NewGRPCServer(fmt.Sprintf(":%d", cfg.Server.GrpcPort))
	serverList := []transport.Server{httpSrv, grpcSrv}

	coreApp := app.New(cfg.Server.Name,
		&log.Conf{
			App:   cfg.Server.Name,
			Level: cfg.Logger.Level,
			Dev:   true,
		},
		serverList...,
	)

	coroutinePool, err := ants.NewPool(100)
	if nil != err {
		log.Fatal(err)
	}

	searchClient := search.NewESClient(strings.Split(SearchBrokers, ",")...)
	entityManager, err := entities.NewEntityManager(context.Background(), coroutinePool, searchClient)
	if nil != err {
		log.Fatal(err)
	}

	{
		// User service
		// create coroutine pool.

		EntitySrv, err := service.NewEntityService(context.Background(), entityManager, searchClient)
		if nil != err {
			log.Fatal(err)
		}
		Core_v1.RegisterEntityHTTPServer(httpSrv.Container, EntitySrv)
		Core_v1.RegisterEntityServer(grpcSrv.GetServe(), EntitySrv)

		SubscriptionSrv, err := service.NewSubscriptionService(context.Background(), entityManager)
		if nil != err {
			log.Fatal(err)
		}
		Core_v1.RegisterSubscriptionHTTPServer(httpSrv.Container, SubscriptionSrv)
		Core_v1.RegisterSubscriptionServer(grpcSrv.GetServe(), SubscriptionSrv)

		TopicSrv, err := service.NewTopicService(context.Background(), entityManager)
		if nil != err {
			log.Fatal(err)
		}
		Core_v1.RegisterTopicHTTPServer(httpSrv.Container, TopicSrv)
		Core_v1.RegisterTopicServer(grpcSrv.GetServe(), TopicSrv)

		SearchSrv := service.NewSearchService(searchClient)
		Core_v1.RegisterSearchHTTPServer(httpSrv.Container, SearchSrv)
		Core_v1.RegisterSearchServer(grpcSrv.GetServe(), SearchSrv)
	}

	// run.
	if err := entityManager.Start(); nil != err {
		panic(err)
	} else if err = coreApp.Run(context.TODO()); err != nil {
		panic(err)
	}

	// new placement.
	go newPlacementServer()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, os.Interrupt)
	<-stop

	if err := coreApp.Stop(context.TODO()); err != nil {
		panic(err)
	}
}

func newPlacementServer() {
	var err error
	cfg := config.GetConfig()
	peers := make([]raft.PeerInfo, 0)
	for _, peer := range cfg.Placement.Raft.Servers {
		peers = append(peers, raft.PeerInfo{
			ID:      peer.ID,
			Address: peer.Addr,
		})
	}

	raftServer := raft.New(cfg.Server.Name, true, peers, cfg.Placement.Raft.LogStorePath)
	if err = raftServer.StartRaft(nil); nil != err {
		panic(err)
	}

	placementServ := placement.NewPlacementService(context.Background(), raftServer)
	go func() {
		if err = placementServ.Start(strconv.FormatInt(int64(cfg.Placement.Port), 10)); nil != err {
			os.Kill.Signal()
		}
	}()

	placementServ.MonitorLeadership()
}
