package runtime

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/tkeel-io/core/pkg/dispatch"
	zfield "github.com/tkeel-io/core/pkg/logger"
	"github.com/tkeel-io/core/pkg/placement"
	"github.com/tkeel-io/core/pkg/repository"
	xkafka "github.com/tkeel-io/core/pkg/util/kafka"
	"github.com/tkeel-io/kit/log"
)

type NodeConf struct {
	Sources []string
}

type Node struct {
	runtimes map[string]*Runtime
	dispatch dispatch.Dispatcher
	repo     repository.IRepository

	ctx    context.Context
	cancel context.CancelFunc
}

func NewNode(ctx context.Context, repo repository.IRepository, dispatcher dispatch.Dispatcher) *Node {
	ctx, cacel := context.WithCancel(ctx)
	return &Node{
		ctx:      ctx,
		cancel:   cacel,
		repo:     repo,
		dispatch: dispatcher,
		runtimes: make(map[string]*Runtime),
	}
}

func (n *Node) Start(cfg NodeConf) error {
	log.Info("start node...")

	for index := range cfg.Sources {
		sourceIns, err := xkafka.NewKafkaPubsub(cfg.Sources[index])
		if nil != err {
			return errors.Wrap(err, "create source instance")
		}

		if err = sourceIns.Received(n.ctx, n); nil != err {
			return errors.Wrap(err, "consume source")
		}

		placement.Global().Append(placement.Info{ID: sourceIns.ID(), Flag: true})
	}

	return nil
}

func (n *Node) HandleMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	rid := msg.Topic
	if _, has := n.runtimes[rid]; !has {
		log.Info("create container", zfield.ID(rid))
		n.runtimes[rid] = NewRuntime(n.ctx, rid)
	}

	// load runtime spec.
	runtime := n.runtimes[rid]
	runtime.DeliveredEvent(context.Background(), msg)
	return nil
}
