package kafka

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/dapr/kit/retry"
	"github.com/pkg/errors"
	v1 "github.com/tkeel-io/core/api/core/v1"
	xerrors "github.com/tkeel-io/core/pkg/errors"
	zfield "github.com/tkeel-io/core/pkg/logger"
	"github.com/tkeel-io/core/pkg/resource/pubsub"
	"github.com/tkeel-io/kit/log"
	"go.uber.org/zap"
)

type kafkaMetadata struct {
	Topic   string   `json:"topic" mapstructure:"topic"`
	Group   string   `json:"group" mapstructure:"group"`
	Brokers []string `json:"brokers" mapstructure:"brokers"`
	Timeout int64    `json:"timeout" mapstructure:"timeout"`
}

func newKafkaPubsub(id string, kafkaMeta *kafkaMetadata) (pubsub.Pubsub, error) {
	var (
		err      error
		client   sarama.Client
		consumer sarama.ConsumerGroup
		producer sarama.SyncProducer
	)

	kafkaCfg := sarama.NewConfig()
	kafkaCfg.Version = sarama.V2_3_0_0
	kafkaCfg.Producer.Retry.Max = 3
	kafkaCfg.Producer.RequiredAcks = sarama.WaitForAll
	kafkaCfg.Producer.Return.Successes = true
	if client, err = sarama.NewClient(kafkaMeta.Brokers, kafkaCfg); nil != err {
		return nil, errors.Wrap(err, "create kafka client instance")
	} else if producer, err = sarama.NewSyncProducerFromClient(client); nil != err {
		return nil, errors.Wrap(err, "create kafka producer instance")
	} else if consumer, err = sarama.NewConsumerGroupFromClient(kafkaMeta.Group, client); nil != err {
		return nil, errors.Wrap(err, "create kafka consumer instance")
	}

	return &kafkaPubsub{
		id:            id,
		kafkaClient:   client,
		kafkaConsumer: consumer,
		kafkaProducer: producer,
		kafkaMetadata: *kafkaMeta,
	}, nil
}

type kafkaPubsub struct {
	id            string
	kafkaClient   sarama.Client
	kafkaConsumer sarama.ConsumerGroup
	kafkaProducer sarama.SyncProducer
	kafkaMetadata kafkaMetadata
}

func (k *kafkaPubsub) ID() string {
	return k.id
}

func (k *kafkaPubsub) Send(ctx context.Context, event v1.Event) error {
	log.L().Debug("pubsub.kafka send", zfield.Message(event), zfield.Topic(k.kafkaMetadata.Topic),
		zfield.ID(k.id), zfield.Endpoints(k.kafkaMetadata.Brokers), zfield.Group(k.kafkaMetadata.Group))

	var (
		err      error
		bytes    []byte
		entityID string
	)

	entityID = event.Entity()
	bytes, err = v1.Marshal(event)
	if nil != err {
		return errors.Wrap(err, "encode event")
	}

	msg := &sarama.ProducerMessage{
		Key:   sarama.StringEncoder(entityID),
		Topic: k.kafkaMetadata.Topic,
		Value: sarama.ByteEncoder(bytes),
	}

	_, _, err = k.kafkaProducer.SendMessage(msg)

	return errors.Wrap(err, "kafka client send message")
}

func (k *kafkaPubsub) Received(ctx context.Context, receiver pubsub.EventHandler) error {
	log.L().Debug("start receive", zfield.ID(k.id), zfield.Topic(k.kafkaMetadata.Topic),
		zfield.Endpoints(k.kafkaMetadata.Brokers), zfield.Group(k.kafkaMetadata.Group))
	go func() {
		defer func() {
			log.L().Debug("Closing ConsumerGroup for topics", zfield.Topic(k.kafkaMetadata.Topic),
				zfield.ID(k.id), zfield.Endpoints(k.kafkaMetadata.Brokers), zfield.Group(k.kafkaMetadata.Group))
			if err := k.kafkaConsumer.Close(); err != nil {
				log.L().Error("Error closing consumer group", zap.Error(err), zfield.Topic(k.kafkaMetadata.Topic),
					zfield.ID(k.id), zfield.Endpoints(k.kafkaMetadata.Brokers), zfield.Group(k.kafkaMetadata.Group))
			}
		}()

		log.L().Debug("Subscribed and listening to topics", zfield.Topic(k.kafkaMetadata.Topic),
			zfield.ID(k.id), zfield.Endpoints(k.kafkaMetadata.Brokers), zfield.Group(k.kafkaMetadata.Group))

		for {
			// Consume the requested topic.
			if innerError := k.kafkaConsumer.Consume(ctx, []string{k.kafkaMetadata.Topic}, &kafkaConsumer{receiverHandler: receiver}); innerError != nil {
				log.L().Error("Error closing consumer group", zap.Error(innerError), zfield.Topic(k.kafkaMetadata.Topic),
					zfield.ID(k.id), zfield.Endpoints(k.kafkaMetadata.Brokers), zfield.Group(k.kafkaMetadata.Group))
			}

			if ctx.Err() != nil {
				log.L().Error("Context error, stopping consumer", zap.Error(ctx.Err()), zfield.Topic(k.kafkaMetadata.Topic),
					zfield.ID(k.id), zfield.Endpoints(k.kafkaMetadata.Brokers), zfield.Group(k.kafkaMetadata.Group))
				return
			}
		}
	}()
	return nil
}

func (k *kafkaPubsub) Commit(v interface{}) error {
	return nil
}

func (k *kafkaPubsub) Close() error {
	log.L().Info("pubsub.noop close", zfield.ID(k.id))
	return nil
}

type kafkaConsumer struct {
	receiverHandler pubsub.EventHandler
}

func (consumer *kafkaConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	if consumer.receiverHandler == nil {
		return fmt.Errorf("nil consumer callback")
	}

	backOffConfig := retry.Config{}
	b := backOffConfig.NewBackOffWithContext(session.Context())
	for msg := range claim.Messages() {
		if err := retry.NotifyRecover(func() error {
			var innerErr error
			log.L().Debug("processing kafka message", zfield.Topic(msg.Topic),
				zfield.Partition(msg.Partition), zfield.Offset(msg.Offset), zfield.Key(string(msg.Key)))

			var ev v1.ProtoEvent
			if innerErr = v1.Unmarshal(msg.Value, &ev); nil != innerErr {
				log.L().Error("processing kafka message", zfield.Topic(msg.Topic),
					zfield.Partition(msg.Partition), zfield.Offset(msg.Offset), zfield.Key(string(msg.Key)))
				return errors.Wrap(innerErr, "decode event")
			} else if innerErr = consumer.receiverHandler(session.Context(), &ev); innerErr == nil {
				session.MarkMessage(msg, "")
			}
			log.L().Error("processing kafka message", zfield.Topic(msg.Topic),
				zfield.Partition(msg.Partition), zfield.Offset(msg.Offset), zfield.Key(string(msg.Key)))
			return errors.Wrap(innerErr, "handle message")
		}, b, func(err error, d time.Duration) {
			log.L().Debug("processing kafka message", zfield.Topic(msg.Topic),
				zfield.Partition(msg.Partition), zfield.Offset(msg.Offset), zfield.Key(string(msg.Key)))
		}, func() {
			log.L().Debug("processing kafka message", zfield.Topic(msg.Topic),
				zfield.Partition(msg.Partition), zfield.Offset(msg.Offset), zfield.Key(string(msg.Key)))
		}); err != nil {
			log.L().Error("processing kafka message", zfield.Topic(msg.Topic),
				zfield.Partition(msg.Partition), zfield.Offset(msg.Offset), zfield.Key(string(msg.Key)))
			return errors.Wrap(err, "handle message")
		}
	}

	return nil
}

func (consumer *kafkaConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *kafkaConsumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func init() {
	log.SuccessStatusEvent(os.Stdout, "Register Resource<pubsub.kafka> successful")
	pubsub.Register("kafka", func(id string, urlText string) (pubsub.Pubsub, error) {
		log.L().Info("create pubsub.kafka instance", zfield.ID(id), zfield.URL(urlText))

		kafkaMeta, err := parseURL(urlText)
		if nil != err {
			log.L().Error("create pubsub.kafka instance",
				zap.Error(err), zfield.ID(id), zfield.URL(urlText))
			return nil, errors.Wrap(err, "parse configuration from url")
		}
		pubsubIns, err := newKafkaPubsub(id, kafkaMeta)
		return pubsubIns, errors.Wrap(err, "new kafka instance")
	})
}

// kafka://localhost:9092/topic/group
func parseURL(urlText string) (*kafkaMetadata, error) {
	urlIns, err := url.Parse(urlText)
	if nil != err {
		return nil, errors.Wrap(err, "parse configuration from url")
	}

	segs := strings.Split(urlIns.Path, "/")
	if len(segs) != 3 {
		return nil, xerrors.ErrInvalidParam
	}

	return &kafkaMetadata{
		Topic:   segs[1],
		Group:   segs[2],
		Brokers: strings.Split(urlIns.Host, ","),
		Timeout: 30,
	}, nil
}
