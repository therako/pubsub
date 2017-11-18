package pubsub

import (
	"log"
	"strconv"

	"cloud.google.com/go/pubsub"
	cache "github.com/patrickmn/go-cache"
	"golang.org/x/net/context"
)

type MessageQueue interface {
	Init() error
	Close() error
	Publish(string, *PubSubData) *pubsub.PublishResult
}

type PubSubData struct {
	Id        string
	Timestamp int64
	Data      []byte
}

type PubSubQueue struct {
	PubsubClient *pubsub.Client
	ctx          context.Context
	topics       *cache.Cache
}

func (pubsubqueue *PubSubQueue) Init(GoogleProjectName string) error {
	var err error
	pubsubqueue.ctx, pubsubqueue.PubsubClient, err = configurePubsub(GoogleProjectName)
	if err != nil {
		log.Fatalln("Error in client connections to PubSub", err)
		return err
	}
	pubsubqueue.topics = cache.New(cache.NoExpiration, cache.NoExpiration)
	return nil
}

func (pubsubqueue *PubSubQueue) Close() error {
	for _, item := range pubsubqueue.topics.Items() {
		if topic, ok := item.Object.(*pubsub.Topic); ok {
			topic.Stop()
		}
	}
	return nil
}

func configurePubsub(projectID string) (context.Context, *pubsub.Client, error) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, nil, err
	}

	return ctx, client, nil
}

func (pubsubqueue *PubSubQueue) Publish(topicName string, pubSubData *PubSubData) *pubsub.PublishResult {
	var topic *pubsub.Topic
	if t, ok := pubsubqueue.topics.Get(topicName); ok {
		if to, ok := t.(*pubsub.Topic); ok {
			topic = to
		}
	}
	if topic == nil {
		topic = pubsubqueue.PubsubClient.Topic(topicName)
		pubsubqueue.topics.SetDefault(topicName, topic)
	}
	attributes := map[string]string{
		"id":        pubSubData.Id,
		"timestamp": strconv.FormatInt(pubSubData.Timestamp, 10),
	}
	publishResult := topic.Publish(pubsubqueue.ctx, &pubsub.Message{Data: pubSubData.Data, Attributes: attributes})
	return publishResult
}
