package kafka

import (
	"fmt"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"os"
)

type kafkaConsumer struct {
	MsgChan chan *ckafka.Message
}

func NewKafkaConsumer(msgChan chan *ckafka.Message) *kafkaConsumer {
	return &kafkaConsumer{MsgChan: msgChan}
}

func (k *kafkaConsumer) Consume() {
	configMap := &ckafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KAFKABOOTSTRAPSERVERS"),
		"group.id":          os.Getenv("KAFKACONSUMERGROUPID"),
	}
	consumer, err := ckafka.NewConsumer(configMap)
	if err != nil {
		log.Fatalf("error consuming kafka" + err.Error())
	}
	topics := []string{os.Getenv("KAFKAREADTOPIC")}
	consumer.SubscribeTopics(topics, nil)
	fmt.Println("kafka consumer has started")
	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			k.MsgChan <- msg
		}
	}
}
