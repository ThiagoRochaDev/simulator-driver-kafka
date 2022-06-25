package kafka

import (
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"os"
)

func NewkafkaProducer() *ckafka.Producer {
	ConfigMap := &ckafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KAFKABOOTSTRAPSERVERS"),
	}
	p, err := ckafka.NewProducer(ConfigMap)
	if err != nil {
		log.Println(err.Error())
	}
	return p
}

func Publish(msg string, topic string, producer *ckafka.Producer) error {
	message := &ckafka.Message{
		TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: ckafka.PartitionAny},
		Value:          []byte(msg),
	}

	err := producer.Produce(message, nil)
	if err != nil {
		return err
	}
	return nil
}
