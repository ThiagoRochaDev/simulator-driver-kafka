package kafka

import (
	"encoding/json"
	"fmt"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	route2 "github.com/estudoimersaocycle/simulator-driver-kafka/application/route"
	"github.com/estudoimersaocycle/simulator-driver-kafka/infra/kafka"
	"log"
	"os"
	"time"
)

// Produce is responsible to publish the positions of each request
// Example of a json request:
//{"clientId":"1","routeId":"1"}
//{"clientId":"2","routeId":"2"}
//{"clientId":"3","routeId":"3"}
func Produce(msg *ckafka.Message) {
	producer := kafka.NewkafkaProducer()
	route := route2.NewRoute()
	json.Unmarshal(msg.Value, &route)
	route.LoadPositions()
	positions, err := route.ExportJsonPositions()
	if err != nil {
		log.Println(err.Error())
	}
	fmt.Println("kafka produce has started")
	for _, p := range positions {
		kafka.Publish(p, os.Getenv("KAFKAPRODUCETOPIC"), producer)
		time.Sleep(time.Millisecond * 500)
	}
}
