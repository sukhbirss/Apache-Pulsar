package main

import (
	"bufio"
	"context"
	"log"
	"os"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

func main() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               "pulsar://54.196.148.93:6650",
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "my-topic",
	})

	reader := bufio.NewReader(os.Stdin)
	var stop bool = false
	for !stop {
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte(input),
		})
		if input == "EXIT" {
			stop = true
		}
	}
}
