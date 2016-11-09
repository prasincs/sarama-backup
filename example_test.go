/*
  A simple kafka consumer-group client

  Copyright 2016 MistSys
*/

package consumer

import (
	"fmt"

	"github.com/Shopify/sarama"
)

func ExampleNewClient() {
	// create a suitable sarama.Client
	sconfig := sarama.NewConfig()
	sconfig.Version = MinVersion          // consumer requires at least 0.9
	sconfig.Consumer.Return.Errors = true // needed if asynchronous ErrOffsetOutOfRange handling is desired (it's a good idea)
	sclient, _ := sarama.NewClient([]string{"kafka-broker:9092"}, sconfig)

	// from that, create a consumer.Client with a default configuration
	client, _ := NewClient("group_name", nil, sclient)
	defer client.Close() // not strictly necessary, since we don't exit, but this is example code and someone might C&V it and exit

	// consume and print errors
	go func() {
		for err := range client.Errors() {
			fmt.Println(err)
		}
	}()

	// consume a topic
	topic_consumer, _ := client.Consume("topic1")
	defer topic_consumer.AsyncClose() // same comment as for client.Close() above

	// process messages and log errors
	for msg := range topic_consumer.Messages() {
		fmt.Println("processing message", msg)
		topic_consumer.Done(msg) // required
	}
}
