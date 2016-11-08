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

	// from that, create a consumer.Client
	client, _ := NewClient("group_name", nil, sclient)
	defer client.Close()

	// consume a topic
	topic_consumer, _ := client.Consume("topic1")
	defer topic_consumer.AsyncClose()

	// process messages and log errors
	for {
		select {
		case m := <-topic_consumer.Messages():
			fmt.Println("processing message", m)
			topic_consumer.Done(m) // required
		case e := <-client.Errors(): // asynchronous errors
			fmt.Println("error", e)
		case e := <-topic_consumer.Errors():
			fmt.Println("error", e)
		}
	}
}
