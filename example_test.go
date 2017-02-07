/*
  A simple kafka consumer-group client

  Copyright 2016 MistSys
*/

package consumer_test

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	consumer "github.com/mistsys/sarama-consumer"
	"github.com/mistsys/sarama-consumer/offsets"
	"github.com/mistsys/sarama-consumer/stable"
)

func ExampleNewClient() {
	// create a suitable sarama.Client
	sconfig := sarama.NewConfig()
	sconfig.Version = consumer.MinVersion // consumer requires at least 0.9
	sconfig.Consumer.Return.Errors = true // needed if asynchronous ErrOffsetOutOfRange handling is desired (it's a good idea)
	sclient, _ := sarama.NewClient([]string{"kafka-broker:9092"}, sconfig)

	// from that, create a consumer.Config with some fancy options
	config := consumer.NewConfig()
	config.Partitioner = stable.New(false)                                                 // use a stable (but inconsistent) partitioner
	config.StartingOffset, config.OffsetOutOfRange = offsets.NoOlderThan(time.Second * 30) // always start and restart no more than 30 seconds in the past (NOTE: requires kafka 0.10 brokers to work properly)

	// and finally a consumer Client
	client, _ := consumer.NewClient("group_name", config, sclient)
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

	// process messages
	for msg := range topic_consumer.Messages() {
		fmt.Println("processing message", msg)
		topic_consumer.Done(msg) // required
	}
}
