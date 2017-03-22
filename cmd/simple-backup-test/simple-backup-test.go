package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	consumer "github.com/prasincs/sarama-backup"
	"github.com/prasincs/sarama-backup/offsets"
	"github.com/prasincs/sarama-backup/stable"
)

func main() {
	// create a suitable sarama.Client
	sconfig := sarama.NewConfig()
	sconfig.Version = consumer.MinVersion // consumer requires at least 0.9
	sconfig.Consumer.Return.Errors = true // needed if asynchronous ErrOffsetOutOfRange handling is desired (it's a good idea)
	sclient, _ := sarama.NewClient([]string{"kafka-broker:9092"}, sconfig)

	// from that, create a consumer.Config with some fancy options
	config := consumer.NewConfig()
	config.Partitioner = stable.New(false)                                                 // use a stable (but inconsistent) partitioner
	config.StartingOffset, config.OffsetOutOfRange = offsets.NoOlderThan(time.Second * 30) // always start and restart no more than 30 seconds in the past (NOTE: requires kafka 0.10 brokers to work properly)

	go func() {
		producer, err := sarama.NewAsyncProducerFromClient(sclient)
		if err != nil {
			log.Fatalf("Failed to create a producer.")
		}
		defer producer.Close()
		ticker := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-ticker.C:
				kmsg := &sarama.ProducerMessage{
					Topic: "topic1",
					Key:   sarama.ByteEncoder("boo"),
					Value: sarama.ByteEncoder([]byte("boo " + strconv.Itoa(int(time.Now().Unix())))),
				}
				producer.Input() <- kmsg
			}
		}
	}()

	// and finally a consumer Client
	client, _ := consumer.NewClient("example_group_name", config, sclient)
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

	var lastOffset int64 = 0
	var lastPartition int32 = 0

	var count = 0
	time.Sleep(10 * time.Second)
	// process messages
	for msg := range topic_consumer.Messages() {
		fmt.Println("processing message", msg)
		count += 1
		if count == 10 {
			break
		}
		lastOffset = msg.Offset
		lastPartition = msg.Partition
	}

	fmt.Println(lastOffset)

	topic_consumer.Commit("topic1", lastPartition, lastOffset)
	//topic_consumer.Done(&sarama.ConsumerMessage{
	//	Topic:     "topic1",
	//	Partition: 0,
	//	Offset:    lastOffset,
	//})

	// this is here because I removed committing from Close()
	time.Sleep(1 * time.Second)

	//Output: Done
}
