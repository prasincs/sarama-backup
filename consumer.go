/*
  simple kafka consumer group client

  Copyright 2016 MistSys
*/

package consumer

import "github.com/Shopify/sarama"

/*
  New creates a new consumer group client on top of an existing
  sarama.Client.

  The consumer group name is used to match this client with other
  instances running elsewhere, but connected to the same cluster
  of kafka brokers and using the same consumer group name.
*/
func New(group_name string, sarama_client sarama.Client) (Client, error) {
	cl := &client{
		client:     sarama_client,
		group_name: group_name,
	}

	return cl, nil
}

/*
  Client is a kafaka client belonging to a consumer group.
*/
type Client interface {
	Consume(topic string) (Consumer, error)
	// TODO custom balancer here
}

/*
  Consumer is a consumer of a topic.

  Messages from any partition assigned to this client arrive on the
  Messages channel, and errors arrive on the Errors channel. These operate
  the same as Messages and Errors in sarama.PartitionConsumer, except
  that messages and errors from any partition are mixed together.

  Every message read from the Messages channel must be eventually passed
  to Done. Calling Done is the signal that that message has been consumed
  and the offset of that message can be comitted back to kafka.

  Of course this requires that the message's Partition and Offset fields not
  be altered.
*/
type Consumer interface {
	// Messages returns the channel of messages arriving from kafka. It always
	// returns the same result, so it is safe to call once and store the result.
	// Every message read from the channel should be passed to Done when processing
	// of the message is complete.
	Messages() <-chan *sarama.ConsumerMessage

	// Done indicates the processing of the message is complete, and its offset can
	// be comitted to kafka. Calling Done twice with the same message, or with a
	// garbage message, can cause panics.
	Done(*sarama.ConsumerMessage)

	// Errors returns the channel of errors. These include errors from the underlying
	// partitions, as well as offset commit errors. Note that sarama's Config.Consumer.Return.Errors
	// is false by default, and without that most errors that occur within sarama are logged rather
	// than returned.
	Errors() <-chan error // TODO consider a custom error type that embodies the source

	// AsyncClose terminates the consumer cleanly. Callers should continue to read from
	// Messages and Errors channels until they are closed. You must call AsyncClose before
	// closing the underlying sarama.Client.
	AsyncClose()
}

// client implements the Client interface
type client struct {
	client     sarama.Client // the sarama client we were constructed from
	group_name string        // the client-group name
}

func (cl *client) Consume(topic string) (Consumer, error) {
	return nil, nil
}

// consumer implements the Consumer interface
type consumer struct {
	client *client
	topic  string

	messages chan *sarama.ConsumerMessage
	errors   chan error
}

func (con *consumer) Messages() <-chan *sarama.ConsumerMessage { return con.messages }
func (con *consumer) Errors() <-chan error                     { return con.errors }

func (con *consumer) Done(*sarama.ConsumerMessage) {
}

func (con *consumer) AsyncClose() {
}
