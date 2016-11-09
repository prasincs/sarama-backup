/*
Package consumer provides kafka 0.9 consumer groups on top of the low level Sarama kafka package.

Consumer groups distribute topics' partitions dynamically across group members,
and restart at the last comitted offset of each partition.

This requires Kafka v0.9+ and follows the steps guide, described in:
https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal

CONFIGURATION

Two customization APIs may be set in the Config:

Config.Offset.OffsetOutOfRange func(topic, partition, sarama.Client) (restart_offset int64, error)
allows users to decide how to react to falling off the tail of the kafka log. The default is to
restart at the newest offset. However depending on the use case restarting at an offset T time in
the past, or even the oldest offset, may make more sense.

Config.Partitioner interface allows users to control how the consumer group distributes partitions
across the group members. The default is to distribute the partitions of each topic round-robin across
the members. However a custom partitioner may want to use a more stable hash to prevent disturbing
so many mappings. And since all topics are partitioned at once, a custom Partitioner can assign matching
partitions from different topics to the same consumer (useful when two topics are keyed identically and
the same key in either topic should arrive at the same consumer).

PHILOSOPHY

The consumer API has three rules: messages must be passed to Consumer.Done() once each message does
not need to be replayed, Client.Errors() must be consumed, and Client.Close or Consumer.AsyncClose() must
be called to clean up resources if your code wishes to stop consuming messages.

Kafka's rule that [if consumers keep up] all messages will be seen at least once, and possibly
many times always applies.

The API of this package deliberately does not wrap or otherwise hide the underlying sarama API.
I believe doing so is a waste of CPU time, generates more work for the gc, and makes building on top of
a package harder than it should be. It also makes no assumptions about how the caller's work should be done.
There are no requirements to process messages in order, nor does it dictate a go-routine organization
on the caller. I've applied RFC1925 #5 and #12 as best I can.

I've used other kafka APIs which did wrap and impose structure and found them difficult to really use,
and as a reaction I try not to impose such APIs on others (nor on myself) even if it means the calling
code is a little more complex.

(For example you have to create a suitably configured samara.Client yourself before calling NewClient.
That's 3 more lines of code, but it also lets you tune the samara.Client's config just as you need it
to be, or even mock the client for test.)

The simple use case of this package is shown in the NewClient example code.

*/
package consumer
