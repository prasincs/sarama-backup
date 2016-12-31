
## sarama-consumer --- a kafka consumer group client

This is as simple a kafka consumer-group client package as I can
create. I deliberately expose sarama types like sarama.Message
and sarama.Client. I don't think wrapping APIs in what is basically
a fancy utility API is wise. (and each wrapper wastes CPU and
creates more garbage for gc to collect)

The assignment of partitions to consumers is pluggable. Two partitioners
are included: the trivial round-robin, and a stable & consistent partitioner.
The stable partitioner keeps the assignment of partitions to consumers 
as stable as it can across time. With it, restarting a consumer within the
kafka heartbeat timeout (typically 10s of seconds) does not disturb the partition
assignments of the other consumers, and adding and removing consumers
reassigns the minimum number of partitions.

The stable partitioner can optionally also be consistent across topics.
When enabled, all topics with the same number of partitions and the same
consumer group members are assigned the same partition->consumer mapping.
This is useful if the messages in the topics use the same partitioning
key, and the consumers benefit from having messages with the same key
(but in different topics) be processed in the same consumer.


Simplest usage, a perpetual consumer of a single topic with default
(round-robin) partitioning:

    import "github.com/mistsys/sarama-consumer"
    import "github.com/Shopify/sarama"

    func main() {
      cfg := sarama.NewConfig()
      cfg.Version = consumer.MinVersion // needed until sarama defaults to >= 0.9
      client, err := sarama.NewClient(..., cfg)
      consumer,err := consumer.NewClient("my group", nil, client).Consume("my topic")
      for {
        select {
          case msg := <-consumer.Output():
            // process the *sarama.Message 'msg'
            consumer.Done(msg)
          case err := <-consumer.Errors():
            // log, whatever
        }
      }
    }

