
## sarama-consumer --- a kafka consumer group client

This is as simple a kafka consumer-group client package as I can
create. I deliberately expose sarama types like sarama.Message
and sarama.Client. I don't think wrapping APIs in what is basically
a fancy utility API is wise. (and each wrapper wastes CPU and
creates more garbage for gc to collect)

Simplest usage, a perpetual consumer of a single topic:

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


