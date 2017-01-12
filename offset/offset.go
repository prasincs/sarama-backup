/*
  Utility functions for controlling starting and out-of-range offsets

  These are useful in conjunction with the sarama-consumer.Config.StartingOffset
  and .OffsetOutOfRange configuration options.

  Copyright 2017 MistSys
*/

package offset

import (
	"time"

	"github.com/Shopify/sarama"
	consumer "github.com/mistsys/sarama-consumer"
)

// return a StartingOffset function which starts the consuming at no more than 'max' time in the past
// and a OffsetOutOfRange function which jumps forward to no more than 'max' time in the past
func NoOlderThan(max time.Duration) (consumer.StartingOffset, consumer.OffsetOutOfRange) {

	return func(topic string, partition int32, committed_offset int64, client sarama.Client) (offset int64, err error) {
			if offset == sarama.OffsetNewest {
				// start with the sarama configured starting point
				offset = client.Config().Consumer.Offsets.Initial
			}
			ts := time.Now().UTC().Add(-max).UnixNano() / 1000000 // millisecond timestamp of oldest time at which we want to find ourselves
			ts_offset, err := client.GetOffset(topic, partition, ts)
			if err == sarama.ErrOffsetOutOfRange {
				return offset, nil
			}
			if err != nil {
				return offset, err
			}
			if offset < ts_offset { // NOTE this works correctly when offset is sarama.OffsetNewest or OffsetOldest, both of which are negative
				// jump ahead to ts_offset
				offset = ts_offset
			}
			return offset, nil
		},
		func(topic string, partition int32, client sarama.Client) (offset int64, err error) {
			ts := time.Now().UTC().Add(-max).UnixNano() / 1000000 // millisecond timestamp of oldest time at which we want to find ourselves
			ts_offset, err := client.GetOffset(topic, partition, ts)
			if err == sarama.ErrOffsetOutOfRange {
				// max time ago is too far back (assuming the local time is valid). so start at the oldest offset
				return sarama.OffsetOldest, nil
			}
			return ts_offset, err
		}
}
