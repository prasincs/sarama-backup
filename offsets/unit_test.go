package offsets

import (
	"math"
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

func TestNoOlderThanNow(t *testing.T) {
	sa, ooor := NoOlderThan(0)
	cl := &fakeClient{}

	o, err := sa("topic1", 0, 100, cl)
	if err != nil {
		t.Error(err)
	}
	if o != sarama.OffsetNewest {
		t.Error("unexpected", o)
	}

	o, err = ooor("topic1", 0, cl)
	if err != nil {
		t.Error(err)
	}
	if o != sarama.OffsetNewest {
		t.Error("unexpected", o)
	}
}

func TestNoOlderThan1Minute(t *testing.T) {
	sa, ooor := NoOlderThan(time.Minute)

	cl := &fakeClient{}

	approx := time.Now().Add(-time.Minute).UnixNano()
	o, err := sa("topic1", 0, 100, cl)
	if err != nil {
		t.Error(err)
	}
	if math.Abs(float64(o-approx)) > 1000000 {
		t.Error("unexpected", o)
	}

	approx = time.Now().Add(-time.Minute).UnixNano()
	o, err = ooor("topic1", 0, cl)
	if err != nil {
		t.Error(err)
	}
	if math.Abs(float64(o-approx)) > 1000000 {
		t.Error("unexpected", o)
	}
}

// a fake sarama.Client which maps time -> offsets using time
type fakeClient struct{}

func (*fakeClient) Config() *sarama.Config                                         { return nil }
func (*fakeClient) Brokers() []*sarama.Broker                                      { return nil }
func (*fakeClient) Topics() ([]string, error)                                      { return nil, nil }
func (*fakeClient) Partitions(topic string) ([]int32, error)                       { return nil, nil }
func (*fakeClient) WritablePartitions(topic string) ([]int32, error)               { return nil, nil }
func (*fakeClient) Leader(topic string, partitionID int32) (*sarama.Broker, error) { return nil, nil }
func (*fakeClient) Replicas(topic string, partitionID int32) ([]int32, error)      { return nil, nil }
func (*fakeClient) RefreshMetadata(topics ...string) error                         { return nil }
func (*fakeClient) Coordinator(consumerGroup string) (*sarama.Broker, error)       { return nil, nil }
func (*fakeClient) RefreshCoordinator(consumerGroup string) error                  { return nil }
func (*fakeClient) Close() error                                                   { return nil }
func (*fakeClient) Closed() bool                                                   { return false }

func (*fakeClient) GetOffset(topic string, partition int32, ts_msec int64) (int64, error) {
	return time.Unix(ts_msec/1000, (ts_msec%1000)*1000000).UnixNano(), nil
}
