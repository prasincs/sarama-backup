/*
  A simple kafka consumer-group client

  Copyright 2016 MistSys
*/

package stable_test

import (
	"fmt"
	"testing"

	"github.com/Shopify/sarama"
	consumer "github.com/mistsys/sarama-consumer"
	"github.com/mistsys/sarama-consumer/stable"
)

func TestRoundRobin(t *testing.T) {
	var rr consumer.Partitioner = stable.Stable

	all_topics := []string{"topic1", "topic2", "topic3", "topic4"}

	var mock_client = mockClient{
		config: sarama.NewConfig(),
		partitions: map[string][]int32{
			"topic1": []int32{0, 1 /*,no 2*/, 3, 4, 5, 6, 7}, // note we pretend partition 2 is offline
			"topic2": []int32{0, 1},
			"topic3": []int32{0, 1, 2, 3, 4, 5}, // exactly 2 partitions per member
			"topic4": []int32{0, 1, 2},          // exactly 1 partitions per member
		},
	}

	// pretend to have 3 members, with no current assignments, all asking for the four topics
	var jreqs [3]sarama.JoinGroupRequest
	for i := range jreqs {
		jreqs[i].GroupId = "group"
		jreqs[i].MemberId = fmt.Sprintf("member%d", i)
		jreqs[i].ProtocolType = "consumer"
		rr.PrepareJoin(&jreqs[i], all_topics, nil)

		t.Logf("JoinGroupRequests[%d] = %v\n", i, jreqs[i])
	}

	var jresp = sarama.JoinGroupResponse{
		GenerationId:  1,
		GroupProtocol: string(stable.Stable),
		Members:       make(map[string][]byte),
	}
	for i := range jreqs {
		jresp.Members[jreqs[i].MemberId] = jreqs[i].GroupProtocols[string(stable.Stable)]
	}
	t.Logf("JoinGroupResponse = %v\n", jresp)

	var sreq = sarama.SyncGroupRequest{
		GroupId:      "group",
		GenerationId: 1,
		MemberId:     "member0",
	}
	err := rr.Partition(&sreq, &jresp, &mock_client)
	t.Logf("SyncGroupRequest = %v\n", sreq)
	if err != nil {
		t.Fatal(err)
	}

	assignments := make(map[string]map[string][]int32) // map of member to topic to the assigned list of partitions
	for i := range jreqs {
		id := jreqs[i].MemberId
		var sresp = sarama.SyncGroupResponse{
			MemberAssignment: sreq.GroupAssignments[id],
		}

		act, err := rr.ParseSync(&sresp)
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("%s assignment %v\n", id, act)

		assignments[id] = act
	}

	for _, topic := range all_topics {
		low := 1 << 30
		high := 0
		used := make(map[int32]string)
		for id, act := range assignments {
			a := act[topic]

			// keep track of the high and low # of partitions that have been assigned
			n := len(a)
			if low > n {
				low = n
			}
			if high < n {
				high = n
			}

			// and make sure no partition is assigned twice
			for id2, p := range a {
				if _, ok := used[p]; ok {
					t.Errorf("Partition %d of topic %q is assigned to multiple consumers, including %v and %v", p, topic, id, id2)
				}
				used[p] = id
			}
		}
		t.Logf("topic %q assigned partitions as %v", topic, used)
		if low != high && low != high-1 {
			t.Errorf("Partition assignment of topic %q is uneven. Some have %d and some have %d", topic, low, high)
		}
	}
}

// mock sarama.Client which implements the metadata API sufficiently for our unit test purposes
type mockClient struct {
	config     *sarama.Config
	partitions map[string][]int32
}

func (mc *mockClient) Config() *sarama.Config {
	return mc.config
}

func (mc *mockClient) Topics() ([]string, error) {
	var topics = make([]string, 0, len(mc.partitions))
	for t := range mc.partitions {
		topics = append(topics, t)
	}
	return topics, nil
}

func (mc *mockClient) Partitions(topic string) ([]int32, error) {
	if p, ok := mc.partitions[topic]; ok {
		return p, nil
	}
	return nil, sarama.ErrUnknownTopicOrPartition
}

func (mc *mockClient) WritablePartitions(topic string) ([]int32, error) {
	return mc.Partitions(topic)
}

func (mc *mockClient) Leader(topic string, part int32) (*sarama.Broker, error) {
	return nil, sarama.ErrBrokerNotAvailable
}

func (mc *mockClient) Replicas(topic string, part int32) ([]int32, error) {
	return nil, sarama.ErrNotEnoughReplicas
}

func (mc *mockClient) RefreshMetadata(topics ...string) error {
	return nil
}

func (mc *mockClient) GetOffset(topic string, part int32, time int64) (int64, error) {
	return 0, nil
}

func (mc *mockClient) Coordinator(group string) (*sarama.Broker, error) {
	return nil, sarama.ErrBrokerNotAvailable
}

func (mc *mockClient) RefreshCoordinator(group string) error {
	return nil
}

func (mc *mockClient) Close() error {
	return nil
}

func (mc *mockClient) Closed() bool {
	return false
}
