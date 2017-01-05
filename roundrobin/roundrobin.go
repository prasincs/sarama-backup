/*
 The simplest partitioner: assign partitions round-robin

 This partitioner is not that always suitable because any change
 in the consumer group results in an almost complete reassignment of
 partitions.

  Copyright 2016 MistSys
*/

package roundrobin

import (
	"fmt"

	"github.com/Shopify/sarama"
)

// a simple partitioner that assigns partitions round-robin across all consumers requesting each topic
type roundRobinPartitioner string

// global instance of the round-robin partitioner
const RoundRobin roundRobinPartitioner = "roundrobin" // use the string "roundrobin" without a dash to match what kafka java code uses, should someone want to mix go and java consumers in the same group

func (rr roundRobinPartitioner) PrepareJoin(jreq *sarama.JoinGroupRequest, topics []string, current map[string][]int32) {
	jreq.AddGroupProtocolMetadata(string(rr),
		&sarama.ConsumerGroupMemberMetadata{
			Version: 1,
			Topics:  topics,
		})
}

// for each topic in jresp, assign the topic's partitions round-robin across the members requesting each topic
func (roundRobinPartitioner) Partition(sreq *sarama.SyncGroupRequest, jresp *sarama.JoinGroupResponse, client sarama.Client) error {
	by_member, err := jresp.GetMembers() // map of member to metadata
	//dbgf("by_member %v", by_member)
	if err != nil {
		return err
	}
	// invert the data, so we have the requests grouped by topic (they arrived grouped by member, since the kafka broker treats the data from each consumer as an opaque blob, so it couldn't do this step for us)
	by_topic := make(map[string][]string) // map of topic to members requesting the topic
	for member, request := range by_member {
		if request.Version != 1 {
			// skip unsupported versions. we'll only assign to clients we can understand. Since we are such a client
			// we won't block all consumers (at least for those topics we consume). If this ends up a bad idea, we
			// can always change this code to return an error.
			continue
		}
		for _, topic := range request.Topics {
			by_topic[topic] = append(by_topic[topic], member)
		}
	}
	//dbgf("by_topic %v", by_topic)

	// finally, build our assignments of partitions to members
	assignments := make(map[string]map[string][]int32, len(by_member)) // map of member to topics, and topic to partitions
	for topic, members := range by_topic {
		partitions, err := client.Partitions(topic)
		//dbgf("Partitions(%q) = %v", topic, partitions)
		if err != nil {
			// what to do? we could maybe skip the topic, assigning it to no-one. But I/O errors are likely to happen again.
			// so let's stop partitioning and return the error.
			return err
		}
		n := len(partitions)
		if n == 0 { // can this happen? best not to /0 later if it can
			// no one gets anything assigned. it is as if this topic didn't exist
			continue
		}

		for i := 0; i < n; {
			for _, member_id := range members {
				topics, ok := assignments[member_id]
				if !ok {
					topics = make(map[string][]int32, len(by_topic)) // capacity is a guess (and an upper bound)
					assignments[member_id] = topics
				}
				topics[topic] = append(topics[topic], partitions[i])
				i++
				if i == n {
					break
				}
			}
		}
	}
	//dbgf("assignments %v", assignments)

	// and encode the assignments in the sync request
	for member_id, topics := range assignments {
		sreq.AddGroupAssignmentMember(member_id,
			&sarama.ConsumerGroupMemberAssignment{
				Version: 1,
				Topics:  topics,
			})
	}

	return nil
}

func (roundRobinPartitioner) ParseSync(sresp *sarama.SyncGroupResponse) (map[string][]int32, error) {
	if len(sresp.MemberAssignment) == 0 {
		// in the corner case that we ask for no topics, we get nothing back. However sarama fd498173ae2bf (head of master branch Nov 6th 2016) will return a useless error if we call sresp.GetMemberAssignment() in this case
		return nil, nil
	}
	ma, err := sresp.GetMemberAssignment()
	//dbgf("MemberAssignment %v", ma)
	if err != nil {
		return nil, err
	}
	if ma.Version != 1 {
		return nil, fmt.Errorf("unsupported MemberAssignment version %d", ma.Version)
	}
	return ma.Topics, nil
}
