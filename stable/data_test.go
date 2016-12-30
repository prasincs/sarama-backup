package stable

import (
	"reflect"
	"testing"
)

func TestDataMarshal(t *testing.T) {
	var d = data{
		version: 1,
		assignments: map[string][]int32{
			"topic1": []int32{1, 2, 99},
			"topic2": []int32{0},
			"topic3": []int32{},
		},
	}

	b := d.marshal()
	t.Logf("% x", b)
	var d2 data
	err := d2.unmarshal(b)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(d, d2) {
		t.Logf("d = %v", d)
		t.Logf("d2 = %v", d2)
		t.Error("unmarshal(marshal(x)) != x")
	}
}
