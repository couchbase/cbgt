//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package cbgt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestVersionGTE(t *testing.T) {
	tests := []struct {
		x        string
		y        string
		expected bool
	}{
		{"0.0.0", "0.0.0", true},
		{"0.0.1", "0.0.0", true},
		{"3.0.1", "2.0", true},
		{"3.0.0", "3.0", true},
		{"2.0.0", "2.0", true},
		{"2.0.1", "2.0", true},
		{"2.0.0", "2.5", false},
		{"1.0", "1.0.0", false},
		{"0.0", "0.0.0", false},
		{"", "", false},
		{"0", "", false},
		{"0.0", "", false},
		{"", "0", false},
		{"", "0.0", false},
		{"hello", "hello", false},
		{"0", "hello", false},
		{"0.0", "hello", false},
		{"hello", "0", false},
		{"hello", "0.0", false},
		{"3.1.0", "4.0.0", false},
		{"3.1.0", "3.2.0", false},
		{"3.2.0", "3.1.0", true},
		{"4.0.0", "3.1.0", true},
	}

	for i, test := range tests {
		actual := VersionGTE(test.x, test.y)
		if actual != test.expected {
			t.Errorf("test: %d, expected: %v, when %s >= %s, got: %v",
				i, test.expected, test.x, test.y, actual)
		}
	}
}

func TestNewUUID(t *testing.T) {
	u0 := NewUUID()
	u1 := NewUUID()
	if u0 == "" || u1 == "" || u0 == u1 {
		t.Errorf("NewUUID() failed, %s, %s", u0, u1)
	}
}

func TestExponentialBackoffLoop(t *testing.T) {
	called := 0
	ExponentialBackoffLoop("test", func() int {
		called += 1
		return -1
	}, 0, 0, 0)
	if called != 1 {
		t.Errorf("expected 1 call")
	}

	called = 0
	ExponentialBackoffLoop("test", func() int {
		called += 1
		if called <= 1 {
			return 1
		}
		return -1
	}, 0, 0, 0)
	if called != 2 {
		t.Errorf("expected 2 calls")
	}

	called = 0
	ExponentialBackoffLoop("test", func() int {
		called += 1
		if called == 1 {
			return 1
		}
		if called == 2 {
			return 0
		}
		return -1
	}, 0, 0, 0)
	if called != 3 {
		t.Errorf("expected 2 calls")
	}

	called = 0
	ExponentialBackoffLoop("test", func() int {
		called += 1
		if called == 1 {
			return 1
		}
		if called == 2 {
			return 0
		}
		return -1
	}, 1, 100000.0, 1)
	if called != 3 {
		t.Errorf("expected 2 calls")
	}
}

func TestTimeoutCancelChan(t *testing.T) {
	c := TimeoutCancelChan(0)
	if c != nil {
		t.Errorf("expected nil")
	}
	c = TimeoutCancelChan(-1)
	if c != nil {
		t.Errorf("expected nil")
	}
	c = TimeoutCancelChan(1)
	if c == nil {
		t.Errorf("expected non-nil")
	}
	msg, ok := <-c
	if ok {
		t.Errorf("expected closed")
	}
	if msg != false {
		t.Errorf("expected false msg on closed")
	}
}

func TestTime(t *testing.T) {
	count := uint64(10)
	duration := uint64(100)
	maxDuration := uint64(50)
	Time(func() error {
		time.Sleep(123 * time.Millisecond)
		return nil
	}, &duration, &count, &maxDuration)
	if count <= 10 {
		t.Errorf("expected count to be > 10")
	}
	if duration <= 100 {
		t.Errorf("expected duration to be > 100")
	}
	if maxDuration <= 50 {
		t.Errorf("expected maxDuration to be > 50")
	}
}

type TestACM struct {
	TotError     uint64
	TotRollback  uint64
	TimeRollback uint64
}

func TestAtomicCopyMetrics(t *testing.T) {
	src := &TestACM{
		TotError:     1,
		TotRollback:  2,
		TimeRollback: 3,
	}
	dst := TestACM{}
	AtomicCopyMetrics(src, &dst, nil)
	if !reflect.DeepEqual(src, &dst) {
		t.Errorf("expected src == dst")
	}
	if dst.TotError != 1 ||
		dst.TotRollback != 2 ||
		dst.TimeRollback != 3 {
		t.Errorf("expected src == dst")
	}
}

func TestErrorToString(t *testing.T) {
	if ErrorToString(fmt.Errorf("hi")) != "hi" {
		t.Errorf("expected hi")
	}
	if ErrorToString(nil) != "" {
		t.Errorf("expected empty string")
	}
}

func TestIndentJSON(t *testing.T) {
	s := IndentJSON(TestIndentJSON, "prefix", "indent")
	if strings.Index(s, "err") < 0 {
		t.Errorf("expected err on bad non-json'able IndentJSON()")
	}
}

func TestStructChanges(t *testing.T) {
	if StructChanges(nil, nil) != nil {
		t.Errorf("expected nil")
	}

	structs := []struct {
		s string
		a int
		b int
	}{
		{"0", 100, 200},
		{"1", 101, 201},
		{"2", 201, 101},
	}

	tests := []struct {
		x   int
		y   int
		exp []string
	}{
		{0, 0, nil},
		{1, 1, nil},
		{2, 2, nil},
		{0, 1, []string{"a: 100 -> 101", "b: 200 -> 201"}},
		{1, 0, []string{"a: 101 -> 100", "b: 201 -> 200"}},
		{1, 2, []string{"a: 101 -> 201", "b: 201 -> 101"}},
	}

	for testi, test := range tests {
		x := structs[test.x]
		y := structs[test.y]
		c := StructChanges(x, y)
		if len(c) != len(test.exp) {
			t.Errorf("testi: %d, test: %#v, got: %#v",
				testi, test, c)
		}

		for entryi, entry := range c {
			if entry != test.exp[entryi] {
				t.Errorf("testi: %d, mismatch entryi: %d,"+
					" test: %#v, got: %#v",
					entryi, testi, test, c)
			}
		}
	}
}

func TestIsNanOrInf(t *testing.T) {
	zval := 0.0
	tests := []struct {
		in  float64
		out bool
	}{
		{
			in:  1,
			out: false,
		},
		{
			in:  0.0 / zval,
			out: true,
		},
		{
			in:  1.0 / zval,
			out: true,
		},
		{
			in:  -1.0 / zval,
			out: true,
		},
	}
	for i, test := range tests {
		actual := isNanOrInf(test.in)
		if actual != test.out {
			t.Errorf("testi: %d, expected %t got %t", i, test.out, actual)
		}
	}
}

func TestFPrintFloatMap(t *testing.T) {
	zval := 0.0
	tests := []struct {
		name            string
		values          map[string]float64
		jsonParsedValue map[string]interface{}
	}{
		// 1 value
		{
			name: "n",
			values: map[string]float64{
				"v1": 3.14,
			},
			jsonParsedValue: map[string]interface{}{
				"n": map[string]interface{}{
					"v1": 3.14,
				},
			},
		},
		// 2 values
		{
			name: "n",
			values: map[string]float64{
				"v1": 3.14,
				"v2": 1.2,
			},
			jsonParsedValue: map[string]interface{}{
				"n": map[string]interface{}{
					"v1": 3.14,
					"v2": 1.2,
				},
			},
		},
		// 3 values, one is +Inf
		{
			name: "n",
			values: map[string]float64{
				"v1":  3.14,
				"v2":  1.2,
				"inf": 1.0 / zval,
			},
			jsonParsedValue: map[string]interface{}{
				"n": map[string]interface{}{
					"v1": 3.14,
					"v2": 1.2,
				},
			},
		},
		// all values invalid
		{
			name: "n",
			values: map[string]float64{
				"inf": 1.0 / zval,
				"nan": 0.0 / zval,
			},
			jsonParsedValue: map[string]interface{}{
				"n": map[string]interface{}{},
			},
		},
	}

	// we can't just compare the generated strings because map iteration order
	// is not stable, instead we parse the string back, and compare the result
	for i, test := range tests {
		var buf bytes.Buffer
		fPrintFloatMap(&buf, test.name, test.values)
		jsonString := buf.String()
		// wrap it in surrounding structure
		jsonString = "{" + jsonString + "}"
		var parsed map[string]interface{}
		err := json.Unmarshal([]byte(jsonString), &parsed)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(parsed, test.jsonParsedValue) {
			t.Errorf("testi: %d, expected %v got %v", i, test.jsonParsedValue, parsed)
		}
	}
}

func TestGetMovingPartitionsCountUtil(t *testing.T) {
	// scaleOut case: 1 => 3 nodes
	numKeepNodes := 3
	numRemoveNodes := 0
	numExistingNodes := 1
	numNewNodes := 2
	numPartitions := 18 // eg: 6 * 3 indexes

	movingPartitionCount := CalcMovingPartitionsCount(numKeepNodes,
		numRemoveNodes, numNewNodes, numExistingNodes, numPartitions)

	if movingPartitionCount != 12 {
		t.Errorf(" moving partitions count should be 12")
	}

	// scaleIn case: 3 => 2 nodes
	numKeepNodes = 2
	numRemoveNodes = 1
	numExistingNodes = 3
	numNewNodes = 0
	numPartitions = 18

	movingPartitionCount = CalcMovingPartitionsCount(numKeepNodes,
		numRemoveNodes, numNewNodes, numExistingNodes, numPartitions)

	if movingPartitionCount != 6 {
		t.Errorf(" moving partitions count should be 6")
	}

	// constant node count case: 2 => 2 nodes
	numKeepNodes = 2
	numRemoveNodes = 1
	numExistingNodes = 2
	numNewNodes = 1
	numPartitions = 18

	movingPartitionCount = CalcMovingPartitionsCount(numKeepNodes,
		numRemoveNodes, numNewNodes, numExistingNodes, numPartitions)

	if movingPartitionCount != 9 {
		t.Errorf(" moving partitions count should be 9")
	}

	// few random cases
	numKeepNodes = 2
	numRemoveNodes = 1
	numExistingNodes = 2
	numNewNodes = 1
	numPartitions = 0

	movingPartitionCount = CalcMovingPartitionsCount(numKeepNodes,
		numRemoveNodes, numNewNodes, numExistingNodes, numPartitions)

	if movingPartitionCount != 0 {
		t.Errorf(" moving partitions count should be 0")
	}

	numKeepNodes = 0
	numRemoveNodes = 1
	numExistingNodes = 2
	numNewNodes = 1
	numPartitions = 18

	movingPartitionCount = CalcMovingPartitionsCount(numKeepNodes,
		numRemoveNodes, numNewNodes, numExistingNodes, numPartitions)

	if movingPartitionCount != 0 {
		t.Errorf(" moving partitions count should be 0")
	}

	numKeepNodes = 3
	numRemoveNodes = 0
	numExistingNodes = 3
	numNewNodes = 0
	numPartitions = 18

	movingPartitionCount = CalcMovingPartitionsCount(numKeepNodes,
		numRemoveNodes, numNewNodes, numExistingNodes, numPartitions)

	if movingPartitionCount != 0 {
		t.Errorf(" moving partitions count should be 0")
	}
}
