package flattenpb_test

import (
	"testing"

	"quamina.net/go/quamina/v2/flattenpb"
	"quamina.net/go/quamina/v2/internal/fakematcher"
)

// benchEvent is a wire-encoded TestMsg with a mix of scalar, nested, and
// repeated fields — representative of a moderately complex real message.
var benchEvent []byte

func init() {
	var b []byte
	b = appendInt32Field(b, 1, 12345)
	b = appendStringField(b, 2, "benchmark-user")
	b = appendBoolField(b, 3, true)
	b = appendFloat32Field(b, 4, 9.81)
	b = appendFloat64Field(b, 5, 3.141592653589793)
	b = appendBytesField(b, 6, []byte("binarydata"))
	b = appendEnumField(b, 7, 2) // ACTIVE
	b = appendMessageField(b, 8, nestedMsg("child-value", 7))
	b = appendPackedInt32s(b, 9, 1, 2, 3, 4, 5)
	b = appendMessageField(b, 10, nestedMsg("item-a", 10))
	b = appendMessageField(b, 10, nestedMsg("item-b", 20))
	b = appendMessageField(b, 11, mapEntry("env", "prod"))
	benchEvent = b
}

// Benchmark_ProtoFlattener_FewFields benchmarks a tracker that only cares
// about one or two leaf paths — the common case in a selective pattern.
func Benchmark_ProtoFlattener_FewFields(b *testing.B) {
	fl := flattenpb.New(testDesc)
	tr := fakematcher.New("id", "nested\nvalue")

	b.ReportAllocs()
	b.ResetTimer()
	var sink any
	for i := 0; i < b.N; i++ {
		fields, err := fl.Flatten(benchEvent, tr)
		if err != nil {
			b.Fatal(err)
		}
		sink = fields
	}
	_ = sink
}

// Benchmark_ProtoFlattener_ManyFields benchmarks a tracker that cares about
// all paths — the worst case for the pruning optimisation.
func Benchmark_ProtoFlattener_ManyFields(b *testing.B) {
	fl := flattenpb.New(testDesc)
	tr := fakematcher.New(
		"id", "name", "flag", "score", "ratio", "data", "status",
		"nested\nvalue", "nested\ncount",
		"tags",
		"items\nvalue", "items\ncount",
		"labels\nenv",
	)

	b.ReportAllocs()
	b.ResetTimer()
	var sink any
	for i := 0; i < b.N; i++ {
		fields, err := fl.Flatten(benchEvent, tr)
		if err != nil {
			b.Fatal(err)
		}
		sink = fields
	}
	_ = sink
}
