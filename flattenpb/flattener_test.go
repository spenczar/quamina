package flattenpb_test

import (
	"bytes"
	"math"
	"testing"

	quamina "quamina.net/go/quamina/v2"
	"quamina.net/go/quamina/v2/flattenpb"
	"quamina.net/go/quamina/v2/internal/fakematcher"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// ---------------------------------------------------------------------------
// Descriptor construction
// ---------------------------------------------------------------------------

// testDesc is the MessageDescriptor for TestMsg, built programmatically so
// there is no protoc dependency.
var testDesc protoreflect.MessageDescriptor

func init() {
	testDesc = buildTestDescriptor()
}

func buildTestDescriptor() protoreflect.MessageDescriptor {
	// Types in the file:
	//   enum  TestStatus { UNKNOWN=0; PENDING=1; ACTIVE=2 }
	//   message NestedMsg { string value=1; int32 count=2 }
	//   message TestMsg {
	//     int32 id=1; string name=2; bool flag=3; float score=4; double ratio=5;
	//     bytes data=6; TestStatus status=7; NestedMsg nested=8;
	//     repeated int32 tags=9; repeated NestedMsg items=10;
	//     map<string,string> labels=11; sint32 signed_val=12; uint64 big=13;
	//     sfixed32 sf32=14; sfixed64 sf64=15; fixed32 f32=16; fixed64 f64=17;
	//   }

	opt := func(s string) *string { return proto.String(s) }
	i32 := func(v int32) *int32  { return proto.Int32(v) }

	typeOf := func(t descriptorpb.FieldDescriptorProto_Type) *descriptorpb.FieldDescriptorProto_Type {
		return &t
	}
	labelOf := func(l descriptorpb.FieldDescriptorProto_Label) *descriptorpb.FieldDescriptorProto_Label {
		return &l
	}

	optional := labelOf(descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL)
	repeated := labelOf(descriptorpb.FieldDescriptorProto_LABEL_REPEATED)

	field := func(name string, num int32, typ descriptorpb.FieldDescriptorProto_Type, label *descriptorpb.FieldDescriptorProto_Label, typeName string) *descriptorpb.FieldDescriptorProto {
		f := &descriptorpb.FieldDescriptorProto{
			Name:   opt(name),
			Number: i32(num),
			Type:   typeOf(typ),
			Label:  label,
		}
		if typeName != "" {
			f.TypeName = opt(typeName)
		}
		return f
	}

	fdp := &descriptorpb.FileDescriptorProto{
		Name:    opt("test.proto"),
		Syntax:  opt("proto3"),
		Package: opt("test"),

		EnumType: []*descriptorpb.EnumDescriptorProto{
			{
				Name: opt("TestStatus"),
				Value: []*descriptorpb.EnumValueDescriptorProto{
					{Name: opt("UNKNOWN"), Number: i32(0)},
					{Name: opt("PENDING"), Number: i32(1)},
					{Name: opt("ACTIVE"), Number: i32(2)},
				},
			},
		},

		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: opt("NestedMsg"),
				Field: []*descriptorpb.FieldDescriptorProto{
					field("value", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING, optional, ""),
					field("count", 2, descriptorpb.FieldDescriptorProto_TYPE_INT32, optional, ""),
				},
			},
			{
				Name: opt("TestMsg"),
				Field: []*descriptorpb.FieldDescriptorProto{
					field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT32, optional, ""),
					field("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, optional, ""),
					field("flag", 3, descriptorpb.FieldDescriptorProto_TYPE_BOOL, optional, ""),
					field("score", 4, descriptorpb.FieldDescriptorProto_TYPE_FLOAT, optional, ""),
					field("ratio", 5, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, optional, ""),
					field("data", 6, descriptorpb.FieldDescriptorProto_TYPE_BYTES, optional, ""),
					field("status", 7, descriptorpb.FieldDescriptorProto_TYPE_ENUM, optional, ".test.TestStatus"),
					field("nested", 8, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE, optional, ".test.NestedMsg"),
					field("tags", 9, descriptorpb.FieldDescriptorProto_TYPE_INT32, repeated, ""),
					field("items", 10, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE, repeated, ".test.NestedMsg"),
					field("labels", 11, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE, repeated, ".test.TestMsg.LabelsEntry"),
					field("signed_val", 12, descriptorpb.FieldDescriptorProto_TYPE_SINT32, optional, ""),
					field("big", 13, descriptorpb.FieldDescriptorProto_TYPE_UINT64, optional, ""),
					field("sf32", 14, descriptorpb.FieldDescriptorProto_TYPE_SFIXED32, optional, ""),
					field("sf64", 15, descriptorpb.FieldDescriptorProto_TYPE_SFIXED64, optional, ""),
					field("f32", 16, descriptorpb.FieldDescriptorProto_TYPE_FIXED32, optional, ""),
					field("f64", 17, descriptorpb.FieldDescriptorProto_TYPE_FIXED64, optional, ""),
				},
				NestedType: []*descriptorpb.DescriptorProto{
					{
						Name: opt("LabelsEntry"),
						Field: []*descriptorpb.FieldDescriptorProto{
							field("key", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING, optional, ""),
							field("value", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, optional, ""),
						},
						Options: &descriptorpb.MessageOptions{
							MapEntry: proto.Bool(true),
						},
					},
				},
			},
		},
	}

	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		panic("buildTestDescriptor: " + err.Error())
	}
	return fd.Messages().ByName("TestMsg")
}

// ---------------------------------------------------------------------------
// Wire-building helpers
// ---------------------------------------------------------------------------

func appendInt32Field(b []byte, num protowire.Number, v int32) []byte {
	b = protowire.AppendTag(b, num, protowire.VarintType)
	return protowire.AppendVarint(b, uint64(v))
}

func appendUint64Field(b []byte, num protowire.Number, v uint64) []byte {
	b = protowire.AppendTag(b, num, protowire.VarintType)
	return protowire.AppendVarint(b, v)
}

func appendBoolField(b []byte, num protowire.Number, v bool) []byte {
	b = protowire.AppendTag(b, num, protowire.VarintType)
	if v {
		return protowire.AppendVarint(b, 1)
	}
	return protowire.AppendVarint(b, 0)
}

func appendStringField(b []byte, num protowire.Number, s string) []byte {
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendString(b, s)
}

func appendBytesField(b []byte, num protowire.Number, v []byte) []byte {
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendBytes(b, v)
}

func appendFloat32Field(b []byte, num protowire.Number, v float32) []byte {
	b = protowire.AppendTag(b, num, protowire.Fixed32Type)
	return protowire.AppendFixed32(b, math.Float32bits(v))
}

func appendFloat64Field(b []byte, num protowire.Number, v float64) []byte {
	b = protowire.AppendTag(b, num, protowire.Fixed64Type)
	return protowire.AppendFixed64(b, math.Float64bits(v))
}

func appendEnumField(b []byte, num protowire.Number, v int32) []byte {
	return appendInt32Field(b, num, v)
}

func appendSint32Field(b []byte, num protowire.Number, v int32) []byte {
	b = protowire.AppendTag(b, num, protowire.VarintType)
	return protowire.AppendVarint(b, protowire.EncodeZigZag(int64(v)))
}

func appendSfixed32Field(b []byte, num protowire.Number, v int32) []byte {
	b = protowire.AppendTag(b, num, protowire.Fixed32Type)
	return protowire.AppendFixed32(b, uint32(v))
}

func appendSfixed64Field(b []byte, num protowire.Number, v int64) []byte {
	b = protowire.AppendTag(b, num, protowire.Fixed64Type)
	return protowire.AppendFixed64(b, uint64(v))
}

func appendFixed32Field(b []byte, num protowire.Number, v uint32) []byte {
	b = protowire.AppendTag(b, num, protowire.Fixed32Type)
	return protowire.AppendFixed32(b, v)
}

func appendFixed64Field(b []byte, num protowire.Number, v uint64) []byte {
	b = protowire.AppendTag(b, num, protowire.Fixed64Type)
	return protowire.AppendFixed64(b, v)
}

func appendMessageField(b []byte, num protowire.Number, inner []byte) []byte {
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendBytes(b, inner)
}

func appendPackedInt32s(b []byte, num protowire.Number, vs ...int32) []byte {
	var packed []byte
	for _, v := range vs {
		packed = protowire.AppendVarint(packed, uint64(v))
	}
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendBytes(b, packed)
}

// nestedMsg builds a NestedMsg wire encoding (value=field1, count=field2).
func nestedMsg(value string, count int32) []byte {
	var b []byte
	b = appendStringField(b, 1, value)
	b = appendInt32Field(b, 2, count)
	return b
}

// mapEntry builds a LabelsEntry wire encoding.
func mapEntry(key, value string) []byte {
	var b []byte
	b = appendStringField(b, 1, key)
	b = appendStringField(b, 2, value)
	return b
}

// ---------------------------------------------------------------------------
// Test helper
// ---------------------------------------------------------------------------

type tc struct {
	name      string
	data      []byte
	paths     []string // tracker paths
	wantPaths []string
	wantVals  []string
	wantErr   bool
}

func runCases(t *testing.T, cases []tc) {
	t.Helper()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fl := flattenpb.New(testDesc)
			tr := fakematcher.New(c.paths...)
			fields, err := fl.Flatten(c.data, tr)
			if c.wantErr {
				if err == nil {
					t.Fatal("wanted error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			checkFields(t, fields, c.wantPaths, c.wantVals)
		})
	}
}

func checkFields(t *testing.T, got []quamina.Field, wantPaths, wantVals []string) {
	t.Helper()
	if len(got) != len(wantPaths) {
		t.Errorf("got %d fields, want %d", len(got), len(wantPaths))
		for _, f := range got {
			t.Logf("  path=%q val=%q", f.Path, f.Val)
		}
		return
	}
	for i, f := range got {
		if !bytes.Equal(f.Path, []byte(wantPaths[i])) {
			t.Errorf("[%d] path: got %q want %q", i, f.Path, wantPaths[i])
		}
		if !bytes.Equal(f.Val, []byte(wantVals[i])) {
			t.Errorf("[%d] val:  got %q want %q", i, f.Val, wantVals[i])
		}
	}
}

// ---------------------------------------------------------------------------
// Scalar tests
// ---------------------------------------------------------------------------

func TestScalars(t *testing.T) {
	runCases(t, []tc{
		{
			name:      "int32",
			data:      appendInt32Field(nil, 1, 42),
			paths:     []string{"id"},
			wantPaths: []string{"id"},
			wantVals:  []string{"42"},
		},
		{
			name:      "int32 negative",
			data:      appendInt32Field(nil, 1, -7),
			paths:     []string{"id"},
			wantPaths: []string{"id"},
			wantVals:  []string{"-7"},
		},
		{
			name:      "string",
			data:      appendStringField(nil, 2, "hello"),
			paths:     []string{"name"},
			wantPaths: []string{"name"},
			wantVals:  []string{`"hello"`},
		},
		{
			name:      "bool true",
			data:      appendBoolField(nil, 3, true),
			paths:     []string{"flag"},
			wantPaths: []string{"flag"},
			wantVals:  []string{"true"},
		},
		{
			name:      "bool false",
			data:      appendBoolField(nil, 3, false),
			paths:     []string{"flag"},
			wantPaths: []string{"flag"},
			wantVals:  []string{"false"},
		},
		{
			name:      "float32",
			data:      appendFloat32Field(nil, 4, 3.14),
			paths:     []string{"score"},
			wantPaths: []string{"score"},
			wantVals:  []string{"3.14"},
		},
		{
			name:      "float64",
			data:      appendFloat64Field(nil, 5, 2.718281828),
			paths:     []string{"ratio"},
			wantPaths: []string{"ratio"},
			wantVals:  []string{"2.718281828"},
		},
		{
			name:      "bytes",
			data:      appendBytesField(nil, 6, []byte("abc")),
			paths:     []string{"data"},
			wantPaths: []string{"data"},
			wantVals:  []string{"YWJj"}, // base64("abc")
		},
		{
			name:      "enum known",
			data:      appendEnumField(nil, 7, 1),
			paths:     []string{"status"},
			wantPaths: []string{"status"},
			wantVals:  []string{`"PENDING"`},
		},
		{
			name:      "enum unknown value",
			data:      appendEnumField(nil, 7, 99),
			paths:     []string{"status"},
			wantPaths: []string{"status"},
			wantVals:  []string{"99"},
		},
		{
			name:      "sint32 positive",
			data:      appendSint32Field(nil, 12, 5),
			paths:     []string{"signed_val"},
			wantPaths: []string{"signed_val"},
			wantVals:  []string{"5"},
		},
		{
			name:      "sint32 negative",
			data:      appendSint32Field(nil, 12, -3),
			paths:     []string{"signed_val"},
			wantPaths: []string{"signed_val"},
			wantVals:  []string{"-3"},
		},
		{
			name:      "uint64",
			data:      appendUint64Field(nil, 13, 1<<40),
			paths:     []string{"big"},
			wantPaths: []string{"big"},
			wantVals:  []string{"1099511627776"},
		},
		{
			name:      "sfixed32",
			data:      appendSfixed32Field(nil, 14, -100),
			paths:     []string{"sf32"},
			wantPaths: []string{"sf32"},
			wantVals:  []string{"-100"},
		},
		{
			name:      "sfixed64",
			data:      appendSfixed64Field(nil, 15, -999999999999),
			paths:     []string{"sf64"},
			wantPaths: []string{"sf64"},
			wantVals:  []string{"-999999999999"},
		},
		{
			name:      "fixed32",
			data:      appendFixed32Field(nil, 16, 42),
			paths:     []string{"f32"},
			wantPaths: []string{"f32"},
			wantVals:  []string{"42"},
		},
		{
			name:      "fixed64",
			data:      appendFixed64Field(nil, 17, 9999),
			paths:     []string{"f64"},
			wantPaths: []string{"f64"},
			wantVals:  []string{"9999"},
		},
		{
			name:      "multiple scalars",
			data:      func() []byte { b := appendInt32Field(nil, 1, 7); return appendStringField(b, 2, "x") }(),
			paths:     []string{"id", "name"},
			wantPaths: []string{"id", "name"},
			wantVals:  []string{"7", `"x"`},
		},
	})
}

// ---------------------------------------------------------------------------
// Tracker pruning
// ---------------------------------------------------------------------------

func TestTrackerPruning(t *testing.T) {
	runCases(t, []tc{
		{
			name:      "field not in tracker is not emitted",
			data:      func() []byte { b := appendInt32Field(nil, 1, 1); return appendStringField(b, 2, "skip") }(),
			paths:     []string{"id"},     // only "id" registered
			wantPaths: []string{"id"},
			wantVals:  []string{"1"},
		},
		{
			name:      "no fields registered",
			data:      appendInt32Field(nil, 1, 42),
			paths:     []string{"name"}, // unrelated path
			wantPaths: []string{},
			wantVals:  []string{},
		},
	})
}

// ---------------------------------------------------------------------------
// Nested messages
// ---------------------------------------------------------------------------

func TestNestedMessage(t *testing.T) {
	inner := nestedMsg("world", 99)
	var msg []byte
	msg = appendMessageField(msg, 8, inner) // nested = NestedMsg{value:"world", count:99}

	runCases(t, []tc{
		{
			name:      "nested leaf",
			data:      msg,
			paths:     []string{"nested\nvalue"},
			wantPaths: []string{"nested\nvalue"},
			wantVals:  []string{`"world"`},
		},
		{
			name:      "nested both leaves",
			data:      msg,
			paths:     []string{"nested\nvalue", "nested\ncount"},
			wantPaths: []string{"nested\nvalue", "nested\ncount"},
			wantVals:  []string{`"world"`, "99"},
		},
		{
			name:      "nested not registered — skipped",
			data:      msg,
			paths:     []string{"id"},
			wantPaths: []string{},
			wantVals:  []string{},
		},
	})
}

// ---------------------------------------------------------------------------
// Repeated scalar fields
// ---------------------------------------------------------------------------

func TestRepeatedScalar(t *testing.T) {
	// tags: [10, 20, 30] — packed encoding
	packed := appendPackedInt32s(nil, 9, 10, 20, 30)

	runCases(t, []tc{
		{
			name:      "packed repeated int32 emits three fields",
			data:      packed,
			paths:     []string{"tags"},
			wantPaths: []string{"tags", "tags", "tags"},
			wantVals:  []string{"10", "20", "30"},
		},
	})

	// Also verify ArrayTrail is set correctly for packed fields.
	t.Run("packed ArrayTrail", func(t *testing.T) {
		fl := flattenpb.New(testDesc)
		tr := fakematcher.New("tags")
		fields, err := fl.Flatten(packed, tr)
		if err != nil {
			t.Fatal(err)
		}
		if len(fields) != 3 {
			t.Fatalf("want 3 fields, got %d", len(fields))
		}
		aid := fields[0].ArrayTrail[0].Array
		for i, f := range fields {
			if len(f.ArrayTrail) != 1 {
				t.Errorf("[%d] ArrayTrail len %d, want 1", i, len(f.ArrayTrail))
			}
			if f.ArrayTrail[0].Array != aid {
				t.Errorf("[%d] Array ID mismatch", i)
			}
			if f.ArrayTrail[0].Pos != int32(i) {
				t.Errorf("[%d] Pos=%d want %d", i, f.ArrayTrail[0].Pos, i)
			}
		}
	})

	// Unpacked repeated fields (same field number appears multiple times).
	unpacked := func() []byte {
		var b []byte
		b = appendInt32Field(b, 9, 100)
		b = appendInt32Field(b, 9, 200)
		return b
	}()

	runCases(t, []tc{
		{
			name:      "unpacked repeated int32",
			data:      unpacked,
			paths:     []string{"tags"},
			wantPaths: []string{"tags", "tags"},
			wantVals:  []string{"100", "200"},
		},
	})
}

// ---------------------------------------------------------------------------
// Repeated message fields
// ---------------------------------------------------------------------------

func TestRepeatedMessage(t *testing.T) {
	var msg []byte
	msg = appendMessageField(msg, 10, nestedMsg("a", 1)) // items[0]
	msg = appendMessageField(msg, 10, nestedMsg("b", 2)) // items[1]

	runCases(t, []tc{
		{
			name:      "repeated message value field",
			data:      msg,
			paths:     []string{"items\nvalue"},
			wantPaths: []string{"items\nvalue", "items\nvalue"},
			wantVals:  []string{`"a"`, `"b"`},
		},
	})

	// ArrayTrail isolation: items[0].value and items[1].value must be in
	// different array positions so cross-element patterns don't match.
	t.Run("ArrayTrail cross-element isolation", func(t *testing.T) {
		fl := flattenpb.New(testDesc)
		tr := fakematcher.New("items\nvalue", "items\ncount")
		fields, err := fl.Flatten(msg, tr)
		if err != nil {
			t.Fatal(err)
		}
		// Expect 4 fields: value/count for each of the two items.
		if len(fields) != 4 {
			t.Fatalf("want 4 fields, got %d", len(fields))
		}
		// Items from element 0 share Array ID and Pos=0.
		// Items from element 1 share the same Array ID but Pos=1.
		arrID := fields[0].ArrayTrail[0].Array
		for _, f := range fields {
			if f.ArrayTrail[0].Array != arrID {
				t.Errorf("array ID mismatch: %v", f.ArrayTrail)
			}
		}
		// First two fields come from item[0] (pos 0), next two from item[1] (pos 1).
		if fields[0].ArrayTrail[0].Pos != 0 || fields[1].ArrayTrail[0].Pos != 0 {
			t.Errorf("item[0] fields should have Pos=0, got %v %v", fields[0].ArrayTrail, fields[1].ArrayTrail)
		}
		if fields[2].ArrayTrail[0].Pos != 1 || fields[3].ArrayTrail[0].Pos != 1 {
			t.Errorf("item[1] fields should have Pos=1, got %v %v", fields[2].ArrayTrail, fields[3].ArrayTrail)
		}
	})
}

// ---------------------------------------------------------------------------
// Map fields
// ---------------------------------------------------------------------------

func TestMapField(t *testing.T) {
	var msg []byte
	msg = appendMessageField(msg, 11, mapEntry("env", "prod"))
	msg = appendMessageField(msg, 11, mapEntry("region", "us-east"))

	runCases(t, []tc{
		{
			name:      "map entry by key",
			data:      msg,
			paths:     []string{"labels\nenv"},
			wantPaths: []string{"labels\nenv"},
			wantVals:  []string{`"prod"`},
		},
		{
			name:      "two map keys",
			data:      msg,
			paths:     []string{"labels\nenv", "labels\nregion"},
			wantPaths: []string{"labels\nenv", "labels\nregion"},
			wantVals:  []string{`"prod"`, `"us-east"`},
		},
		{
			name:      "unregistered map key not emitted",
			data:      msg,
			paths:     []string{"labels\nother"},
			wantPaths: []string{},
			wantVals:  []string{},
		},
	})
}

// ---------------------------------------------------------------------------
// Error cases
// ---------------------------------------------------------------------------

func TestErrors(t *testing.T) {
	runCases(t, []tc{
		{
			name:    "truncated tag",
			data:    []byte{0xff, 0xff}, // invalid/truncated varint tag
			paths:   []string{"id"},
			wantErr: true,
		},
		{
			name:    "truncated varint value",
			data:    func() []byte { b := protowire.AppendTag(nil, 1, protowire.VarintType); return b }(), // tag with no value
			paths:   []string{"id"},
			wantErr: true,
		},
	})
}

// ---------------------------------------------------------------------------
// Copy
// ---------------------------------------------------------------------------

func TestCopy(t *testing.T) {
	orig := flattenpb.New(testDesc)
	cpy := orig.Copy()

	data := appendInt32Field(nil, 1, 7)
	tr := fakematcher.New("id")

	f1, err := orig.Flatten(data, tr)
	if err != nil {
		t.Fatal(err)
	}
	f2, err := cpy.Flatten(data, tr)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(f1[0].Val, f2[0].Val) {
		t.Errorf("Copy produced different result: %q vs %q", f1[0].Val, f2[0].Val)
	}
}

// ---------------------------------------------------------------------------
// Integration: quamina.WithFlattener
// ---------------------------------------------------------------------------

func TestWithFlattener(t *testing.T) {
	fl := flattenpb.New(testDesc)
	q, err := quamina.New(quamina.WithFlattener(fl))
	if err != nil {
		t.Fatal(err)
	}

	// Add a pattern matching id = 42
	if err := q.AddPattern("hit", `{"id": [42]}`); err != nil {
		t.Fatal(err)
	}
	if err := q.AddPattern("miss", `{"id": [99]}`); err != nil {
		t.Fatal(err)
	}

	event := appendInt32Field(nil, 1, 42)
	matches, err := q.MatchesForEvent(event)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 || matches[0] != quamina.X("hit") {
		t.Errorf("expected [hit], got %v", matches)
	}
}
