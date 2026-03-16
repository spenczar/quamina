// Package flattenpb implements a quamina.Flattener for binary-encoded Protocol Buffer messages.
package flattenpb

import (
	"encoding/base64"
	"fmt"
	"math"
	"strconv"

	quamina "quamina.net/go/quamina/v2"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// msgSchema holds the pre-built lookup tables for a single message type.
type msgSchema struct {
	// byNum maps field number to its descriptor.
	byNum map[protowire.Number]protoreflect.FieldDescriptor
	// nameBytes maps field number to the UTF-8 field name as a []byte, cached to
	// avoid allocating on every Flatten call.
	nameBytes map[protowire.Number][]byte
}

// Flattener implements quamina.Flattener for binary-encoded protobuf messages.
// Construct one with New; it is not safe for concurrent use across goroutines
// without calling Copy first.
type Flattener struct {
	desc       protoreflect.MessageDescriptor
	// allSchemas maps each message type's full name to its pre-built schema,
	// covering the root message and all transitively reachable nested types.
	allSchemas map[protoreflect.FullName]*msgSchema

	// reused across Flatten calls to reduce allocations
	fields      []quamina.Field
	valBuf      []byte           // backing buffer for all Val slices
	arrayPosBuf []quamina.ArrayPos // backing buffer for all ArrayTrail slices
	nextArray   int32
}

// New creates a Flattener for the given MessageDescriptor.
func New(desc protoreflect.MessageDescriptor) *Flattener {
	allSchemas := make(map[protoreflect.FullName]*msgSchema)
	buildAllSchemas(desc, allSchemas)
	return &Flattener{
		desc:       desc,
		allSchemas: allSchemas,
		fields:     make([]quamina.Field, 0, 32),
	}
}

// buildAllSchemas recursively builds msgSchema entries for desc and all transitively
// reachable message types, storing them in out. Cycles are detected via the out map.
func buildAllSchemas(desc protoreflect.MessageDescriptor, out map[protoreflect.FullName]*msgSchema) {
	if _, seen := out[desc.FullName()]; seen {
		return
	}
	fds := desc.Fields()
	schema := &msgSchema{
		byNum:     make(map[protowire.Number]protoreflect.FieldDescriptor, fds.Len()),
		nameBytes: make(map[protowire.Number][]byte, fds.Len()),
	}
	out[desc.FullName()] = schema
	for i := range fds.Len() {
		fd := fds.Get(i)
		num := protowire.Number(fd.Number())
		schema.byNum[num] = fd
		schema.nameBytes[num] = []byte(fd.Name())
		if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
			buildAllSchemas(fd.Message(), out)
		}
	}
}

// Copy implements quamina.Flattener.
func (f *Flattener) Copy() quamina.Flattener {
	return New(f.desc)
}

// Flatten implements quamina.Flattener.
func (f *Flattener) Flatten(event []byte, tracker quamina.SegmentsTreeTracker) ([]quamina.Field, error) {
	f.fields = f.fields[:0]
	f.valBuf = f.valBuf[:0]
	f.arrayPosBuf = f.arrayPosBuf[:0]
	f.nextArray = 0
	err := f.flattenMsg(event, f.desc, tracker, nil)
	return f.fields, err
}

// fieldArrays tracks array IDs and per-field positions for repeated fields within a
// single message. Both maps are keyed by field number: ids maps a field number to its
// unique array ID, pos maps it to the next unused position index within that array.
type fieldArrays struct {
	ids map[protowire.Number]int32
	pos map[protowire.Number]int32
}

// arrayID returns the array ID for field num, assigning one if this is the first occurrence.
func (a *fieldArrays) arrayID(num protowire.Number, nextArray *int32) int32 {
	if a.ids == nil {
		a.ids = make(map[protowire.Number]int32)
	}
	id, exists := a.ids[num]
	if !exists {
		*nextArray++
		id = *nextArray
		a.ids[num] = id
	}
	return id
}

// nextPos returns the current position index for field num and increments it.
func (a *fieldArrays) nextPos(num protowire.Number) int32 {
	if a.pos == nil {
		a.pos = make(map[protowire.Number]int32)
	}
	pos := a.pos[num]
	a.pos[num] = pos + 1
	return pos
}

// trail appends a new ArrayPos for field num to buf, copies parent into buf first,
// and returns the resulting sub-slice. buf is the Flattener's arrayPosBuf.
func (a *fieldArrays) trail(num protowire.Number, parent []quamina.ArrayPos, buf *[]quamina.ArrayPos, nextArray *int32) []quamina.ArrayPos {
	id := a.arrayID(num, nextArray)
	pos := a.nextPos(num)
	start := len(*buf)
	*buf = append(*buf, parent...)
	*buf = append(*buf, quamina.ArrayPos{Array: id, Pos: pos})
	return (*buf)[start:]
}

// flattenMsg recursively parses a protobuf-encoded message, emitting quamina Fields for
// every leaf that the tracker considers used.
func (f *Flattener) flattenMsg(
	data []byte,
	desc protoreflect.MessageDescriptor,
	tracker quamina.SegmentsTreeTracker,
	arrayTrail []quamina.ArrayPos,
) error {
	schema := f.allSchemas[desc.FullName()]
	var arrays fieldArrays

	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		data = data[n:]

		fd, ok := schema.byNum[num]
		if !ok {
			// Unknown field — consume and skip.
			n = protowire.ConsumeFieldValue(num, typ, data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			data = data[n:]
			continue
		}

		name := schema.nameBytes[num]
		if !tracker.IsSegmentUsed(name) {
			n = protowire.ConsumeFieldValue(num, typ, data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			data = data[n:]
			continue
		}

		// Packed repeated scalars are encoded as a single length-delimited blob containing
		// multiple concatenated values. Each value within the blob gets its own ArrayPos,
		// so we defer trail assignment to decodePacked rather than assigning one here.
		isPackedRepeated := fd.IsList() && typ == protowire.BytesType && isScalarKind(fd.Kind())

		var fieldTrail []quamina.ArrayPos
		if fd.IsList() && !isPackedRepeated {
			fieldTrail = arrays.trail(num, arrayTrail, &f.arrayPosBuf, &f.nextArray)
		} else {
			fieldTrail = arrayTrail
		}

		var err error
		data, err = f.dispatchField(data, fd, num, typ, name, fieldTrail, arrayTrail, &arrays, isPackedRepeated, tracker)
		if err != nil {
			return err
		}
	}
	return nil
}

// dispatchField consumes a single field value from data (immediately following the
// already-consumed tag), emits any matching quamina.Fields, and returns the remaining data.
func (f *Flattener) dispatchField(
	data []byte,
	fd protoreflect.FieldDescriptor,
	num protowire.Number,
	typ protowire.Type,
	name []byte,
	fieldTrail, arrayTrail []quamina.ArrayPos,
	arrays *fieldArrays,
	isPackedRepeated bool,
	tracker quamina.SegmentsTreeTracker,
) ([]byte, error) {
	switch typ {
	case protowire.VarintType:
		v, n := protowire.ConsumeVarint(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		if path := tracker.PathForSegment(name); path != nil {
			start := len(f.valBuf)
			var isNum bool
			f.valBuf, isNum = appendVarint(f.valBuf, fd, v)
			f.fields = append(f.fields, quamina.Field{
				Path: path, Val: f.valBuf[start:], ArrayTrail: fieldTrail, IsNumber: isNum,
			})
		}

	case protowire.Fixed32Type:
		v, n := protowire.ConsumeFixed32(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		if path := tracker.PathForSegment(name); path != nil {
			start := len(f.valBuf)
			var isNum bool
			f.valBuf, isNum = appendFixed32(f.valBuf, fd, v)
			f.fields = append(f.fields, quamina.Field{
				Path: path, Val: f.valBuf[start:], ArrayTrail: fieldTrail, IsNumber: isNum,
			})
		}

	case protowire.Fixed64Type:
		v, n := protowire.ConsumeFixed64(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		if path := tracker.PathForSegment(name); path != nil {
			start := len(f.valBuf)
			var isNum bool
			f.valBuf, isNum = appendFixed64(f.valBuf, fd, v)
			f.fields = append(f.fields, quamina.Field{
				Path: path, Val: f.valBuf[start:], ArrayTrail: fieldTrail, IsNumber: isNum,
			})
		}

	case protowire.BytesType:
		b, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]

		switch fd.Kind() {
		case protoreflect.MessageKind, protoreflect.GroupKind:
			if fd.IsMap() {
				if err := f.flattenMapEntry(b, fd, tracker, name, fieldTrail); err != nil {
					return nil, err
				}
			} else {
				if child, ok := tracker.Get(name); ok {
					if err := f.flattenMsg(b, fd.Message(), child, fieldTrail); err != nil {
						return nil, err
					}
				}
			}

		case protoreflect.StringKind:
			if path := tracker.PathForSegment(name); path != nil {
				start := len(f.valBuf)
				f.valBuf = append(f.valBuf, '"')
				f.valBuf = append(f.valBuf, b...)
				f.valBuf = append(f.valBuf, '"')
				f.fields = append(f.fields, quamina.Field{
					Path: path, Val: f.valBuf[start:], ArrayTrail: fieldTrail, IsNumber: false,
				})
			}

		case protoreflect.BytesKind:
			if path := tracker.PathForSegment(name); path != nil {
				start := len(f.valBuf)
				f.valBuf = base64.StdEncoding.AppendEncode(f.valBuf, b)
				f.fields = append(f.fields, quamina.Field{
					Path: path, Val: f.valBuf[start:], ArrayTrail: fieldTrail, IsNumber: false,
				})
			}

		default:
			// Packed repeated scalar — each element within b is a separate occurrence.
			if isPackedRepeated {
				if path := tracker.PathForSegment(name); path != nil {
					if err := f.decodePacked(b, fd, num, path, arrays, arrayTrail); err != nil {
						return nil, err
					}
				}
			}
		}

	default:
		n := protowire.ConsumeFieldValue(num, typ, data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
	}
	return data, nil
}

// flattenMapEntry parses a single map-entry message (key=1, value=2) and emits the value
// with a path extended by the key.
func (f *Flattener) flattenMapEntry(
	data []byte,
	mapFd protoreflect.FieldDescriptor,
	tracker quamina.SegmentsTreeTracker,
	fieldName []byte,
	arrayTrail []quamina.ArrayPos,
) error {
	entryDesc := mapFd.Message()
	keyFd := entryDesc.Fields().ByNumber(1)
	valFd := entryDesc.Fields().ByNumber(2)
	if keyFd == nil || valFd == nil {
		return fmt.Errorf("flattenpb: map entry missing key or value field")
	}

	// First pass: extract the key.
	var keyBytes []byte
	scan := data
	for len(scan) > 0 {
		num, typ, n := protowire.ConsumeTag(scan)
		if n < 0 {
			return protowire.ParseError(n)
		}
		scan = scan[n:]
		if num == 1 {
			kb, err := extractKeyBytes(keyFd, scan, typ)
			if err != nil {
				return err
			}
			keyBytes = kb
		}
		n = protowire.ConsumeFieldValue(num, typ, scan)
		if n < 0 {
			return protowire.ParseError(n)
		}
		scan = scan[n:]
	}
	if keyBytes == nil {
		// Key absent means zero-value key; use empty string for string keys.
		keyBytes = []byte{}
	}

	// Get the child tracker for the map field, then for the specific key.
	mapTracker, ok := tracker.Get(fieldName)
	if !ok {
		return nil
	}
	if !mapTracker.IsSegmentUsed(keyBytes) {
		return nil
	}

	// Second pass: extract and emit the value.
	scan = data
	for len(scan) > 0 {
		num, typ, n := protowire.ConsumeTag(scan)
		if n < 0 {
			return protowire.ParseError(n)
		}
		scan = scan[n:]
		if num == 2 {
			if err := f.emitMapValue(scan, typ, valFd, mapTracker, keyBytes, arrayTrail); err != nil {
				return err
			}
		}
		n = protowire.ConsumeFieldValue(num, typ, scan)
		if n < 0 {
			return protowire.ParseError(n)
		}
		scan = scan[n:]
	}
	return nil
}

// extractKeyBytes encodes a map key field as a []byte suitable for use as a path segment.
func extractKeyBytes(keyFd protoreflect.FieldDescriptor, data []byte, typ protowire.Type) ([]byte, error) {
	switch typ {
	case protowire.VarintType:
		v, n := protowire.ConsumeVarint(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		switch keyFd.Kind() {
		case protoreflect.BoolKind:
			if v != 0 {
				return []byte("true"), nil
			}
			return []byte("false"), nil
		case protoreflect.Sint32Kind:
			return []byte(strconv.FormatInt(int64(int32(protowire.DecodeZigZag(v))), 10)), nil
		case protoreflect.Sint64Kind:
			return []byte(strconv.FormatInt(protowire.DecodeZigZag(v), 10)), nil
		case protoreflect.Int32Kind, protoreflect.Sfixed32Kind:
			return []byte(strconv.FormatInt(int64(int32(v)), 10)), nil
		case protoreflect.Int64Kind, protoreflect.Sfixed64Kind:
			return []byte(strconv.FormatInt(int64(v), 10)), nil
		default:
			return []byte(strconv.FormatUint(v, 10)), nil
		}
	case protowire.BytesType:
		b, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		return b, nil
	default:
		return nil, fmt.Errorf("flattenpb: unexpected wire type %v for map key", typ)
	}
}

// emitMapValue emits the map value field, with the key as the final path segment.
func (f *Flattener) emitMapValue(
	data []byte,
	typ protowire.Type,
	valFd protoreflect.FieldDescriptor,
	mapTracker quamina.SegmentsTreeTracker,
	keyBytes []byte,
	arrayTrail []quamina.ArrayPos,
) error {
	switch typ {
	case protowire.VarintType:
		v, n := protowire.ConsumeVarint(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		_ = n
		if path := mapTracker.PathForSegment(keyBytes); path != nil {
			start := len(f.valBuf)
			var isNum bool
			f.valBuf, isNum = appendVarint(f.valBuf, valFd, v)
			f.fields = append(f.fields, quamina.Field{
				Path: path, Val: f.valBuf[start:], ArrayTrail: arrayTrail, IsNumber: isNum,
			})
		}
	case protowire.Fixed32Type:
		v, n := protowire.ConsumeFixed32(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		_ = n
		if path := mapTracker.PathForSegment(keyBytes); path != nil {
			start := len(f.valBuf)
			var isNum bool
			f.valBuf, isNum = appendFixed32(f.valBuf, valFd, v)
			f.fields = append(f.fields, quamina.Field{
				Path: path, Val: f.valBuf[start:], ArrayTrail: arrayTrail, IsNumber: isNum,
			})
		}
	case protowire.Fixed64Type:
		v, n := protowire.ConsumeFixed64(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		_ = n
		if path := mapTracker.PathForSegment(keyBytes); path != nil {
			start := len(f.valBuf)
			var isNum bool
			f.valBuf, isNum = appendFixed64(f.valBuf, valFd, v)
			f.fields = append(f.fields, quamina.Field{
				Path: path, Val: f.valBuf[start:], ArrayTrail: arrayTrail, IsNumber: isNum,
			})
		}
	case protowire.BytesType:
		b, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		_ = n
		switch valFd.Kind() {
		case protoreflect.MessageKind, protoreflect.GroupKind:
			if child, ok := mapTracker.Get(keyBytes); ok {
				return f.flattenMsg(b, valFd.Message(), child, arrayTrail)
			}
		case protoreflect.StringKind:
			if path := mapTracker.PathForSegment(keyBytes); path != nil {
				start := len(f.valBuf)
				f.valBuf = append(f.valBuf, '"')
				f.valBuf = append(f.valBuf, b...)
				f.valBuf = append(f.valBuf, '"')
				f.fields = append(f.fields, quamina.Field{
					Path: path, Val: f.valBuf[start:], ArrayTrail: arrayTrail, IsNumber: false,
				})
			}
		case protoreflect.BytesKind:
			if path := mapTracker.PathForSegment(keyBytes); path != nil {
				start := len(f.valBuf)
				f.valBuf = base64.StdEncoding.AppendEncode(f.valBuf, b)
				f.fields = append(f.fields, quamina.Field{
					Path: path, Val: f.valBuf[start:], ArrayTrail: arrayTrail, IsNumber: false,
				})
			}
		}
	}
	return nil
}

// decodePacked decodes a packed repeated scalar blob, emitting one Field per element.
func (f *Flattener) decodePacked(
	packed []byte,
	fd protoreflect.FieldDescriptor,
	num protowire.Number,
	path []byte,
	arrays *fieldArrays,
	arrayTrail []quamina.ArrayPos,
) error {
	aid := arrays.arrayID(num, &f.nextArray)
	for len(packed) > 0 {
		valStart := len(f.valBuf)
		var isNum bool

		switch fd.Kind() {
		case protoreflect.FloatKind, protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind:
			v, n := protowire.ConsumeFixed32(packed)
			if n < 0 {
				return protowire.ParseError(n)
			}
			packed = packed[n:]
			f.valBuf, isNum = appendFixed32(f.valBuf, fd, v)

		case protoreflect.DoubleKind, protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind:
			v, n := protowire.ConsumeFixed64(packed)
			if n < 0 {
				return protowire.ParseError(n)
			}
			packed = packed[n:]
			f.valBuf, isNum = appendFixed64(f.valBuf, fd, v)

		default:
			v, n := protowire.ConsumeVarint(packed)
			if n < 0 {
				return protowire.ParseError(n)
			}
			packed = packed[n:]
			f.valBuf, isNum = appendVarint(f.valBuf, fd, v)
		}

		pos := arrays.nextPos(num)
		trailStart := len(f.arrayPosBuf)
		f.arrayPosBuf = append(f.arrayPosBuf, arrayTrail...)
		f.arrayPosBuf = append(f.arrayPosBuf, quamina.ArrayPos{Array: aid, Pos: pos})
		f.fields = append(f.fields, quamina.Field{
			Path:       path,
			Val:        f.valBuf[valStart:],
			ArrayTrail: f.arrayPosBuf[trailStart:],
			IsNumber:   isNum,
		})
	}
	return nil
}

// isScalarKind reports whether a field kind uses a scalar (non-message, non-string, non-bytes) encoding.
func isScalarKind(k protoreflect.Kind) bool {
	switch k {
	case protoreflect.MessageKind, protoreflect.GroupKind,
		protoreflect.StringKind, protoreflect.BytesKind:
		return false
	}
	return true
}

// appendVarint appends the quamina Val representation of a varint field to buf.
func appendVarint(buf []byte, fd protoreflect.FieldDescriptor, v uint64) ([]byte, bool) {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return strconv.AppendBool(buf, v != 0), false

	case protoreflect.EnumKind:
		ev := fd.Enum().Values().ByNumber(protoreflect.EnumNumber(v))
		if ev == nil {
			return strconv.AppendInt(buf, int64(v), 10), true
		}
		buf = append(buf, '"')
		buf = append(buf, ev.Name()...)
		buf = append(buf, '"')
		return buf, false

	case protoreflect.Sint32Kind:
		decoded := protowire.DecodeZigZag(v & 0xFFFFFFFF)
		return strconv.AppendInt(buf, int64(int32(decoded)), 10), true

	case protoreflect.Sint64Kind:
		return strconv.AppendInt(buf, protowire.DecodeZigZag(v), 10), true

	case protoreflect.Int32Kind:
		return strconv.AppendInt(buf, int64(int32(v)), 10), true

	case protoreflect.Int64Kind:
		return strconv.AppendInt(buf, int64(v), 10), true

	default:
		// uint32, uint64, and anything else that uses varint
		return strconv.AppendUint(buf, v, 10), true
	}
}

// appendFixed32 appends the quamina Val representation of a fixed32 field to buf.
func appendFixed32(buf []byte, fd protoreflect.FieldDescriptor, v uint32) ([]byte, bool) {
	switch fd.Kind() {
	case protoreflect.FloatKind:
		return strconv.AppendFloat(buf, float64(math.Float32frombits(v)), 'g', -1, 32), true
	case protoreflect.Sfixed32Kind:
		return strconv.AppendInt(buf, int64(int32(v)), 10), true
	default:
		return strconv.AppendUint(buf, uint64(v), 10), true
	}
}

// appendFixed64 appends the quamina Val representation of a fixed64 field to buf.
func appendFixed64(buf []byte, fd protoreflect.FieldDescriptor, v uint64) ([]byte, bool) {
	switch fd.Kind() {
	case protoreflect.DoubleKind:
		return strconv.AppendFloat(buf, math.Float64frombits(v), 'g', -1, 64), true
	case protoreflect.Sfixed64Kind:
		return strconv.AppendInt(buf, int64(v), 10), true
	default:
		return strconv.AppendUint(buf, v, 10), true
	}
}
