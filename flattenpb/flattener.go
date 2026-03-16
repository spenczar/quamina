// Package flattenpb implements a quamina.Flattener for binary-encoded Protocol Buffer messages.
// It uses google.golang.org/protobuf/encoding/protowire for efficient wire-format parsing without
// reflection overhead on the hot path.
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

// Flattener implements quamina.Flattener for binary-encoded protobuf messages.
// Construct one with New; it is not safe for concurrent use across goroutines
// without calling Copy first.
type Flattener struct {
	desc  protoreflect.MessageDescriptor
	byNum map[protowire.Number]protoreflect.FieldDescriptor // top-level, built once at New

	// reused across Flatten calls to reduce allocations
	fields    []quamina.Field
	nextArray int32
}

// New creates a Flattener for the given MessageDescriptor.
func New(desc protoreflect.MessageDescriptor) *Flattener {
	f := &Flattener{
		desc:   desc,
		byNum:  buildByNum(desc),
		fields: make([]quamina.Field, 0, 32),
	}
	return f
}

// buildByNum builds a field-number → descriptor map for the given message descriptor.
func buildByNum(desc protoreflect.MessageDescriptor) map[protowire.Number]protoreflect.FieldDescriptor {
	fds := desc.Fields()
	m := make(map[protowire.Number]protoreflect.FieldDescriptor, fds.Len())
	for i := 0; i < fds.Len(); i++ {
		fd := fds.Get(i)
		m[protowire.Number(fd.Number())] = fd
	}
	return m
}

// Copy implements quamina.Flattener.
func (f *Flattener) Copy() quamina.Flattener {
	return New(f.desc)
}

// Flatten implements quamina.Flattener.
func (f *Flattener) Flatten(event []byte, tracker quamina.SegmentsTreeTracker) ([]quamina.Field, error) {
	f.fields = f.fields[:0]
	f.nextArray = 0
	err := f.flattenMsg(event, f.byNum, tracker, nil)
	return f.fields, err
}

// flattenMsg recursively parses a protobuf-encoded message, emitting quamina Fields for
// every leaf that the tracker considers used.
//
// arrayIDs and arrayPos are lazily allocated; they track per-field-number array identity
// and current position within each repeated field at this message level.
func (f *Flattener) flattenMsg(
	data []byte,
	byNum map[protowire.Number]protoreflect.FieldDescriptor,
	tracker quamina.SegmentsTreeTracker,
	arrayTrail []quamina.ArrayPos,
) error {
	// Per-message-level array tracking (allocated lazily to avoid cost when no repeated fields).
	var arrayIDs map[protowire.Number]int32
	var arrayPos map[protowire.Number]int32

	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		data = data[n:]

		fd, ok := byNum[num]
		if !ok {
			// Unknown field — consume and skip.
			n = protowire.ConsumeFieldValue(num, typ, data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			data = data[n:]
			continue
		}

		name := []byte(string(fd.Name()))
		if !tracker.IsSegmentUsed(name) {
			n = protowire.ConsumeFieldValue(num, typ, data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			data = data[n:]
			continue
		}

		// Detect packed repeated scalars before computing the array trail so we
		// can handle position-counting per element rather than per blob.
		isPackedRepeated := fd.IsList() && typ == protowire.BytesType && isScalarKind(fd.Kind())

		// Compute the array trail for this field occurrence.
		var fieldTrail []quamina.ArrayPos
		if fd.IsList() && !isPackedRepeated {
			if arrayIDs == nil {
				arrayIDs = make(map[protowire.Number]int32)
				arrayPos = make(map[protowire.Number]int32)
			}
			aid, exists := arrayIDs[num]
			if !exists {
				f.nextArray++
				aid = f.nextArray
				arrayIDs[num] = aid
			}
			pos := arrayPos[num]
			arrayPos[num] = pos + 1
			fieldTrail = appendArrayPos(arrayTrail, quamina.ArrayPos{Array: aid, Pos: pos})
		} else {
			fieldTrail = arrayTrail
		}

		switch typ {
		case protowire.VarintType:
			v, n := protowire.ConsumeVarint(data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			data = data[n:]
			if path := tracker.PathForSegment(name); path != nil {
				val, isNum := encodeVarint(fd, v)
				f.fields = append(f.fields, quamina.Field{
					Path: path, Val: val, ArrayTrail: fieldTrail, IsNumber: isNum,
				})
			}

		case protowire.Fixed32Type:
			v, n := protowire.ConsumeFixed32(data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			data = data[n:]
			if path := tracker.PathForSegment(name); path != nil {
				val, isNum := encodeFixed32(fd, v)
				f.fields = append(f.fields, quamina.Field{
					Path: path, Val: val, ArrayTrail: fieldTrail, IsNumber: isNum,
				})
			}

		case protowire.Fixed64Type:
			v, n := protowire.ConsumeFixed64(data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			data = data[n:]
			if path := tracker.PathForSegment(name); path != nil {
				val, isNum := encodeFixed64(fd, v)
				f.fields = append(f.fields, quamina.Field{
					Path: path, Val: val, ArrayTrail: fieldTrail, IsNumber: isNum,
				})
			}

		case protowire.BytesType:
			b, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			data = data[n:]

			switch fd.Kind() {
			case protoreflect.MessageKind, protoreflect.GroupKind:
				if fd.IsMap() {
					if err := f.flattenMapEntry(b, fd, tracker, name, fieldTrail); err != nil {
						return err
					}
				} else {
					if child, ok := tracker.Get(name); ok {
						childByNum := buildByNum(fd.Message())
						if err := f.flattenMsg(b, childByNum, child, fieldTrail); err != nil {
							return err
						}
					}
				}

			case protoreflect.StringKind:
				if path := tracker.PathForSegment(name); path != nil {
					val := make([]byte, len(b)+2)
					val[0] = '"'
					copy(val[1:], b)
					val[len(b)+1] = '"'
					f.fields = append(f.fields, quamina.Field{
						Path: path, Val: val, ArrayTrail: fieldTrail, IsNumber: false,
					})
				}

			case protoreflect.BytesKind:
				if path := tracker.PathForSegment(name); path != nil {
					encoded := base64.StdEncoding.EncodeToString(b)
					f.fields = append(f.fields, quamina.Field{
						Path: path, Val: []byte(encoded), ArrayTrail: fieldTrail, IsNumber: false,
					})
				}

			default:
				// Packed repeated scalar — each element in b is a repeated occurrence.
				if isPackedRepeated {
					if arrayIDs == nil {
						arrayIDs = make(map[protowire.Number]int32)
						arrayPos = make(map[protowire.Number]int32)
					}
					if _, exists := arrayIDs[num]; !exists {
						f.nextArray++
						arrayIDs[num] = f.nextArray
					}
					if path := tracker.PathForSegment(name); path != nil {
						if err := f.decodePacked(b, fd, num, path, arrayIDs, arrayPos, arrayTrail); err != nil {
							return err
						}
					}
				}
			}

		default:
			n = protowire.ConsumeFieldValue(num, typ, data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			data = data[n:]
		}
	}
	return nil
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
			val, isNum := encodeVarint(valFd, v)
			f.fields = append(f.fields, quamina.Field{
				Path: path, Val: val, ArrayTrail: arrayTrail, IsNumber: isNum,
			})
		}
	case protowire.Fixed32Type:
		v, n := protowire.ConsumeFixed32(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		_ = n
		if path := mapTracker.PathForSegment(keyBytes); path != nil {
			val, isNum := encodeFixed32(valFd, v)
			f.fields = append(f.fields, quamina.Field{
				Path: path, Val: val, ArrayTrail: arrayTrail, IsNumber: isNum,
			})
		}
	case protowire.Fixed64Type:
		v, n := protowire.ConsumeFixed64(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		_ = n
		if path := mapTracker.PathForSegment(keyBytes); path != nil {
			val, isNum := encodeFixed64(valFd, v)
			f.fields = append(f.fields, quamina.Field{
				Path: path, Val: val, ArrayTrail: arrayTrail, IsNumber: isNum,
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
				childByNum := buildByNum(valFd.Message())
				return f.flattenMsg(b, childByNum, child, arrayTrail)
			}
		case protoreflect.StringKind:
			if path := mapTracker.PathForSegment(keyBytes); path != nil {
				val := make([]byte, len(b)+2)
				val[0] = '"'
				copy(val[1:], b)
				val[len(b)+1] = '"'
				f.fields = append(f.fields, quamina.Field{
					Path: path, Val: val, ArrayTrail: arrayTrail, IsNumber: false,
				})
			}
		case protoreflect.BytesKind:
			if path := mapTracker.PathForSegment(keyBytes); path != nil {
				encoded := base64.StdEncoding.EncodeToString(b)
				f.fields = append(f.fields, quamina.Field{
					Path: path, Val: []byte(encoded), ArrayTrail: arrayTrail, IsNumber: false,
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
	arrayIDs map[protowire.Number]int32,
	arrayPos map[protowire.Number]int32,
	arrayTrail []quamina.ArrayPos,
) error {
	aid := arrayIDs[num]
	for len(packed) > 0 {
		var val []byte
		var isNum bool

		switch fd.Kind() {
		case protoreflect.FloatKind, protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind:
			v, n := protowire.ConsumeFixed32(packed)
			if n < 0 {
				return protowire.ParseError(n)
			}
			packed = packed[n:]
			val, isNum = encodeFixed32(fd, v)

		case protoreflect.DoubleKind, protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind:
			v, n := protowire.ConsumeFixed64(packed)
			if n < 0 {
				return protowire.ParseError(n)
			}
			packed = packed[n:]
			val, isNum = encodeFixed64(fd, v)

		default:
			v, n := protowire.ConsumeVarint(packed)
			if n < 0 {
				return protowire.ParseError(n)
			}
			packed = packed[n:]
			val, isNum = encodeVarint(fd, v)
		}

		pos := arrayPos[num]
		arrayPos[num] = pos + 1
		trail := appendArrayPos(arrayTrail, quamina.ArrayPos{Array: aid, Pos: pos})
		f.fields = append(f.fields, quamina.Field{
			Path: path, Val: val, ArrayTrail: trail, IsNumber: isNum,
		})
	}
	return nil
}

// appendArrayPos returns a new slice with ap appended to trail.
func appendArrayPos(trail []quamina.ArrayPos, ap quamina.ArrayPos) []quamina.ArrayPos {
	result := make([]quamina.ArrayPos, len(trail)+1)
	copy(result, trail)
	result[len(trail)] = ap
	return result
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

// encodeVarint converts a raw varint wire value to its quamina Val representation.
func encodeVarint(fd protoreflect.FieldDescriptor, v uint64) ([]byte, bool) {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		if v != 0 {
			return []byte("true"), false
		}
		return []byte("false"), false

	case protoreflect.EnumKind:
		ev := fd.Enum().Values().ByNumber(protoreflect.EnumNumber(v))
		if ev == nil {
			return []byte(strconv.FormatInt(int64(v), 10)), true
		}
		name := string(ev.Name())
		out := make([]byte, len(name)+2)
		out[0] = '"'
		copy(out[1:], name)
		out[len(name)+1] = '"'
		return out, false

	case protoreflect.Sint32Kind:
		decoded := protowire.DecodeZigZag(v & 0xFFFFFFFF)
		return []byte(strconv.FormatInt(int64(int32(decoded)), 10)), true

	case protoreflect.Sint64Kind:
		decoded := protowire.DecodeZigZag(v)
		return []byte(strconv.FormatInt(decoded, 10)), true

	case protoreflect.Int32Kind:
		return []byte(strconv.FormatInt(int64(int32(v)), 10)), true

	case protoreflect.Int64Kind:
		return []byte(strconv.FormatInt(int64(v), 10)), true

	default:
		// uint32, uint64, and anything else that uses varint
		return []byte(strconv.FormatUint(v, 10)), true
	}
}

// encodeFixed32 converts a raw fixed32 wire value to its quamina Val representation.
func encodeFixed32(fd protoreflect.FieldDescriptor, v uint32) ([]byte, bool) {
	switch fd.Kind() {
	case protoreflect.FloatKind:
		f32 := math.Float32frombits(v)
		return []byte(strconv.FormatFloat(float64(f32), 'g', -1, 32)), true
	case protoreflect.Sfixed32Kind:
		return []byte(strconv.FormatInt(int64(int32(v)), 10)), true
	default:
		// fixed32
		return []byte(strconv.FormatUint(uint64(v), 10)), true
	}
}

// encodeFixed64 converts a raw fixed64 wire value to its quamina Val representation.
func encodeFixed64(fd protoreflect.FieldDescriptor, v uint64) ([]byte, bool) {
	switch fd.Kind() {
	case protoreflect.DoubleKind:
		f64 := math.Float64frombits(v)
		return []byte(strconv.FormatFloat(f64, 'g', -1, 64)), true
	case protoreflect.Sfixed64Kind:
		return []byte(strconv.FormatInt(int64(v), 10)), true
	default:
		// fixed64
		return []byte(strconv.FormatUint(v, 10)), true
	}
}
