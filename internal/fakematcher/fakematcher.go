// Package fakematcher provides a SegmentsTreeTracker implementation for use in tests.
// It is importable by any package within this module but not by external consumers.
package fakematcher

import (
	"strings"

	quamina "quamina.net/go/quamina/v2"
)

type tracker struct {
	prefix string
	paths  map[string]bool
}

// New returns a SegmentsTreeTracker that recognises the given full paths
// (using \n as the segment separator, matching quamina's convention).
func New(paths ...string) quamina.SegmentsTreeTracker {
	pathSet := make(map[string]bool, len(paths))
	for _, p := range paths {
		pathSet[p] = true
	}
	return &tracker{prefix: "", paths: pathSet}
}

func (t *tracker) childPath(seg string) string {
	if t.prefix == "" {
		return seg
	}
	return t.prefix + "\n" + seg
}

// Get implements SegmentsTreeTracker.
func (t *tracker) Get(segment []byte) (quamina.SegmentsTreeTracker, bool) {
	cp := t.childPath(string(segment))
	for p := range t.paths {
		if p == cp || strings.HasPrefix(p, cp+"\n") {
			return &tracker{prefix: cp, paths: t.paths}, true
		}
	}
	return nil, false
}

// IsRoot implements SegmentsTreeTracker.
func (t *tracker) IsRoot() bool { return t.prefix == "" }

// IsSegmentUsed implements SegmentsTreeTracker.
func (t *tracker) IsSegmentUsed(segment []byte) bool {
	full := t.childPath(string(segment))
	for p := range t.paths {
		if p == full || strings.HasPrefix(p, full+"\n") {
			return true
		}
	}
	return false
}

// PathForSegment implements SegmentsTreeTracker.
func (t *tracker) PathForSegment(name []byte) []byte {
	full := t.childPath(string(name))
	if t.paths[full] {
		return []byte(full)
	}
	return nil
}

// NodesCount implements SegmentsTreeTracker — returns a large number so the
// flattener never short-circuits based on count.
func (t *tracker) NodesCount() int { return 1000 }

// FieldsCount implements SegmentsTreeTracker — same rationale as NodesCount.
func (t *tracker) FieldsCount() int { return 1000 }

// String implements SegmentsTreeTracker.
func (t *tracker) String() string { return "fakematcher(" + t.prefix + ")" }
