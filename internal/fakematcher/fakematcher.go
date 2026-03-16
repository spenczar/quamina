// Package fakematcher provides a SegmentsTreeTracker implementation for use in tests.
// It is importable by any package within this module but not by external consumers.
//
// The tracker is implemented as a trie keyed by path segments, mirroring the structure
// of the production segmentsTree. PathForSegment and Get are O(1) map lookups with no
// per-call allocations, so benchmarks that use this tracker are not distorted by
// test-helper overhead.
package fakematcher

import (
	"strings"

	quamina "quamina.net/go/quamina/v2"
)

type node struct {
	root     bool
	children map[string]*node  // segment → child node (for non-leaf path components)
	fields   map[string][]byte // segment → pre-built full path []byte (for leaf components)
}

// New returns a SegmentsTreeTracker pre-populated with the given paths
// (using \n as the segment separator, matching quamina's convention).
func New(paths ...string) quamina.SegmentsTreeTracker {
	root := &node{
		root:     true,
		children: make(map[string]*node),
		fields:   make(map[string][]byte),
	}
	for _, p := range paths {
		root.add(p)
	}
	return root
}

func (n *node) add(path string) {
	segs := strings.Split(path, "\n")
	cur := n
	for i, seg := range segs {
		if i == len(segs)-1 {
			cur.fields[seg] = []byte(path)
		} else {
			if cur.children[seg] == nil {
				cur.children[seg] = &node{
					children: make(map[string]*node),
					fields:   make(map[string][]byte),
				}
			}
			cur = cur.children[seg]
		}
	}
}

// Get implements SegmentsTreeTracker.
func (n *node) Get(segment []byte) (quamina.SegmentsTreeTracker, bool) {
	child, ok := n.children[string(segment)]
	return child, ok
}

// IsRoot implements SegmentsTreeTracker.
func (n *node) IsRoot() bool { return n.root }

// IsSegmentUsed implements SegmentsTreeTracker.
func (n *node) IsSegmentUsed(segment []byte) bool {
	s := string(segment)
	if _, ok := n.fields[s]; ok {
		return true
	}
	_, ok := n.children[s]
	return ok
}

// PathForSegment implements SegmentsTreeTracker.
func (n *node) PathForSegment(name []byte) []byte {
	return n.fields[string(name)]
}

// NodesCount implements SegmentsTreeTracker.
func (n *node) NodesCount() int { return len(n.children) }

// FieldsCount implements SegmentsTreeTracker.
func (n *node) FieldsCount() int { return len(n.fields) }

// String implements SegmentsTreeTracker.
func (n *node) String() string { return "fakematcher" }
