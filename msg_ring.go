//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"fmt"
	"io"
	"sync"
)

// MsgRingMaxSmallBufSize is the cutoff point, in bytes, in which a
// msg ring categorizes a buf as small versus large for reuse.
var MsgRingMaxSmallBufSize = 1024

// MsgRingMaxSmallBufSize is the max pool size for reused buf's.
var MsgRingMaxBufPoolSize = 8

// A MsgRing wraps an io.Writer, and remembers a ring of previous
// writes to the io.Writer.  It is concurrent safe and is useful, for
// example, for remembering recent log messages.
type MsgRing struct {
	m     sync.Mutex
	inner io.Writer
	Next  int      `json:"next"`
	Msgs  [][]byte `json:"msgs"`

	SmallBufs [][]byte // Pool of small buffers.
	LargeBufs [][]byte // Pool of large buffers.
}

// NewMsgRing returns a MsgRing of a given ringSize.
func NewMsgRing(inner io.Writer, ringSize int) (*MsgRing, error) {
	if inner == nil {
		return nil, fmt.Errorf("msg_ring: nil inner io.Writer")
	}
	if ringSize <= 0 {
		return nil, fmt.Errorf("msg_ring: non-positive ring size")
	}
	return &MsgRing{
		inner: inner,
		Next:  0,
		Msgs:  make([][]byte, ringSize),
	}, nil
}

// Implements the io.Writer interface.
func (m *MsgRing) Write(p []byte) (n int, err error) {
	m.m.Lock()

	// RECYCLING ISN'T OCCURRING ANYWAY;
	// DISABLE RECYCLING ON ACCOUNT OF MB-67483
	//
	// Recycle the oldMsg into the small-vs-large pools, as long as
	// there's enough pool space.
	// oldMsg := m.Msgs[m.Next]
	// if oldMsg != nil {
	//     if len(oldMsg) <= MsgRingMaxSmallBufSize {
	//         if len(m.SmallBufs) < MsgRingMaxBufPoolSize {
	//             m.SmallBufs = append(m.SmallBufs)
	//         }
	//     } else {
	//         if len(m.LargeBufs) < MsgRingMaxBufPoolSize {
	//             m.LargeBufs = append(m.LargeBufs)
	//         }
	//     }
	// }

	// Allocate a new buf or recycled buf from the pools.
	var buf []byte

	if len(p) <= MsgRingMaxSmallBufSize {
		if len(m.SmallBufs) > 0 {
			buf = m.SmallBufs[len(m.SmallBufs)-1]
			m.SmallBufs = m.SmallBufs[0 : len(m.SmallBufs)-1]
		}
	} else {
		// Although we wastefully throw away any cached large bufs
		// that aren't large enough, this simple approach doesn't
		// "learn" the wrong large buf size.
		for len(m.LargeBufs) > 0 && buf == nil {
			largeBuf := m.LargeBufs[len(m.LargeBufs)-1]
			m.LargeBufs = m.LargeBufs[0 : len(m.LargeBufs)-1]
			if len(p) <= cap(largeBuf) {
				buf = largeBuf
			}
		}
	}

	if buf == nil {
		buf = make([]byte, len(p))
	}

	copy(buf[0:len(p)], p)

	m.Msgs[m.Next] = buf
	m.Next += 1
	if m.Next >= len(m.Msgs) {
		m.Next = 0
	}

	m.m.Unlock()

	return m.inner.Write(p)
}

// Retrieves the recent writes to the MsgRing.
func (m *MsgRing) Messages() [][]byte {
	rv := make([][]byte, 0, len(m.Msgs))

	m.m.Lock()

	// Pre-alloc a buf to hold a copy of all msgs.
	bufSize := 0
	for _, msg := range m.Msgs {
		bufSize += len(msg)
	}

	buf := make([]byte, 0, bufSize)

	n := len(m.Msgs)
	i := 0
	idx := m.Next
	for i < n {
		if msg := m.Msgs[idx]; msg != nil {
			bufLen := len(buf)
			buf = append(buf, msg...)
			rv = append(rv, buf[bufLen:])
		}
		idx += 1
		if idx >= n {
			idx = 0
		}
		i += 1
	}

	m.m.Unlock()

	return rv
}
