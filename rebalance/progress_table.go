//  Copyright 2015-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rebalance

import (
	"bytes"
	"fmt"
)

// ProgressTableString implements the ProgressToString func signature
// by generating a tabular representation of the progress.
func ProgressTableString(maxNodeLen, maxPIndexLen int,
	seenNodes map[string]bool,
	seenNodesSorted []string,
	seenPIndexes map[string]bool,
	seenPIndexesSorted []string,
	progressEntries map[string]map[string]map[string]*ProgressEntry) string {
	var b bytes.Buffer

	WriteProgressTable(&b, maxNodeLen, maxPIndexLen,
		seenNodes,
		seenNodesSorted,
		seenPIndexes,
		seenPIndexesSorted,
		progressEntries)

	return b.String()
}

// WriteProgressTable writes progress entries in tabular format to a
// bytes buffer.
func WriteProgressTable(b *bytes.Buffer,
	maxNodeLen, maxPIndexLen int,
	seenNodes map[string]bool,
	seenNodesSorted []string,
	seenPIndexes map[string]bool,
	seenPIndexesSorted []string,
	progressEntries map[string]map[string]map[string]*ProgressEntry,
) {
	written, _ := b.Write([]byte("%%%"))
	for i := written; i < maxPIndexLen; i++ {
		b.WriteByte(' ')
	}
	b.WriteByte(' ')

	for i, seenNode := range seenNodesSorted {
		if i > 0 {
			b.WriteByte(' ')
		}

		// TODO: Emit node human readable ADDR:PORT.
		b.Write([]byte(seenNode))
	}
	b.WriteByte('\n')

	for _, seenPIndex := range seenPIndexesSorted {
		b.Write([]byte(" %                  "))
		b.Write([]byte(seenPIndex))

		for _, seenNode := range seenNodesSorted {
			b.WriteByte(' ')

			sourcePartitions, exists :=
				progressEntries[seenPIndex]
			if !exists || sourcePartitions == nil {
				WriteProgressCell(b, nil, nil, maxNodeLen)
				continue
			}

			nodes, exists := sourcePartitions[""]
			if !exists || nodes == nil {
				WriteProgressCell(b, nil, nil, maxNodeLen)
				continue
			}

			pe, exists := nodes[seenNode]
			if !exists || pe == nil {
				WriteProgressCell(b, nil, nil, maxNodeLen)
				continue
			}

			WriteProgressCell(b, pe, sourcePartitions, maxNodeLen)
		}

		b.WriteByte('\n')
	}
}

var opMap = map[string]string{
	"":        ".",
	"add":     "+",
	"del":     "-",
	"promote": "P",
	"demote":  "D",
}

// WriteProgressCell writes a cell in a progress table to a buffer.
func WriteProgressCell(b *bytes.Buffer,
	pe *ProgressEntry,
	sourcePartitions map[string]map[string]*ProgressEntry,
	maxNodeLen int) {
	var written int

	totPct := 0.0 // To compute average pct.
	numPct := 0

	if pe != nil {
		written, _ = fmt.Fprintf(b, "%d ", pe.Move)

		if sourcePartitions != nil {
			n, _ := b.Write([]byte(opMap[pe.StateOp.Op]))
			written = written + n

			for sourcePartition, nodes := range sourcePartitions {
				if sourcePartition == "" {
					continue
				}

				pex := nodes[pe.Node]
				if pex == nil || pex.WantUUIDSeq.UUID == "" {
					continue
				}

				if pex.WantUUIDSeq.Seq <= pex.CurrUUIDSeq.Seq {
					totPct = totPct + 1.0
					numPct = numPct + 1
					continue
				}

				n := pex.CurrUUIDSeq.Seq - pex.InitUUIDSeq.Seq
				d := pex.WantUUIDSeq.Seq - pex.InitUUIDSeq.Seq
				if d > 0 {
					pct := float64(n) / float64(d)
					totPct = totPct + pct
					numPct = numPct + 1
				}
			}
		}
	} else {
		b.Write([]byte("  ."))
		written = 3
	}

	if numPct > 0 {
		avgPct := totPct / float64(numPct)

		n, _ := fmt.Fprintf(b, " %.1f%%", avgPct*100.0)
		written = written + n
	}

	for i := written; i < maxNodeLen; i++ {
		b.WriteByte(' ')
	}
}
