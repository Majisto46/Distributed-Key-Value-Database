package mvcc

import (
	"sync/atomic"
	"time"
)

// Oracle generates globally monotonic timestamps for transactions.
// It can use a pure atomic logical counter or a Hybrid Logical Clock (HLC).
type Oracle struct {
	lastTS uint64
}

// NewOracle initializes an Oracle with a starting timestamp.
func NewOracle() *Oracle {
	// Initialize with current Unix nanosecond timestamp to align with physical time
	initial := uint64(time.Now().UnixNano())
	return &Oracle{
		lastTS: initial,
	}
}

// NextTimestamp returns a monotonically increasing uint64 timestamp.
func (o *Oracle) NextTimestamp() uint64 {
	return atomic.AddUint64(&o.lastTS, 1)
}

// CurrentTimestamp returns the latest observed timestamp without incrementing.
func (o *Oracle) CurrentTimestamp() uint64 {
	return atomic.LoadUint64(&o.lastTS)
}

