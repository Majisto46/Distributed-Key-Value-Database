package sharding

// MigrationItem specifies a single key that must be transferred from source to target.
type MigrationItem struct {
	Key        []byte
	SourceNode string
	TargetNode string
}

// RebalanceReport provides analytical metrics on shard data movement.
type RebalanceReport struct {
	TotalKeys      int
	MovedKeys      int
	PercentMoved   float64
	SourceNodeMove map[string]int // sourceNode -> count of keys sent
	TargetNodeGain map[string]int // targetNode -> count of keys received
	Migrations     []MigrationItem
}

// CalculateRebalance computes the minimal set of keys that must migrate when
// transitioning from oldRing to newRing.
func CalculateRebalance(keys [][]byte, oldRing, newRing *HashRing) (*RebalanceReport, error) {
	report := &RebalanceReport{
		TotalKeys:      len(keys),
		SourceNodeMove: make(map[string]int),
		TargetNodeGain: make(map[string]int),
	}

	for _, k := range keys {
		source, err := oldRing.GetNode(k)
		if err != nil {
			return nil, err
		}

		target, err := newRing.GetNode(k)
		if err != nil {
			return nil, err
		}

		if source != target {
			report.MovedKeys++
			report.SourceNodeMove[source]++
			report.TargetNodeGain[target]++
			report.Migrations = append(report.Migrations, MigrationItem{
				Key:        k,
				SourceNode: source,
				TargetNode: target,
			})
		}
	}

	if report.TotalKeys > 0 {
		report.PercentMoved = (float64(report.MovedKeys) / float64(report.TotalKeys)) * 100.0
	}

	return report, nil
}

