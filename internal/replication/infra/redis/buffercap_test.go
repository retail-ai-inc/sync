package redis

import (
	"testing"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
)

// How much of the disk one shard's buffer may take. The figure used to be a
// constant per shard, so four shards on a ten gigabyte volume were permitted
// thirty-two gigabytes between them and the disk would have filled first.

func TestWhatTheTaskSaysWins(t *testing.T) {
	if got := bufferCapacity(t.TempDir(), 4, 1<<30, nil, metrics.Labels{}); got != 1<<30 {
		t.Errorf("capacity = %d, want the task's own %d", got, 1<<30)
	}
}

func TestTheDeploymentsSettingComesNext(t *testing.T) {
	previous := storedBufferBytes
	t.Cleanup(func() { storedBufferBytes = previous })
	storedBufferBytes = func() int64 { return 512 << 20 }

	if got := bufferCapacity(t.TempDir(), 4, 0, nil, metrics.Labels{}); got != 512<<20 {
		t.Errorf("capacity = %d, want the stored %d", got, 512<<20)
	}
}

// Failing both, the volume decides: its size, the share of it the buffers may
// take, divided by the shards sharing it.
func TestTheVolumeIsDividedBetweenTheShards(t *testing.T) {
	dir := t.TempDir()
	volume, err := volumeBytes(dir)
	if err != nil || volume <= 0 {
		t.Skipf("the volume holding %s cannot be measured here", dir)
	}

	one := bufferCapacity(dir, 1, 0, nil, metrics.Labels{})
	four := bufferCapacity(dir, 4, 0, nil, metrics.Labels{})
	if one < four {
		t.Errorf("one shard may hold %d and four may hold %d each; the share should "+
			"shrink as the shards sharing the disk grow", one, four)
	}
	if four > defaultMaxBytes || one > defaultMaxBytes {
		t.Errorf("capacity %d/%d is above the built-in limit of %d", one, four, defaultMaxBytes)
	}
	if four < minBufferBytes {
		t.Errorf("capacity %d is below the least worth keeping, %d", four, minBufferBytes)
	}
}

// A volume that cannot be measured leaves the buffer's own default in place,
// which is what this did before there was a rule at all.
func TestAVolumeThatCannotBeMeasuredKeepsTheBuiltInLimit(t *testing.T) {
	if got := bufferCapacity("/nowhere/at/all", 2, 0, nil, metrics.Labels{}); got != 0 {
		t.Errorf("capacity = %d, want 0 so the buffer keeps its own default", got)
	}
}
