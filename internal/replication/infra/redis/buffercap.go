package redis

import (
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/app/pipeline"
)

// How much of the disk one shard's buffer may take.
//
// The buffer holds the replication stream so that a stopped task resumes from
// disk instead of taking a full resync -- the source keeps a megabyte, this
// keeps gigabytes, and that is the whole point of it. The size it is allowed
// to reach was a constant of eight gigabytes per shard, which is a figure with
// no relationship to the disk it is written to: staging runs four shards
// against a ten gigabyte volume, so the constant permitted thirty-two
// gigabytes and the disk would have filled first. A full disk stops every task
// on the process at once, and nothing else reports it coming.

// storedBufferBytes reads what the deployment has set, and is a variable so a
// test can say what that is without a control database.
var storedBufferBytes = pipeline.RedisBufferBytes

const (
	// bufferVolumeShare is how much of the volume the buffers may take between
	// them. The rest is the control database, which grows with the monitoring
	// history, and the room a filesystem needs to not be full.
	bufferVolumeShare = 0.6
	// minBufferBytes is the least worth keeping. Below this the buffer stops
	// being the thing that saves a full resync.
	minBufferBytes = 256 << 20
)

// bufferCapacity reports how many bytes one shard's buffer may hold.
//
// What the task says wins, then what the deployment has set, and failing both
// the volume decides: its size, the share above, divided by the shards sharing
// it. A volume that cannot be measured falls back to the buffer's own default,
// which is what this did before.
func bufferCapacity(dir string, shards int, configured int64,
	log logrus.FieldLogger, labels metrics.Labels) int64 {

	if configured > 0 {
		return configured
	}
	if stored := storedBufferBytes(); stored > 0 {
		return stored
	}
	if shards < 1 {
		shards = 1
	}

	volume, err := volumeBytes(dir)
	if err != nil || volume <= 0 {
		if log != nil && err != nil {
			log.Warnf("[Redis] Could not measure the volume holding %s (%v), so the "+
				"buffer keeps its built-in limit", dir, err)
		}
		return 0
	}
	metrics.SetBufferVolume(labels, volume)

	share := int64(float64(volume) * bufferVolumeShare / float64(shards))
	switch {
	case share < minBufferBytes:
		share = minBufferBytes
	case share > defaultMaxBytes:
		share = defaultMaxBytes
	}
	if log != nil {
		log.Infof("[Redis] The buffer for this shard may hold %d MB: %d MB of volume, "+
			"%.0f%% of it, %d shard(s) sharing it", share>>20, volume>>20,
			bufferVolumeShare*100, shards)
	}
	return share
}
