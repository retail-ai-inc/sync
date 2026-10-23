package redis

import (
	"strconv"
	"sync"
)

const SlotCount = 16384

// SlotOf reports which hash slot a key belongs to, by the same rule the server
// uses. This has to agree with the server exactly, because it is what decides
// where a slot's position marker is written.
func SlotOf(key []byte) int {
	if tag, ok := hashTag(key); ok {
		key = tag
	}
	return int(crc16(key) & (SlotCount - 1))
}

// hashTag returns the part of a key between the first '{' and the first '}'
// after it, which is what the server hashes when both are present and something
// lies between them.
//
// It is how two keys are deliberately placed in the same slot, and it is what
// lets a position marker be written in the same transaction as the data.
func hashTag(key []byte) ([]byte, bool) {
	open := -1
	for i, b := range key {
		if b == '{' {
			open = i
			break
		}
	}
	if open < 0 {
		return nil, false
	}
	for i := open + 1; i < len(key); i++ {
		if key[i] == '}' {
			if i == open+1 {
				// "{}" is not a tag: the server hashes the whole key.
				return nil, false
			}
			return key[open+1 : i], true
		}
	}
	return nil, false
}

func crc16(data []byte) uint16 {
	var crc uint16
	for _, b := range data {
		crc ^= uint16(b) << 8
		for i := 0; i < 8; i++ {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

// slotTags holds, for every slot, a short string that hashes into it. The
// table is what makes a per-slot position possible: wrapping one of these in
// braces produces a key guaranteed to live in a chosen slot, so the marker for
// slot 1234 can be written in the same MULTI as the data in slot 1234.  **The
// table must never change.** The markers it names hold the only record of how
// far each slot has been applied; generating different tags in a later version
// would abandon every one of them and silently re-apply history.
var slotTags struct {
	once sync.Once
	tags [SlotCount]string
}

func SlotTags() *[SlotCount]string {
	slotTags.once.Do(func() {
		found := 0
		for i := 0; found < SlotCount; i++ {
			tag := strconv.Itoa(i)
			slot := int(crc16([]byte(tag)) & (SlotCount - 1))
			if slotTags.tags[slot] == "" {
				slotTags.tags[slot] = tag
				found++
			}
		}
	})
	return &slotTags.tags
}

// OffsetKey names the key holding how far one slot has been applied.
//
// The task id is part of the name so that two tasks replicating into the same
// cluster cannot overwrite each other's progress. The name is deliberately
// recognisable: a reverse reconcile, which deletes anything the source does not
// have, has to be able to leave these alone.
func OffsetKey(slot, taskID int) string {
	return "{" + SlotTags()[slot] + "}:__off:" + strconv.Itoa(taskID)
}

const offsetKeyPrefix = "}:__off:"

// IsOffsetKey reports whether a key is one of the position markers this package
// writes, rather than replicated data.
func IsOffsetKey(key string) bool {
	for i := 0; i+len(offsetKeyPrefix) <= len(key); i++ {
		if key[i:i+len(offsetKeyPrefix)] == offsetKeyPrefix {
			return len(key) > 0 && key[0] == '{'
		}
	}
	return false
}
