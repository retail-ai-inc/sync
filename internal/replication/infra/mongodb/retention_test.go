package mongodb

import (
	"strings"
	"testing"
)

// config.shards spells a shard as "setName/host:port,host:port". The set name
// has to travel as replicaSet: without it the driver treats the hosts as seeds
// of an unnamed set and may read a secondary, whose oplog is not the history
// the change stream is reading.
func TestAShardIsAddressedAsItsReplicaSet(t *testing.T) {
	uri := shardURI(
		"shard-2/a.svc:27017,b.svc:27017,c.svc:27017",
		"mongodb://root:hunter2@mongos.svc:27017/shop")

	for _, want := range []string{
		"a.svc:27017,b.svc:27017,c.svc:27017",
		"replicaSet=shard-2",
		"root:hunter2@",
		"readPreference=primary",
	} {
		if !strings.Contains(uri, want) {
			t.Errorf("uri = %q, want it to carry %q", uri, want)
		}
	}
}

// A shard with no set name is still addressable; a shard with no hosts is not.
func TestAShardWithoutASetNameStillResolves(t *testing.T) {
	uri := shardURI("a.svc:27017", "mongodb://mongos.svc:27017")
	if !strings.Contains(uri, "a.svc:27017") {
		t.Errorf("uri = %q, want the host", uri)
	}
	if strings.Contains(uri, "replicaSet=") {
		t.Errorf("uri = %q, want no replica set where the shard names none", uri)
	}
	if got := shardURI("shard-0/", "mongodb://mongos.svc:27017"); got != "" {
		t.Errorf("a shard with no members produced %q, want nothing to connect to", got)
	}
}

// The source's credentials are what the task already uses; a shard reached
// without them answers "not authorized" and the window goes unpublished.
func TestTheShardBorrowsTheSourcesCredentials(t *testing.T) {
	with := shardURI("s/a:27017", "mongodb://user:pw@mongos:27017/db")
	if !strings.Contains(with, "user:pw@") {
		t.Errorf("uri = %q, want the source's credentials", with)
	}
	without := shardURI("s/a:27017", "mongodb://mongos:27017/db")
	if strings.Contains(without, "@") {
		t.Errorf("uri = %q, want no credentials where the source carries none", without)
	}
}
