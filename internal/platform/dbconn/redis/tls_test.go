package redis

import "testing"

// TLSFor exists for the Redis replication link, which speaks PSYNC over its own
// socket and so cannot take the *goredis.Options this package builds. Before
// it, a rediss:// source passed the connection check made here and then could
// not be replicated from at all, because the link dialled plain TCP.

func TestAPlainDSNAsksForNoTLS(t *testing.T) {
	for _, dsn := range []string{
		"redis://10.118.193.3:6379/0",
		"redis://user:pw@10.118.193.3:6379/1",
		"redis://a:6379,b:6379,c:6379/0",
	} {
		config, err := TLSFor(dsn)
		if err != nil {
			t.Fatalf("TLSFor(%q): %v", dsn, err)
		}
		if config != nil {
			t.Errorf("TLSFor(%q) asked for TLS on a plain source", dsn)
		}
	}
}

func TestARedissDSNAsksForTLS(t *testing.T) {
	for name, dsn := range map[string]string{
		"single":           "rediss://10.60.117.91:6379/0",
		"cluster":          "rediss://a:6379,b:6379,c:6379/0",
		"with credentials": "rediss://user:pw@10.60.117.91:6379/0",
	} {
		t.Run(name, func(t *testing.T) {
			config, err := TLSFor(dsn)
			if err != nil {
				t.Fatalf("TLSFor: %v", err)
			}
			if config == nil {
				t.Fatal("a rediss:// source asked for no TLS, so the replication " +
					"link would dial plain TCP and the source could not be read")
			}
			if config.InsecureSkipVerify {
				t.Error("certificate verification was off without being asked for")
			}
		})
	}
}

// TestSkipVerifyIsCarriedThrough: the parameter is this project's own, and
// neither of go-redis's URL parsers knows it, so it has to survive the split
// here as well as in the client builder.
func TestSkipVerifyIsCarriedThrough(t *testing.T) {
	config, err := TLSFor("rediss://10.60.117.91:6379/0?skip_verify=true")
	if err != nil {
		t.Fatalf("TLSFor: %v", err)
	}
	if config == nil {
		t.Fatal("no TLS settings at all")
	}
	if !config.InsecureSkipVerify {
		t.Error("skip_verify was asked for and not carried through, so a source " +
			"with a self-signed certificate cannot be replicated from")
	}
}

func TestADSNThatIsNotOneIsReported(t *testing.T) {
	if _, err := TLSFor("://nonsense"); err == nil {
		t.Error("a DSN that cannot be read was accepted")
	}
}
