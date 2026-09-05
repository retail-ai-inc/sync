package redis

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"
)

func TestInfoFieldReadsOneValue(t *testing.T) {
	const info = "# Memory\r\nused_memory:545259520\r\nmaxmemory:1073741824\r\n" +
		"maxmemory_policy:volatile-lru\r\n"

	for field, want := range map[string]string{
		"used_memory":      "545259520",
		"maxmemory":        "1073741824",
		"maxmemory_policy": "volatile-lru",
		"nothing_like_it":  "",
	} {
		if got := infoField(info, field); got != want {
			t.Errorf("infoField(%q) = %q, want %q", field, got, want)
		}
	}
}

// TestInfoFieldDoesNotMatchAPrefix covers used_memory against
// used_memory_peak, which sits next to it in every reply.
func TestInfoFieldDoesNotMatchAPrefix(t *testing.T) {
	const info = "used_memory_peak:999\r\nused_memory:100\r\n"
	if got := infoField(info, "used_memory"); got != "100" {
		t.Errorf("infoField(used_memory) = %q, want 100 and not the peak", got)
	}
}

func TestInfoFieldIgnoresCommentsAndBlanks(t *testing.T) {
	const info = "# Memory\r\n\r\n# still a comment:not a field\r\nmaxmemory:0\r\n"
	if got := infoField(info, "maxmemory"); got != "0" {
		t.Errorf("infoField(maxmemory) = %q, want 0", got)
	}
	if got := infoField(info, "still a comment"); got != "" {
		t.Errorf("a comment line was read as a field: %q", got)
	}
}

// TestModuleNameReadsEveryReplyShape covers RESP2, which answers a flat
// name/value array, and RESP3, which answers a map. Reading only one of them
// would report a source's modules as none and compare nothing.
func TestModuleNameReadsEveryReplyShape(t *testing.T) {
	for name, entry := range map[string]interface{}{
		"RESP2 flat array": []interface{}{"name", "search", "ver", int64(20811)},
		"RESP3 string map": map[string]interface{}{"name": "search", "ver": int64(20811)},
		"RESP3 any map":    map[interface{}]interface{}{"name": "search"},
	} {
		t.Run(name, func(t *testing.T) {
			if got := moduleName(entry); got != "search" {
				t.Errorf("moduleName() = %q, want search", got)
			}
		})
	}
}

func TestModuleNameOfSomethingElse(t *testing.T) {
	for name, entry := range map[string]interface{}{
		"no name field": []interface{}{"ver", int64(1)},
		"odd length":    []interface{}{"name"},
		"not a list":    "search",
		"nil":           nil,
	} {
		t.Run(name, func(t *testing.T) {
			if got := moduleName(entry); got != "" {
				t.Errorf("moduleName() = %q, want nothing", got)
			}
		})
	}
}

// TestTheStreamDialsTLSWhenAsked drives Dial against a real TLS listener,
// because whether the socket is wrapped is a property of the dial and not
// something the options can be inspected for.
//
// Before this, StreamOptions had no TLS field: a rediss:// source was reached
// over TLS by every other client this program opens, the replication link
// dialled plain TCP regardless, and the source passed its connection check and
// then could not be replicated from.
func TestTheStreamDialsTLSWhenAsked(t *testing.T) {
	certificate, pool := selfSignedFor(t, "127.0.0.1")
	listener, err := tls.Listen("tcp", "127.0.0.1:0",
		&tls.Config{Certificates: []tls.Certificate{certificate}})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	handshaken := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			handshaken <- err
			return
		}
		defer conn.Close()
		handshaken <- conn.(*tls.Conn).Handshake()
		// Enough of a reply that authenticate() does not hang the dial.
		_, _ = conn.Write([]byte("+OK\r\n"))
		time.Sleep(50 * time.Millisecond)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// The dial may still fail further in -- this is not a Redis server -- but
	// the handshake is what is being asserted.
	_, _ = Dial(ctx, StreamOptions{
		Addr: listener.Addr().String(),
		TLS:  &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
	})

	select {
	case err := <-handshaken:
		if err != nil {
			t.Errorf("the TLS handshake failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("no connection reached the TLS listener")
	}
}

// TestTheStreamDialsPlainWithoutTLS: an ordinary redis:// source must not have
// a handshake forced on it.
func TestTheStreamDialsPlainWithoutTLS(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	accepted := make(chan struct{}, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		accepted <- struct{}{}
		_, _ = conn.Write([]byte("+OK\r\n"))
		time.Sleep(50 * time.Millisecond)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = Dial(ctx, StreamOptions{Addr: listener.Addr().String()})

	select {
	case <-accepted:
	case <-time.After(5 * time.Second):
		t.Error("a plain dial never reached the listener")
	}
}

// selfSignedFor builds a certificate for one host and a pool that trusts it.
func selfSignedFor(t *testing.T, host string) (tls.Certificate, *x509.CertPool) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate a key: %v", err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: host},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP(host)},
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template,
		&key.PublicKey, key)
	if err != nil {
		t.Fatalf("create a certificate: %v", err)
	}

	pool := x509.NewCertPool()
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse the certificate: %v", err)
	}
	pool.AddCert(parsed)

	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, pool
}

// The repair writes the source's value straight to the target, outside the
// applier. That is only safe once the target holds everything the source had
// when the comparison read it: anything still buffered applies on top of the
// repair, and for a command that is not idempotent that compounds rather than
// corrects -- a counter repaired to 100 with ten increments still to come ends
// at 110.
//
// Before this, the only guard was Settle, a two-second wait and a second look.
// Settle is a guess at how long replication takes, so whenever the link was
// further behind than that, an ordinary pending change looked like divergence.

func TestARepairWaitsUntilTheTargetHasCaughtUp(t *testing.T) {
	for name, c := range map[string]struct {
		applied func() int64
		head    int64
		want    bool
	}{
		"caught up exactly":    {func() int64 { return 1000 }, 1000, true},
		"past it":              {func() int64 { return 1200 }, 1000, true},
		"still behind":         {func() int64 { return 900 }, 1000, false},
		"far behind":           {func() int64 { return 0 }, 1000, false},
		"nobody can say":       {nil, 1000, false},
		"source would not say": {func() int64 { return 1000 }, 0, false},
	} {
		t.Run(name, func(t *testing.T) {
			r := &Reconciler{Applied: c.applied}
			got, err := r.caughtUpTo(context.Background(), c.head)
			if err != nil {
				t.Fatalf("caughtUpTo: %v", err)
			}
			if got != c.want {
				t.Errorf("caughtUpTo(%d) = %v, want %v", c.head, got, c.want)
			}
		})
	}
}

// TestNotKnowingHowFarBehindMeansNoRepair is the direction that matters. Being
// unable to say is not a reason to write to the target -- the whole hazard is
// repairing over changes that have not arrived.
func TestNotKnowingHowFarBehindMeansNoRepair(t *testing.T) {
	r := &Reconciler{Applied: nil}
	if ok, _ := r.caughtUpTo(context.Background(), 1000); ok {
		t.Error("a reconciler that cannot read the applied offset repaired anyway")
	}

	r = &Reconciler{Applied: func() int64 { return 1 << 40 }}
	if ok, _ := r.caughtUpTo(context.Background(), 0); ok {
		t.Error("a source that would not report its offset was treated as caught up")
	}
}
