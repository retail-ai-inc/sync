package mysql

import (
	"crypto/tls"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
)

// The replication stream carries every row of a payment database across a
// region boundary.
func TestTLSVerifiesTheServerItConnectedTo(t *testing.T) {
	cfg := &mysqldriver.Config{TLSConfig: "true", Addr: "10.60.0.5:3306"}

	got := tlsFor(cfg)
	if got == nil {
		t.Fatal("TLSConfig=true produced no TLS settings")
	}
	if got.InsecureSkipVerify {
		t.Error("verification is off, so anything answering on that address is trusted")
	}
	if got.ServerName != "10.60.0.5" {
		t.Errorf("ServerName = %q, want the host without its port — the certificate "+
			"is issued for a name, not a name and a port", got.ServerName)
	}
	if got.MinVersion < tls.VersionTLS12 {
		t.Errorf("MinVersion = %x, want at least TLS 1.2", got.MinVersion)
	}
}

// TestAHostWithNoPortKeepsItsName. A DSN may carry a bare host, and taking
// everything before the last colon would leave nothing to verify against.
func TestAHostWithNoPortKeepsItsName(t *testing.T) {
	got := tlsFor(&mysqldriver.Config{TLSConfig: "true", Addr: "mysql.internal"})
	if got == nil {
		t.Fatal("no TLS settings")
	}
	if got.ServerName != "mysql.internal" {
		t.Errorf("ServerName = %q, want mysql.internal", got.ServerName)
	}
}

// TestSkipVerifyIsHonouredButStillEncrypts. Asking not to verify is a decision
// somebody made — a self-signed certificate on a private link — and it must not
// quietly become a plaintext connection.
func TestSkipVerifyIsHonouredButStillEncrypts(t *testing.T) {
	got := tlsFor(&mysqldriver.Config{TLSConfig: "skip-verify", Addr: "10.60.0.5:3306"})
	if got == nil {
		t.Fatal("skip-verify produced no TLS settings, so the connection would be plaintext")
	}
	if !got.InsecureSkipVerify {
		t.Error("skip-verify did not skip verification")
	}
	if got.MinVersion < tls.VersionTLS12 {
		t.Errorf("MinVersion = %x, want at least TLS 1.2 even without verification",
			got.MinVersion)
	}
}

// TestNoTLSAskedForIsNoTLSConfigured, rather than a half-configured one that
// would fail in a way nobody could read.
func TestNoTLSAskedForIsNoTLSConfigured(t *testing.T) {
	for _, value := range []string{"", "false", "preferred", "custom"} {
		if got := tlsFor(&mysqldriver.Config{TLSConfig: value, Addr: "h:3306"}); got != nil {
			t.Errorf("TLSConfig=%q produced %+v, want none", value, got)
		}
	}
}

// TestTheSettingIsReadWithoutRegardToCase, because a DSN is written by hand.
func TestTheSettingIsReadWithoutRegardToCase(t *testing.T) {
	for _, value := range []string{"TRUE", "True", "SKIP-VERIFY", "Skip-Verify"} {
		if got := tlsFor(&mysqldriver.Config{TLSConfig: value, Addr: "h:3306"}); got == nil {
			t.Errorf("TLSConfig=%q produced no TLS settings", value)
		}
	}
}
