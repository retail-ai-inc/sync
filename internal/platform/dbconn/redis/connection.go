package redis

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// skipVerifyParam is the query parameter the DSN builder uses to ask for an
// encrypted connection whose certificate is not checked, which is the usual
// shape inside a cluster with a private certificate authority. go-redis has no
// URL syntax for it, so it is stripped here and applied to the TLS config.
const skipVerifyParam = "skip_verify"

// GetRedisClient opens a client for one endpoint.
//
// A DSN naming more than one host is read as a cluster. That matters because
// the single-node client has no idea slots exist: pointed at a cluster it would
// answer MOVED for most of the keyspace, and it would not follow a resharding
// or a failover. The cluster client routes each key to the node that owns it.
func GetRedisClient(dsn string) (goredis.UniversalClient, error) {
	cleaned, skipVerify, err := splitSkipVerify(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse redis DSN: %v", err)
	}

	var client goredis.UniversalClient
	if isClusterDSN(cleaned) {
		opt, err := goredis.ParseClusterURL(cleaned)
		if err != nil {
			return nil, fmt.Errorf("failed to parse redis DSN: %v", err)
		}
		// ParseClusterURL takes the whole authority as one address, commas and
		// all, so a DSN naming three seeds becomes one host that resolves to
		// nothing. Splitting it here is what makes a multi-node DSN work at all;
		// without it the client dials "a:1,b:2,c:3" and reports no such host.
		opt.Addrs = splitSeeds(opt.Addrs)
		applySkipVerify(&opt.TLSConfig, skipVerify)
		client = goredis.NewClusterClient(opt)
	} else {
		opt, err := goredis.ParseURL(cleaned)
		if err != nil {
			return nil, fmt.Errorf("failed to parse redis DSN: %v", err)
		}
		applySkipVerify(&opt.TLSConfig, skipVerify)
		client = goredis.NewClient(opt)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("failed to ping redis: %v", err)
	}
	return client, nil
}

// splitSeeds separates the comma-joined seeds a DSN carries.
//
// The DSN builder renders a cluster as one authority with the seeds joined by
// commas, because a URL has room for exactly one host. Every seed is a way in to
// the same cluster: the client asks whichever answers for the slot map and
// connects to the rest itself.
func splitSeeds(addrs []string) []string {
	var seeds []string
	for _, addr := range addrs {
		for _, seed := range strings.Split(addr, ",") {
			if seed = strings.TrimSpace(seed); seed != "" {
				seeds = append(seeds, seed)
			}
		}
	}
	return seeds
}

func isClusterDSN(dsn string) bool {
	u, err := url.Parse(dsn)
	if err != nil {
		return false
	}
	return strings.Contains(u.Host, ",")
}

// splitSkipVerify removes the skip_verify parameter from a DSN and reports
// whether it was set, since neither of go-redis's URL parsers understands it.
func splitSkipVerify(dsn string) (string, bool, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", false, err
	}
	q := u.Query()
	if !q.Has(skipVerifyParam) {
		return dsn, false, nil
	}
	skip := strings.EqualFold(q.Get(skipVerifyParam), "true")
	q.Del(skipVerifyParam)
	u.RawQuery = q.Encode()
	return u.String(), skip, nil
}

// applySkipVerify turns certificate checking off, creating the TLS config when
// the scheme did not already ask for one.
func applySkipVerify(cfg **tls.Config, skip bool) {
	if !skip {
		return
	}
	if *cfg == nil {
		*cfg = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	(*cfg).InsecureSkipVerify = true
}
