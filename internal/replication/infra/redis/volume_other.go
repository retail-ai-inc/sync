//go:build !linux

package redis

// volumeBytes reports nothing anywhere else, which leaves the buffer's
// built-in limit in place. This runs on Linux; the file exists so a developer's
// machine still builds.
func volumeBytes(string) (int64, error) { return 0, nil }
