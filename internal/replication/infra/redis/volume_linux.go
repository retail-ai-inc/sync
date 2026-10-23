//go:build linux

package redis

import "golang.org/x/sys/unix"

// volumeBytes reports how big the filesystem holding a directory is.
func volumeBytes(dir string) (int64, error) {
	var fs unix.Statfs_t
	if err := unix.Statfs(dir, &fs); err != nil {
		return 0, err
	}
	return int64(fs.Blocks) * int64(fs.Bsize), nil
}
