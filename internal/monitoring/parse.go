package monitoring

import "strconv"

// ParseInt parses a string to an integer with error handling.
func ParseInt(s string) (int, error) {
	return strconv.Atoi(s)
}
