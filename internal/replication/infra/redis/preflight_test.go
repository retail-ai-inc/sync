package redis

import "testing"

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
