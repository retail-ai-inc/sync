package monitoring

import (
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestParseInt(t *testing.T) {
	tests := []struct {
		in      string
		want    int
		wantErr bool
	}{
		{"42", 42, false},
		{"-7", -7, false},
		{"0", 0, false},
		{"", 0, true},
		{"1.5", 0, true},
		{"abc", 0, true},
		{" 42", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := ParseInt(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseInt(%q) succeeded with %d, want an error", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseInt(%q) returned %v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("ParseInt(%q) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}
