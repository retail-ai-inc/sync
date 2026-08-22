package mongodb

import (
	"os"
	"path/filepath"
	"strings"
)

// A collection's buffered events, its dead letters and its resume token are all
// kept under a name built from the database and the collection. That name used
// to be "<db>_<coll>", which is not reversible: the collection "daily" of
// database "shop_orders" and the collection "orders_daily" of database "shop"
// produce the same one. Within a single task the database is fixed so nothing
// collides, but the natural way to configure several tasks is to point them at
// one state directory — and then two collections share a buffer directory,
// their events interleave, and each overwrites the other's resume token.

// namePart escapes one component so that the join below can be undone, and so
// that a name from the database cannot reach outside the state directory.
func namePart(part string) string {
	var escaped strings.Builder
	for _, r := range part {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '-', r == '.':
			escaped.WriteRune(r)
		default:
			// %XX, which is why '%' itself is escaped as well.
			escaped.WriteString("%")
			escaped.WriteString(strings.ToUpper(hexBytes(r)))
		}
	}
	return escaped.String()
}

// hexBytes renders one rune as its bytes in hexadecimal.
func hexBytes(r rune) string {
	const digits = "0123456789abcdef"
	var out strings.Builder
	for _, b := range []byte(string(r)) {
		out.WriteByte(digits[b>>4])
		out.WriteByte(digits[b&0x0f])
	}
	return out.String()
}

// collectionKey names the state belonging to one collection.
func collectionKey(db, coll string) string {
	return namePart(db) + "_" + namePart(coll)
}

// adoptOldName moves state written under the old, ambiguous name to the new one.
//
// Without this an upgrade orphans whatever was buffered: the syncer looks under
// the new name, finds nothing, and the events sit on disk until somebody
// notices the volume filling.
func adoptOldName(dir, db, coll string) {
	old := filepath.Join(dir, db+"_"+coll)
	current := filepath.Join(dir, collectionKey(db, coll))
	if old == current {
		return
	}
	if _, err := os.Stat(old); err != nil {
		return
	}
	if _, err := os.Stat(current); err == nil {
		return // the new name is already in use; leave both alone
	}
	_ = os.Rename(old, current)
}
