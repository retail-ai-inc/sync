package verify

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/retail-ai-inc/sync/internal/replication/infra/security"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	masked    = "masked"
	encrypted = "encrypted"
)

// Protection is the field security replication applies to one table. A nil
// *Protection is a table with none.
type Protection struct {
	policy security.TableSecurity
	// masks is the policy without its encrypted rules: encryptAES draws a random
	// nonce, so an encrypted value cannot be predicted and is never compared.
	masks security.TableSecurity
	// encrypted are the dotted paths an encrypted rule names.
	encrypted []string
}

// ProtectionOf returns nil for a policy that rewrites nothing, so a table
// without field security is compared and repaired exactly as before.
func ProtectionOf(policy security.TableSecurity) *Protection {
	if !policy.SecurityEnabled {
		return nil
	}
	p := &Protection{policy: policy, masks: security.TableSecurity{SecurityEnabled: true}}
	for _, fc := range policy.FieldSecurity {
		switch kindOf(fc) {
		case masked:
			p.masks.FieldSecurity = append(p.masks.FieldSecurity, fc)
		case encrypted:
			p.encrypted = append(p.encrypted, fc.Field)
		}
	}
	if len(p.masks.FieldSecurity) == 0 && len(p.encrypted) == 0 {
		return nil
	}
	return p
}

func kindOf(fc security.FieldSecurityConfig) string {
	return strings.ToLower(strings.TrimSpace(fc.SecurityType))
}

// column reports what replication does to a column. The first rule naming it
// decides, matched case-insensitively, because that is how ProcessValue picks.
func (p *Protection) column(name string) string {
	if p == nil {
		return ""
	}
	for _, fc := range p.policy.FieldSecurity {
		if strings.EqualFold(fc.Field, name) {
			return kindOf(fc)
		}
	}
	return ""
}

func (p *Protection) rewrites(column string) bool {
	kind := p.column(column)
	return kind == masked || kind == encrypted
}

// Protected reports which of the columns replication rewrites.
func (p *Protection) Protected(columns []string) []string {
	var out []string
	for _, c := range columns {
		if p.rewrites(c) {
			out = append(out, c)
		}
	}
	return out
}

// Comparable leaves out the columns replication encrypts.
func (p *Protection) Comparable(columns []string) []string {
	if p == nil {
		return columns
	}
	out := make([]string, 0, len(columns))
	for _, c := range columns {
		if p.column(c) != encrypted {
			out = append(out, c)
		}
	}
	return out
}

// Encrypted reports the dotted field paths replication encrypts in a document.
func (p *Protection) Encrypted() []string {
	if p == nil {
		return nil
	}
	return append([]string(nil), p.encrypted...)
}

// ProtectsID reports whether a rule rewrites a document's _id or part of it.
// Document rules match their path exactly, as the replication path does.
func (p *Protection) ProtectsID() bool {
	if p == nil {
		return false
	}
	for _, fc := range p.policy.FieldSecurity {
		kind := kindOf(fc)
		if (kind == masked || kind == encrypted) && strings.Split(fc.Field, ".")[0] == "_id" {
			return true
		}
	}
	return false
}

// maskedCell is a masked column's source value as the target reads it back.
// value must be scanned into an interface{}, as replication scans it, so a
// number masks to 0 there and here alike.
func (p *Protection) maskedCell(column string, value interface{}) (sql.NullString, error) {
	var cell sql.NullString
	if err := cell.Scan(security.ProcessValue(value, column, p.policy)); err != nil {
		return sql.NullString{}, fmt.Errorf("render the masked value of %s: %w", column, err)
	}
	return cell, nil
}

// written is what replication writes for a column's source value. It never
// returns the raw value of a column replication encrypts.
func (p *Protection) written(column string, value interface{}) (interface{}, error) {
	kind := p.column(column)
	if kind == encrypted && !security.KeyConfigured() {
		return nil, fmt.Errorf("%s: %w", column, security.ErrNoFieldKey)
	}
	out := security.ProcessValue(value, column, p.policy)
	// ProcessValue hands back its input when encryption fails.
	if kind == encrypted && reflect.DeepEqual(out, value) {
		return nil, fmt.Errorf("%s could not be encrypted", column)
	}
	return out, nil
}

// comparable is a source document as the digest sees it: masked the way
// replication masks it. Encrypted fields are left for Skip to remove.
func (p *Protection) comparable(doc bson.M) bson.M {
	if p == nil || len(p.masks.FieldSecurity) == 0 {
		return doc
	}
	return processDocument(doc, p.masks)
}

// document is what replication writes for a source document. It never returns
// the raw value of a field replication encrypts.
func (p *Protection) document(doc bson.M) (bson.M, error) {
	if p == nil {
		return doc, nil
	}
	if len(p.encrypted) > 0 && !security.KeyConfigured() {
		return nil, security.ErrNoFieldKey
	}
	for _, fc := range p.policy.FieldSecurity {
		if kind := kindOf(fc); kind != masked && kind != encrypted {
			continue
		}
		// ProcessValue leaves a subdocument a rule names unchanged, which would
		// write it here in the clear.
		value, _ := valueAt(doc, strings.Split(fc.Field, "."))
		if _, isDocument := asDocument(value); isDocument {
			return nil, fmt.Errorf("%s is a document, which cannot be written %s", fc.Field, kindOf(fc))
		}
	}
	out := processDocument(doc, p.policy)
	for _, field := range p.encrypted {
		path := strings.Split(field, ".")
		before, present := valueAt(doc, path)
		if !present {
			continue
		}
		// ProcessValue hands back its input when encryption fails.
		if after, _ := valueAt(out, path); reflect.DeepEqual(before, after) {
			return nil, fmt.Errorf("%s could not be encrypted", field)
		}
	}
	return out, nil
}

// processDocument applies a policy to a document the way the MongoDB syncer's
// maskDocument does.
func processDocument(doc bson.M, policy security.TableSecurity) bson.M {
	processed, ok := security.ProcessValue(map[string]interface{}(doc), "", policy).(map[string]interface{})
	if !ok {
		return doc
	}
	return bson.M(processed)
}

// asDocument reports the fields of a value that is a document, in any of the
// shapes the driver and ProcessValue produce one in.
func asDocument(value interface{}) (map[string]interface{}, bool) {
	switch typed := value.(type) {
	case bson.M:
		return typed, true
	case map[string]interface{}:
		return typed, true
	case bson.D:
		fields := make(map[string]interface{}, len(typed))
		for _, element := range typed {
			fields[element.Key] = element.Value
		}
		return fields, true
	}
	return nil, false
}

// valueAt reads the field a dotted path names, descending only through
// documents, as ProcessValue does.
func valueAt(doc bson.M, path []string) (interface{}, bool) {
	var current interface{} = doc
	for _, key := range path {
		fields, ok := asDocument(current)
		if !ok {
			return nil, false
		}
		if current, ok = fields[key]; !ok {
			return nil, false
		}
	}
	return current, true
}

// without returns the document with the named dotted paths removed, descending
// only through documents. A subdocument at a named path is kept: ProcessValue
// does not encrypt one, so it is compared as replication writes it. Every level
// it changes is copied; the input is never written to.
func without(doc bson.M, fields []string) bson.M {
	for _, field := range fields {
		doc = bson.M(withoutPath(doc, strings.Split(field, ".")))
	}
	return doc
}

func withoutPath(fields map[string]interface{}, path []string) map[string]interface{} {
	child, present := fields[path[0]]
	if !present {
		return fields
	}
	inner, isDocument := asDocument(child)
	if len(path) == 1 && isDocument {
		return fields
	}
	if len(path) > 1 {
		if !isDocument {
			return fields
		}
		child = withoutPath(inner, path[1:])
	}

	copied := make(map[string]interface{}, len(fields))
	for k, v := range fields {
		copied[k] = v
	}
	if len(path) == 1 {
		delete(copied, path[0])
	} else {
		copied[path[0]] = child
	}
	return copied
}
