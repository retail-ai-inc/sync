package security

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/logging"
	"go.mongodb.org/mongo-driver/bson"
)

// legacyKey is the AES-256 key this has always used when none is configured.
// It is a literal in a public repository, so anything encrypted under it can be
// read by anyone who has the source — which is everyone. It is kept only so that
// values already written to a target under it stay readable; a deployment that
// means it sets SYNC_FIELD_KEY.
var legacyKey = []byte("0123456789abcdef0123456789abcdef") // 32 AES-256

// encryptionKey is what fields are actually sealed with.
var encryptionKey = fieldKey()

// usingLegacyKey says no key was configured, which is worth saying out loud the
// first time a field is encrypted rather than only in a document somewhere.
var usingLegacyKey bool

// legacyWarned keeps that warning to one line per process.
var legacyWarned sync.Once

// fieldKey reads the field encryption key from the environment.
//
// SYNC_FIELD_KEY names it; SYNC_CONFIG_KEY — which seals the stored credentials
// — is accepted as well so that a deployment that has set one key does not have
// to invent a second. Either may be given as 64 hex characters, as base64, or as
// 32 raw bytes.
func fieldKey() []byte {
	for _, name := range []string{"SYNC_FIELD_KEY", "SYNC_CONFIG_KEY"} {
		raw := strings.TrimSpace(os.Getenv(name))
		if raw == "" {
			continue
		}
		key, err := decodeFieldKey(raw)
		if err != nil {
			// Carrying on under the published key while the operator believes
			// their own key is in use is the worst of the three outcomes.
			panic("security: " + name + " is set but cannot be used: " + err.Error())
		}
		return key
	}
	usingLegacyKey = true
	return legacyKey
}

// decodeFieldKey reads a key in either of the forms an operator is likely to
// have it in.
func decodeFieldKey(raw string) ([]byte, error) {
	for _, decode := range []func(string) ([]byte, error){
		hex.DecodeString,
		base64.StdEncoding.DecodeString,
		base64.RawStdEncoding.DecodeString,
	} {
		if key, err := decode(raw); err == nil && len(key) == 32 {
			return key, nil
		}
	}
	if len(raw) == 32 {
		return []byte(raw), nil
	}
	return nil, fmt.Errorf("a key must be 32 bytes, given as 64 hex characters, "+
		"as base64, or raw; got %d characters", len(raw))
}

// warnAboutTheLegacyKey says once that the encryption is not protecting anything.
func warnAboutTheLegacyKey(fieldName string) {
	if !usingLegacyKey {
		return
	}
	legacyWarned.Do(func() {
		logging.Log.Errorf("[Security] Field %q is configured as encrypted, but "+
			"neither SYNC_FIELD_KEY nor SYNC_CONFIG_KEY is set, so it is being "+
			"sealed with the key built into this program — which is published in "+
			"its source. Anyone with the source can read those fields on the "+
			"target. Set SYNC_FIELD_KEY to a key of your own.", fieldName)
	})
}

// Define field security configuration
type FieldSecurityConfig struct {
	Field        string `json:"field"`
	SecurityType string `json:"securityType"` // masked or encrypted
}

// Store table security configuration
type TableSecurity struct {
	SecurityEnabled bool
	FieldSecurity   []FieldSecurityConfig
}

// Encrypt data
func encryptAES(plaintext []byte) (string, error) {
	block, err := aes.NewCipher(encryptionKey)
	if err != nil {
		return "", err
	}

	// GCM Mode
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	// Generate random nonce
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}

	// Encrypt
	ciphertext := gcm.Seal(nonce, nonce, plaintext, nil)

	// Return encrypted data encoded in base64
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// Modify ProcessValue method to handle nested objects
func ProcessValue(value interface{}, fieldName string, config TableSecurity) interface{} {
	if !config.SecurityEnabled {
		logging.Log.Debugf("[Security] Security processing not enabled: field=%s", fieldName)
		return value
	}

	logging.Log.Debugf("[Security] Security processing: field=%s", fieldName)

	// A document: every configured field whose path starts with this one is
	// applied inside it, to whatever depth it names.
	if document, ok := asDocument(value); ok {
		return processDocument(document, fieldName, config)
	}

	// For regular fields, use original processing logic
	for _, fc := range config.FieldSecurity {
		if !strings.EqualFold(fc.Field, fieldName) {
			continue
		}
		{
			logging.Log.Debugf("[Security] Processing top level field: %s = %v, type=%s", fieldName, value, fc.SecurityType)
			// Anything other than the two known kinds used to fall out of the
			// switch with processed still nil, and nil was returned — so a
			// securityType of "Masked", or anything a client had made up, wrote
			// the field to the target as NULL. The comparison is case-insensitive
			// now, and an unrecognised kind leaves the value alone rather than
			// destroying it.
			processed := value

			switch strings.ToLower(strings.TrimSpace(fc.SecurityType)) {
			case "masked":
				processed = maskValue(value)
			case "encrypted":
				warnAboutTheLegacyKey(fieldName)
				switch v := value.(type) {
				case string:
					encrypted, err := encryptAES([]byte(v))
					if err != nil {
						logging.Log.Errorf("[Security] Encryption failed: %v", err)
						return value
					}
					processed = encrypted
				case []byte:
					encrypted, err := encryptAES(v)
					if err != nil {
						logging.Log.Errorf("[Security] Encryption failed: %v", err)
						return value
					}
					processed = encrypted
				default:
					strVal := fmt.Sprintf("%v", v)
					encrypted, err := encryptAES([]byte(strVal))
					if err != nil {
						logging.Log.Errorf("[Security] Encryption failed: %v", err)
						return value
					}
					processed = encrypted
				}
			default:
				logging.Log.Errorf("[Security] Field %q is configured with an "+
					"unknown security type %q, so it is being replicated unchanged. "+
					"The kinds this understands are \"masked\" and \"encrypted\".",
					fieldName, fc.SecurityType)
			}
			logging.Log.Debugf("[Security] After processing: %s = %v", fieldName, processed)
			return processed
		}
	}
	return value
}

// maskValue hides a value while keeping something the target column can hold.
//
// A text value becomes asterisks of the same length. Everything else used to
// become the literal string "****" whatever its type, which for a numeric or
// boolean column on the target is either an error or a truncation — and which
// applied to every VARCHAR read through go-sql-driver too, because that returns
// []byte and []byte was not the string case. Non-text values are replaced with
// the zero of their own type, which hides them and still fits the column.
func maskValue(value interface{}) interface{} {
	switch v := value.(type) {
	case nil:
		// A null field has nothing to hide, and writing "****" over it would
		// make an absent value look like a present one.
		return nil
	case string:
		return strings.Repeat("*", len(v))
	case []byte:
		return []byte(strings.Repeat("*", len(v)))
	case int:
		return int(0)
	case int8:
		return int8(0)
	case int16:
		return int16(0)
	case int32:
		return int32(0)
	case int64:
		return int64(0)
	case uint:
		return uint(0)
	case uint8:
		return uint8(0)
	case uint16:
		return uint16(0)
	case uint32:
		return uint32(0)
	case uint64:
		return uint64(0)
	case float32:
		return float32(0)
	case float64:
		return float64(0)
	case bool:
		return false
	case time.Time:
		return time.Time{}
	default:
		return "****"
	}
}

// A field's path may name something several levels down — "profile.contact.phone"
// — and the rule has to reach it.
//
// It used to reach exactly one level. processNestedObject stripped the parent
// prefix and looked the remainder up as a literal key, so "profile.contact.phone"
// went looking for a key called "contact.phone", did not find one, and left the
// value in the clear. Beside it sat three more attempts at the same job —
// ProcessNestedFieldValue, processNestedObjectValue, getNestedValue,
// processNestedFieldSafe — of which two had no callers at all and the third
// could not be reached, because ProcessValue only handed it values that were not
// documents while its first act was to require one. That was about 190 lines,
// none of which ran.

// asDocument reports the map behind a value, whichever of the two shapes the
// drivers produce it in.
func asDocument(value interface{}) (map[string]interface{}, bool) {
	switch typed := value.(type) {
	case map[string]interface{}:
		return typed, true
	case bson.M:
		return map[string]interface{}(typed), true
	}
	return nil, false
}

// processDocument applies the security rules that name fields inside a document,
// returning a copy. prefix is the path of the document itself, empty for the
// top level.
func processDocument(document map[string]interface{}, prefix string, config TableSecurity) map[string]interface{} {
	result := make(map[string]interface{}, len(document))
	for key, value := range document {
		result[key] = value
	}

	for _, fc := range config.FieldSecurity {
		path, inside := pathWithin(fc.Field, prefix)
		if !inside {
			continue
		}
		applyAtPath(result, path, fc, config)
	}
	return result
}

// pathWithin reports the part of a configured field path that lies inside a
// document at the given prefix.
func pathWithin(field, prefix string) (path []string, inside bool) {
	if prefix == "" {
		return strings.Split(field, "."), true
	}
	rest, found := strings.CutPrefix(field, prefix+".")
	if !found || rest == "" {
		return nil, false
	}
	return strings.Split(rest, "."), true
}

// applyAtPath walks a copy of the document to the named field and replaces it.
//
// Each level it descends through is copied too, so the caller's document is
// never written to — the row a syncer read from the source has to keep the value
// it read.
func applyAtPath(document map[string]interface{}, path []string, fc FieldSecurityConfig, config TableSecurity) {
	if len(path) == 0 {
		return
	}

	key := path[0]
	value, present := document[key]
	if !present {
		return
	}

	if len(path) == 1 {
		document[key] = applyRule(value, key, fc, config)
		return
	}

	child, ok := asDocument(value)
	if !ok {
		logging.Log.Warnf("[Security] %q names a field inside %q, which is a %T "+
			"rather than a document, so it cannot be processed", fc.Field, key, value)
		return
	}

	copied := make(map[string]interface{}, len(child))
	for k, v := range child {
		copied[k] = v
	}
	applyAtPath(copied, path[1:], fc, config)
	document[key] = copied
}

// applyRule applies one field's rule to one value, reusing the top-level
// processing so a nested field and a plain one are treated identically.
func applyRule(value interface{}, key string, fc FieldSecurityConfig, config TableSecurity) interface{} {
	return ProcessValue(value, key, TableSecurity{
		SecurityEnabled: config.SecurityEnabled,
		FieldSecurity:   []FieldSecurityConfig{{Field: key, SecurityType: fc.SecurityType}},
	})
}

func FindTableSecurityFromMappings(tableName string, mappings []config.DatabaseMapping) TableSecurity {
	var result TableSecurity

	logging.Log.Debugf("[Security] Searching table security configuration: tableName=%s, mappingsCount=%d", tableName, len(mappings))

	for i, mapping := range mappings {
		logging.Log.Debugf("[Security] Checking mapping[%d]: contains %d tables", i, len(mapping.Tables))

		for j, table := range mapping.Tables {
			logging.Log.Debugf("[Security] Checking table[%d-%d]: sourceTable=%s, targetTable=%s",
				i, j, table.SourceTable, table.TargetTable)

			if table.SourceTable == tableName || table.TargetTable == tableName {
				result.SecurityEnabled = table.SecurityEnabled
				logging.Log.Debugf("[Security] Table found! SecurityEnabled=%v, FieldSecurityCount=%d",
					result.SecurityEnabled, len(table.FieldSecurity))

				for k, field := range table.FieldSecurity {
					logging.Log.Debugf("[Security] Field security config[%d]: %v", k, field)

					if fieldMap, ok := field.(map[string]interface{}); ok {
						fieldName, _ := fieldMap["field"].(string)
						secType, _ := fieldMap["securityType"].(string)

						logging.Log.Debugf("[Security] Parsing field: field=%s, securityType=%s", fieldName, secType)

						if fieldName != "" && secType != "" {
							result.FieldSecurity = append(result.FieldSecurity, FieldSecurityConfig{
								Field:        fieldName,
								SecurityType: secType,
							})
						}
					}
				}

				return result
			}
		}
	}

	logging.Log.Debugf("[Security] Table security configuration not found")
	return result
}
