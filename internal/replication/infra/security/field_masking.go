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

	// First check if it's a nested object
	if nested, ok := value.(map[string]interface{}); ok {
		logging.Log.Debugf("[Security] Found nested object: %s", fieldName)
		return processNestedObject(nested, fieldName, config)
	}
	// Check bson.M type
	if bsonM, ok := value.(bson.M); ok {
		logging.Log.Debugf("[Security] Found bson.M object: %s", fieldName)
		nested := map[string]interface{}(bsonM)
		return processNestedObject(nested, fieldName, config)
	}

	// Check if it's a nested field path (contains dot)
	if strings.Contains(fieldName, ".") {
		logging.Log.Debugf("[Security] Nested path requires special handling: %s", fieldName)
		return ProcessNestedFieldValue(value, fieldName, config)
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

// Add new function to process nested objects
func processNestedObject(nested map[string]interface{}, parentField string, config TableSecurity) interface{} {
	logging.Log.Debugf("[Security] Processing nested object fields: parent=%s", parentField)
	result := make(map[string]interface{})

	// Copy all fields
	for k, v := range nested {
		result[k] = v
	}

	// Check each security field configuration
	for _, fc := range config.FieldSecurity {
		// Check if it's a field of current nested object
		if strings.HasPrefix(fc.Field, parentField+".") {
			// Get sub-field name
			subField := strings.TrimPrefix(fc.Field, parentField+".")
			if subField == "" {
				continue
			}

			logging.Log.Debugf("[Security] Found nested field to process: %s.%s, type=%s",
				parentField, subField, fc.SecurityType)

			// If sub-field exists in nested object
			if value, exists := result[subField]; exists {
				// Create temporary config for sub-field processing
				tempConfig := TableSecurity{
					SecurityEnabled: true,
					FieldSecurity: []FieldSecurityConfig{
						{
							Field:        subField,
							SecurityType: fc.SecurityType,
						},
					},
				}

				// Process sub-field value
				logging.Log.Debugf("[Security] Processing sub-field: %s.%s = %v", parentField, subField, value)
				processed := ProcessValue(value, subField, tempConfig)
				result[subField] = processed
				logging.Log.Debugf("[Security] Sub-field processing complete: %s.%s = %v", parentField, subField, processed)
			}
		}
	}

	return result
}

// Add new method to specifically handle nested field values (for MongoDB and other document databases)
func ProcessNestedFieldValue(value interface{}, fieldPath string, config TableSecurity) interface{} {
	// Only process nested object types
	nested, ok := value.(map[string]interface{})
	if !ok {
		// Try to process bson.M type
		bsonM, ok := value.(bson.M)
		if ok {
			// Convert bson.M to map[string]interface{}
			nested = map[string]interface{}(bsonM)
		} else {
			logging.Log.Warnf("[Security] Nested processing failed: value is not object type field=%s type=%T", fieldPath, value)
			return value
		}
	}

	logging.Log.Debugf("[Security] Processing nested object field: %s", fieldPath)

	// Find matching nested field configuration
	for _, fc := range config.FieldSecurity {
		if fc.Field == fieldPath {
			// Find matching nested path configuration
			paths := strings.Split(fieldPath, ".")
			if len(paths) < 2 {
				logging.Log.Warnf("[Security] Invalid nested path: %s", fieldPath)
				return value
			}

			// Create copy of processed nested object
			result := make(map[string]interface{})
			for k, v := range nested {
				result[k] = v
			}

			// Process nested path
			processNestedObjectValue(result, paths, fc.SecurityType)
			return result
		}
	}

	return value
}

// Process specific path value in nested object
func processNestedObjectValue(obj map[string]interface{}, paths []string, securityType string) {
	if len(paths) < 2 || obj == nil {
		return
	}

	current := obj
	// Navigate to second-to-last level of nested path
	for i := 0; i < len(paths)-2; i++ {
		path := paths[i]
		next, ok := current[path].(map[string]interface{})
		if !ok {
			// Try bson.M type
			bsonM, ok := current[path].(bson.M)
			if ok {
				next = map[string]interface{}(bsonM)
				current[path] = next
			} else {
				logging.Log.Errorf("[Security] Failed to navigate nested path: %s is not an object", strings.Join(paths[:i+1], "."))
				return
			}
		}
		current = next
	}

	// Get field names of second-to-last and last levels
	parentField := paths[len(paths)-2]
	lastField := paths[len(paths)-1]

	// Get parent object
	parent, ok := current[parentField].(map[string]interface{})
	if !ok {
		// Try bson.M type
		bsonM, ok := current[parentField].(bson.M)
		if ok {
			parent = map[string]interface{}(bsonM)
			current[parentField] = parent
		} else {
			logging.Log.Errorf("[Security] Failed to get parent object: %s is not an object", strings.Join(paths[:len(paths)-1], "."))
			return
		}
	}

	// Get final value to process
	if finalValue, exists := parent[lastField]; exists {
		// Create temporary security config for final value
		tempConfig := TableSecurity{
			SecurityEnabled: true,
			FieldSecurity: []FieldSecurityConfig{
				{
					Field:        lastField,
					SecurityType: securityType,
				},
			},
		}

		// Process value and update
		logging.Log.Debugf("[Security] Nested processing: path=%s, original value=%v", strings.Join(paths, "."), finalValue)
		processed := ProcessValue(finalValue, lastField, tempConfig)
		parent[lastField] = processed
		logging.Log.Debugf("[Security] Nested processing complete: path=%s, processed=%v", strings.Join(paths, "."), processed)
	} else {
		logging.Log.Warnf("[Security] Final field in nested path does not exist: %s", strings.Join(paths, "."))
	}
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
