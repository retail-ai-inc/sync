package security

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"strings"

	"github.com/retail-ai-inc/sync/pkg/config"
	"github.com/retail-ai-inc/sync/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
)

// Encryption key - should be managed securely in production environment
var encryptionKey = []byte("0123456789abcdef0123456789abcdef") // 32 AES-256

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
		logger.Log.Debugf("[Security] Security processing not enabled: field=%s", fieldName)
		return value
	}

	logger.Log.Debugf("[Security] Security processing: field=%s", fieldName)

	// First check if it's a nested object
	if nested, ok := value.(map[string]interface{}); ok {
		logger.Log.Debugf("[Security] Found nested object: %s", fieldName)
		return processNestedObject(nested, fieldName, config)
	}
	// Check bson.M type
	if bsonM, ok := value.(bson.M); ok {
		logger.Log.Debugf("[Security] Found bson.M object: %s", fieldName)
		nested := map[string]interface{}(bsonM)
		return processNestedObject(nested, fieldName, config)
	}

	// Check if it's a nested field path (contains dot)
	if strings.Contains(fieldName, ".") {
		logger.Log.Debugf("[Security] Nested path requires special handling: %s", fieldName)
		return ProcessNestedFieldValue(value, fieldName, config)
	}

	// For regular fields, use original processing logic
	for _, fc := range config.FieldSecurity {
		if fc.Field == fieldName {
			logger.Log.Debugf("[Security] Processing top level field: %s = %v, type=%s", fieldName, value, fc.SecurityType)
			var processed interface{}

			switch fc.SecurityType {
			case "masked":
				switch v := value.(type) {
				case string:
					processed = strings.Repeat("*", len(v))
				default:
					processed = "****"
				}
			case "encrypted":
				switch v := value.(type) {
				case string:
					encrypted, err := encryptAES([]byte(v))
					if err != nil {
						logger.Log.Errorf("[Security] Encryption failed: %v", err)
						return value
					}
					processed = encrypted
				case []byte:
					encrypted, err := encryptAES(v)
					if err != nil {
						logger.Log.Errorf("[Security] Encryption failed: %v", err)
						return value
					}
					processed = encrypted
				default:
					strVal := fmt.Sprintf("%v", v)
					encrypted, err := encryptAES([]byte(strVal))
					if err != nil {
						logger.Log.Errorf("[Security] Encryption failed: %v", err)
						return value
					}
					processed = encrypted
				}
			}
			logger.Log.Debugf("[Security] After processing: %s = %v", fieldName, processed)
			return processed
		}
	}
	return value
}

// Add new function to process nested objects
func processNestedObject(nested map[string]interface{}, parentField string, config TableSecurity) interface{} {
	logger.Log.Debugf("[Security] Processing nested object fields: parent=%s", parentField)
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

			logger.Log.Debugf("[Security] Found nested field to process: %s.%s, type=%s",
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
				logger.Log.Debugf("[Security] Processing sub-field: %s.%s = %v", parentField, subField, value)
				processed := ProcessValue(value, subField, tempConfig)
				result[subField] = processed
				logger.Log.Debugf("[Security] Sub-field processing complete: %s.%s = %v", parentField, subField, processed)
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
			logger.Log.Warnf("[Security] Nested processing failed: value is not object type field=%s type=%T", fieldPath, value)
			return value
		}
	}

	logger.Log.Debugf("[Security] Processing nested object field: %s", fieldPath)

	// Find matching nested field configuration
	for _, fc := range config.FieldSecurity {
		if fc.Field == fieldPath {
			// Find matching nested path configuration
			paths := strings.Split(fieldPath, ".")
			if len(paths) < 2 {
				logger.Log.Warnf("[Security] Invalid nested path: %s", fieldPath)
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
				logger.Log.Errorf("[Security] Failed to navigate nested path: %s is not an object", strings.Join(paths[:i+1], "."))
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
			logger.Log.Errorf("[Security] Failed to get parent object: %s is not an object", strings.Join(paths[:len(paths)-1], "."))
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
		logger.Log.Debugf("[Security] Nested processing: path=%s, original value=%v", strings.Join(paths, "."), finalValue)
		processed := ProcessValue(finalValue, lastField, tempConfig)
		parent[lastField] = processed
		logger.Log.Debugf("[Security] Nested processing complete: path=%s, processed=%v", strings.Join(paths, "."), processed)
	} else {
		logger.Log.Warnf("[Security] Final field in nested path does not exist: %s", strings.Join(paths, "."))
	}
}

func FindTableSecurityFromMappings(tableName string, mappings []config.DatabaseMapping) TableSecurity {
	var result TableSecurity

	logger.Log.Debugf("[Security] Searching table security configuration: tableName=%s, mappingsCount=%d", tableName, len(mappings))

	for i, mapping := range mappings {
		logger.Log.Debugf("[Security] Checking mapping[%d]: contains %d tables", i, len(mapping.Tables))

		for j, table := range mapping.Tables {
			logger.Log.Debugf("[Security] Checking table[%d-%d]: sourceTable=%s, targetTable=%s",
				i, j, table.SourceTable, table.TargetTable)

			if table.SourceTable == tableName || table.TargetTable == tableName {
				result.SecurityEnabled = table.SecurityEnabled
				logger.Log.Debugf("[Security] Table found! SecurityEnabled=%v, FieldSecurityCount=%d",
					result.SecurityEnabled, len(table.FieldSecurity))

				for k, field := range table.FieldSecurity {
					logger.Log.Debugf("[Security] Field security config[%d]: %v", k, field)

					if fieldMap, ok := field.(map[string]interface{}); ok {
						fieldName, _ := fieldMap["field"].(string)
						secType, _ := fieldMap["securityType"].(string)

						logger.Log.Debugf("[Security] Parsing field: field=%s, securityType=%s", fieldName, secType)

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

	logger.Log.Debugf("[Security] Table security configuration not found")
	return result
}

// Get nested path value
func getNestedValue(doc map[string]interface{}, paths []string) interface{} {
	if doc == nil || len(paths) == 0 {
		return nil
	}

	current := doc
	for i := 0; i < len(paths)-1; i++ {
		if nested, ok := current[paths[i]].(map[string]interface{}); ok {
			current = nested
		} else {
			return nil // Path does not exist
		}
	}

	// Return last level value
	return current[paths[len(paths)-1]]
}

// Modify processNestedFieldSafe, add more detailed diagnostic information
func processNestedFieldSafe(doc map[string]interface{}, paths []string, securityType string) {
	// If path is invalid or document is empty, return directly
	if len(paths) == 0 || doc == nil {
		logger.Log.Errorf("[Security] Nested path processing failed: invalid path or document")
		return
	}

	fullPath := strings.Join(paths, ".")
	logger.Log.Debugf("[Security] === Start processing nested field: %s ===", fullPath)
	logger.Log.Debugf("[Security] Security type: %s, path array: %v", securityType, paths)
	logger.Log.Debugf("[Security] Document content: %+v", doc)

	// Limit maximum nesting depth to prevent potential recursion issues
	if len(paths) > 10 {
		logger.Log.Errorf("[Security] Nested path too deep (>10 levels), skipped: %s", fullPath)
		return
	}

	current := doc
	logger.Log.Debugf("[Security] Start navigating nested path...")

	for i := 0; i < len(paths)-1; i++ {
		path := paths[i]
		currentPath := strings.Join(paths[:i+1], ".")
		logger.Log.Debugf("[Security] Navigating to: %s", currentPath)

		// If current path doesn't exist, end processing
		if _, exists := current[path]; !exists {
			logger.Log.Errorf("[Security] Path does not exist: %s", currentPath)
			return
		}

		logger.Log.Debugf("[Security] Path exists: %s, value type: %T", currentPath, current[path])

		// If current node is not an object, cannot continue processing
		nextMap, ok := current[path].(map[string]interface{})
		if !ok {
			logger.Log.Errorf("[Security] Field is not an object: %s, value type: %T, value: %v",
				currentPath, current[path], current[path])
			return
		}

		logger.Log.Debugf("[Security] Successfully navigated to: %s", currentPath)
		current = nextMap
	}

	// Process last level field
	lastField := paths[len(paths)-1]
	logger.Log.Debugf("[Security] Reached final field: %s", lastField)

	if value, exists := current[lastField]; exists {
		logger.Log.Debugf("[Security] Found nested value: %s = %v", fullPath, value)

		// Simulate TableSecurity config to process single field
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
		logger.Log.Debugf("[Security] Start processing nested value...")
		processed := ProcessValue(value, lastField, tempConfig)
		current[lastField] = processed
		logger.Log.Debugf("[Security] Nested field processing complete: %s", fullPath)
		logger.Log.Debugf("[Security]   - Before processing: %v", value)
		logger.Log.Debugf("[Security]   - After processing: %v", processed)
		logger.Log.Debugf("[Security]   - Changed: %v", value != processed)
	} else {
		logger.Log.Errorf("[Security] Field does not exist in object: %s", fullPath)
	}

	logger.Log.Debugf("[Security] === Nested field processing complete: %s ===", fullPath)
}
