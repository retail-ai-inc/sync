// Package secret encrypts the credentials the configuration database holds.
//
// A replication task stores the connection settings for both of its databases,
// passwords included, as JSON in a SQLite file. Masking them on the way out of
// the API — which is done — does nothing about that file: anybody who can read
// it has the credentials for both regions' payment databases. A backup, a volume
// snapshot, or one `cat` inside the pod is enough.
//
// The values are sealed with AES-256-GCM under a key supplied out of band. It is
// deliberately not stored anywhere near the file it protects: the point is that
// having the file is not enough.
package secret

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// prefix marks a sealed value. It exists so a database holding a mix of sealed
// and plaintext values — which is every database part-way through the first
// deployment that has a key — can be read without knowing which is which.
const prefix = "enc:v1:"

var ErrNoKey = errors.New("a stored credential is encrypted and SYNC_CONFIG_KEY is not set")

type Keeper struct {
	aead cipher.AEAD
}

// Default is the keeper the configuration store uses. It is nil when no key is
// configured, in which case values are stored as they always were.
var Default *Keeper

func init() {
	keeper, err := KeeperFromEnv()
	if err != nil {
		// A key that is present but unusable is a configuration mistake worth
		// stopping for: carrying on would write credentials in the clear while
		// the operator believed otherwise.
		panic("secret: SYNC_CONFIG_KEY is set but cannot be used: " + err.Error())
	}
	Default = keeper
}

func Configured() bool { return Default != nil }

// KeeperFromEnv builds a keeper from SYNC_CONFIG_KEY, reporting nil when the
// variable is unset.
//
// The key may be given as base64 or hex and has to decode to 32 bytes, which is
// what AES-256 takes. Anything else is refused rather than stretched into one:
// a key derived from a short string would look like encryption and provide much
// less of it.
func KeeperFromEnv() (*Keeper, error) {
	raw := strings.TrimSpace(os.Getenv("SYNC_CONFIG_KEY"))
	if raw == "" {
		return nil, nil
	}

	key, err := decodeKey(raw)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Keeper{aead: aead}, nil
}

// decodeKey reads a key in either of the two forms an operator is likely to
// have it in.
func decodeKey(raw string) ([]byte, error) {
	for _, decode := range []func(string) ([]byte, error){
		base64.StdEncoding.DecodeString,
		base64.RawStdEncoding.DecodeString,
		hex.DecodeString,
	} {
		if key, err := decode(raw); err == nil && len(key) == 32 {
			return key, nil
		}
	}
	return nil, fmt.Errorf("the key must be 32 bytes, given as base64 or hex")
}

// Seal encrypts a value. A value that is already sealed is returned unchanged,
// so re-writing a configuration does not encrypt it twice.
func (k *Keeper) Seal(plaintext string) (string, error) {
	if k == nil || plaintext == "" || IsSealed(plaintext) {
		return plaintext, nil
	}

	nonce := make([]byte, k.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("generate a nonce: %w", err)
	}
	sealed := k.aead.Seal(nonce, nonce, []byte(plaintext), nil)
	return prefix + base64.RawStdEncoding.EncodeToString(sealed), nil
}

// Open decrypts a value. A value that was never sealed is returned unchanged,
// which is how a database written before a key was configured keeps working.
func (k *Keeper) Open(stored string) (string, error) {
	if !IsSealed(stored) {
		return stored, nil
	}
	if k == nil {
		return "", ErrNoKey
	}

	raw, err := base64.RawStdEncoding.DecodeString(strings.TrimPrefix(stored, prefix))
	if err != nil {
		return "", fmt.Errorf("read a sealed credential: %w", err)
	}
	if len(raw) < k.aead.NonceSize() {
		return "", fmt.Errorf("read a sealed credential: it is too short to be one")
	}

	nonce, body := raw[:k.aead.NonceSize()], raw[k.aead.NonceSize():]
	plaintext, err := k.aead.Open(nil, nonce, body, nil)
	if err != nil {
		return "", fmt.Errorf("open a sealed credential: %w. The key may not be the "+
			"one it was sealed with", err)
	}
	return string(plaintext), nil
}

func IsSealed(stored string) bool { return strings.HasPrefix(stored, prefix) }

// ---------------------------------------------------------- task documents

// credentialKeys are the fields of a task's connection settings that are worth
// protecting. The host and the database name are not secrets and an operator
// reading the file needs to be able to tell which task is which.
var credentialKeys = []string{"password"}

// connectionKeys are the objects inside a task's configuration that hold
// connection settings.
var connectionKeys = []string{"sourceConn", "targetConn"}

// SealTaskConfig returns a task's stored JSON with its credentials encrypted.
//
// It works on the JSON rather than on a typed value because the same document is
// parsed by two packages with two different shapes, and a field neither of them
// models must not be dropped on the way through.
func SealTaskConfig(configJSON string) (string, error) {
	return transformTaskConfig(configJSON, func(value string) (string, error) {
		return Default.Seal(value)
	})
}

func OpenTaskConfig(configJSON string) (string, error) {
	return transformTaskConfig(configJSON, func(value string) (string, error) {
		return Default.Open(value)
	})
}

func transformTaskConfig(configJSON string, transform func(string) (string, error)) (string, error) {
	if strings.TrimSpace(configJSON) == "" {
		return configJSON, nil
	}

	var document map[string]interface{}
	if err := json.Unmarshal([]byte(configJSON), &document); err != nil {
		// A document that cannot be parsed is left exactly as it is. It is
		// already broken, and rewriting it would replace one problem with a
		// less obvious one.
		return configJSON, nil
	}

	changed := false
	for _, connectionKey := range connectionKeys {
		connection, ok := document[connectionKey].(map[string]interface{})
		if !ok {
			continue
		}
		for _, field := range credentialKeys {
			current, ok := connection[field].(string)
			if !ok || current == "" {
				continue
			}
			replacement, err := transform(current)
			if err != nil {
				return "", fmt.Errorf("%s.%s: %w", connectionKey, field, err)
			}
			if replacement != current {
				connection[field] = replacement
				changed = true
			}
		}
	}
	if !changed {
		return configJSON, nil
	}

	rewritten, err := json.Marshal(document)
	if err != nil {
		return "", err
	}
	return string(rewritten), nil
}
