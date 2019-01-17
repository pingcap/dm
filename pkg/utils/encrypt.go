package utils

import (
	"encoding/base64"

	"github.com/juju/errors"
	"github.com/pingcap/dm/pkg/encrypt"
)

// Encrypt tries to encrypt plaintext to base64 encoded ciphertext
func Encrypt(plaintext string) (string, error) {
	ciphertext, err := encrypt.Encrypt([]byte(plaintext))
	if err != nil {
		return "", errors.Trace(err)
	}

	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// Decrypt tries to decrypt base64 encoded ciphertext to plaintext
func Decrypt(ciphertextB64 string) (string, error) {
	ciphertext, err := base64.StdEncoding.DecodeString(ciphertextB64)
	if err != nil {
		return "", errors.Trace(err)
	}

	plaintext, err := encrypt.Decrypt(ciphertext)
	if err != nil {
		return "", errors.Trace(err)
	}
	return string(plaintext), nil
}
