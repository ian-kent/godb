package godb

import (
	"crypto/rand"
)

type ObjectID []byte

func NewObjectID() []byte {
	uuid := make([]byte, 16)
	rand.Read(uuid)
	return uuid
}
