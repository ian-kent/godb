package godb

import(
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestNewDatabase(t *testing.T) {
	db := NewDatabase()
	if assert.NotNil(t, db) {
		assert.NotNil(t, db.Documents)
		assert.Equal(t, len(db.Documents), 0, "contains 0 documents")
		assert.NotNil(t, db.Indexes)
		assert.Equal(t, len(db.Indexes), 0, "contains 0 indexes")
	}
}
