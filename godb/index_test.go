package godb

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"strconv"
	"testing"
)

func TestNewPostInsertIndex(t *testing.T) {
	db := NewDatabase()
	docs := 0

	for i := 0; i < 1000; i++ {
		age := rand.Intn(60-20) + 20
		if i%10 == 0 {
			age = 50
		}
		doc := &TestDoc{
			Name: "Test document " + strconv.Itoa(i),
			Age:  age,
		}
		n, _ := db.Insert(doc)
		docs += n
	}

	assert.Equal(t, docs, 1000, "insert return value correct")
	assert.Equal(t, len(db.Documents), 1000, "db contains 1000 documents")

	err := db.NewIndex("Name")
	assert.Nil(t, err, "no error creating index")

	err = db.NewIndex("Name")
	assert.NotNil(t, err, "can't create existing index")
	assert.Equal(t, err, ErrIndexAlreadyExists, "error is ErrIndexAlreadyExists")

	assert.Equal(t, len(db.Indexes), 1, "index has been created")
	assert.Equal(t, db.GetIndex("Name").Count, 1000, "index contains 1000 documents")
}

func TestNewPreInsertIndex(t *testing.T) {
	db := NewDatabase()
	docs := 0

	err := db.NewIndex("Name")
	assert.Nil(t, err, "no error creating index")

	err = db.NewIndex("Name")
	assert.NotNil(t, err, "can't create existing index")
	assert.Equal(t, err, ErrIndexAlreadyExists, "error is ErrIndexAlreadyExists")

	assert.Equal(t, len(db.Indexes), 1, "index has been created")
	assert.Equal(t, db.GetIndex("Name").Count, 0, "index contains 0 documents")

	for i := 0; i < 1000; i++ {
		age := rand.Intn(60-20) + 20
		if i%10 == 0 {
			age = 50
		}
		doc := &TestDoc{
			Name: "Test document " + strconv.Itoa(i),
			Age:  age,
		}
		n, _ := db.Insert(doc)
		docs += n
	}

	assert.Equal(t, docs, 1000, "insert return value correct")
	assert.Equal(t, len(db.Documents), 1000, "db contains 1000 documents")

	assert.Equal(t, db.GetIndex("Name").Count, 1000, "index contains 1000 documents")
}
