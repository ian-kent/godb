package godb

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

type TestDoc struct {
	Name string
	Age  int
}

func TestNewDatabase(t *testing.T) {
	db := NewDatabase()
	if assert.NotNil(t, db) {
		assert.NotNil(t, db.Documents)
		assert.Equal(t, len(db.Documents), 0, "contains 0 documents")
		assert.NotNil(t, db.Indexes)
		assert.Equal(t, len(db.Indexes), 0, "contains 0 indexes")
	}
}

func TestInsert(t *testing.T) {
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

	var d0, d250, d500, d750, d999 TestDoc

	db.Documents[0].Unmarshal(&d0)
	db.Documents[250].Unmarshal(&d250)
	db.Documents[500].Unmarshal(&d500)
	db.Documents[750].Unmarshal(&d750)
	db.Documents[999].Unmarshal(&d999)

	assert.Equal(t, d0.Name, "Test document 0")
	assert.Equal(t, d250.Name, "Test document 250")
	assert.Equal(t, d500.Name, "Test document 500")
	assert.Equal(t, d750.Name, "Test document 750")
	assert.Equal(t, d999.Name, "Test document 999")
}

func TestBatchInsert(t *testing.T) {
	db := NewDatabase()

	batch := make([]interface{}, 1000)
	for i := 0; i < 1000; i++ {
		age := rand.Intn(60-20) + 20
		if i%10 == 0 {
			age = 50
		}
		doc := &TestDoc{
			Name: "Test document " + strconv.Itoa(i),
			Age:  age,
		}
		batch[i] = doc
	}

	docs, _ := db.Insert(batch...)

	assert.Equal(t, docs, 1000, "insert return value correct")
	assert.Equal(t, len(db.Documents), 1000, "db contains 1000 documents")

	var d0, d250, d500, d750, d999 TestDoc
	db.Documents[0].Unmarshal(&d0)
	assert.Equal(t, d0.Name, "Test document 0")
	db.Documents[250].Unmarshal(&d250)
	assert.Equal(t, d250.Name, "Test document 250")
	db.Documents[500].Unmarshal(&d500)
	assert.Equal(t, d500.Name, "Test document 500")
	db.Documents[750].Unmarshal(&d750)
	assert.Equal(t, d750.Name, "Test document 750")
	db.Documents[999].Unmarshal(&d999)
	assert.Equal(t, d999.Name, "Test document 999")
}

func TestNewIndex(t *testing.T) {
	db := NewDatabase()

	err := db.NewIndex("Name")
	assert.Nil(t, err, "no error creating index")

	err = db.NewIndex("Name")
	assert.NotNil(t, err, "can't create existing index")
	assert.Equal(t, err, ErrIndexAlreadyExists, "error is ErrIndexAlreadyExists")
}

func TestNewCompoundIndex(t *testing.T) {
	db := NewDatabase()

	err := db.NewIndex("Name", "Age")
	assert.Nil(t, err, "no error creating index")

	err = db.NewIndex("Name", "Age")
	assert.NotNil(t, err, "can't create existing index")
	assert.Equal(t, err, ErrIndexAlreadyExists, "error is ErrIndexAlreadyExists")
}

func TestNewIndexes(t *testing.T) {
	db := NewDatabase()

	err := db.NewIndexes([]string{"Name"}, []string{"Name", "Age"})
	assert.Nil(t, err, "no error creating indexes")

	err = db.NewIndexes([]string{"Name"}, []string{"Name", "Age"})
	assert.NotNil(t, err, "can't create existing index")
	assert.Equal(t, err, ErrIndexAlreadyExists, "error is ErrIndexAlreadyExists")
}

func TestGetIndex(t *testing.T) {
	db := NewDatabase()

	idx := db.GetIndex("Name")
	assert.Nil(t, idx, "nil for uncreated index")

	err := db.NewIndex("Name")
	assert.Nil(t, err, "no error creating index")

	idx = db.GetIndex("Name")
	assert.NotNil(t, idx, "not nil for uncreated index")
	assert.Equal(t, idx.Name, "Name", "index name is Name")
}

func TestFind(t *testing.T) {
	db := NewDatabase()

	dn := 0
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
		dn += n
	}

	assert.Equal(t, dn, 1000, "insert return value correct")
	assert.Equal(t, len(db.Documents), 1000, "db contains 1000 documents")

	n, docs := db.Find(&struct{ Name string }{Name: "Test document 1234"}, 0, 10)
	assert.Equal(t, n, 0, "no results for invalid name")
	assert.Equal(t, len(docs), 0, "results array is empty")

	n, docs = db.Find(&struct{ Name string }{Name: "Test document 500"}, 0, 10)
	assert.Equal(t, n, 1, "1 result for valid name")
	assert.Equal(t, len(docs), 1, "1 item in results array")

	var d TestDoc
	docs[0].Unmarshal(&d)
	assert.Equal(t, d.Name, "Test document 500")
}

func TestFindOne(t *testing.T) {
	db := NewDatabase()

	dn := 0
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
		dn += n
	}

	assert.Equal(t, dn, 1000, "insert return value correct")
	assert.Equal(t, len(db.Documents), 1000, "db contains 1000 documents")

	doc := db.FindOne(&struct{ Name string }{Name: "Test document 1234"}, 0)
	assert.Nil(t, doc, "no result for invalid name")

	doc = db.FindOne(&struct{ Name string }{Name: "Test document 500"}, 0)
	assert.NotNil(t, doc, "result for valid name")

	var d TestDoc
	doc.Unmarshal(&d)
	assert.Equal(t, d.Name, "Test document 500")
}

func BenchmarkInsert(b *testing.B) {
	db := NewDatabase()
	for i := 0; i < b.N; i++ {
		db.Insert(&TestDoc{
			Name: "Test document " + strconv.Itoa(i),
			Age:  50,
		})
	}
}

func BenchmarkBatchInsert(b *testing.B) {
	db := NewDatabase()

	batchSize := 1000
	if batchSize < b.N {
		batchSize = b.N
	}
	rand.Seed(time.Now().Unix())

	var wg sync.WaitGroup
	for i := 0; i < b.N/batchSize; i++ {
		wg.Add(1)
		go func(i int) {
			//log.Info("Starting insert %d", i)
			defer wg.Done()
			batch := make([]interface{}, batchSize)
			for j := 0; j < batchSize; j++ {
				age := rand.Intn(60-20) + 20
				bn := i*batchSize + j
				if bn%1000 == 0 {
					age = 50
				}
				batch[j] = &TestDoc{
					Name: "Test document " + strconv.Itoa(bn),
					Age:  age,
				}
			}
			db.Insert(batch...)
		}(i)
	}
	wg.Wait()
}

func BenchmarkIndexedInsert(b *testing.B) {
	db := NewDatabase()
	db.NewIndex("Name")

	for i := 0; i < b.N; i++ {
		db.Insert(&TestDoc{
			Name: "Test document " + strconv.Itoa(i),
			Age:  50,
		})
	}
}

func BenchmarkIndex(b *testing.B) {
	db := NewDatabase()
	for i := 0; i < b.N; i++ {
		db.Insert(&TestDoc{
			Name: "Test document " + strconv.Itoa(i),
			Age:  50,
		})
	}
	b.ResetTimer()

	db.NewIndex("Name")
}

func BenchmarkIndexedBatchInsert(b *testing.B) {
	db := NewDatabase()
	db.NewIndex("Name")

	batchSize := 1000
	if batchSize < b.N {
		batchSize = b.N
	}
	rand.Seed(time.Now().Unix())

	var wg sync.WaitGroup
	for i := 0; i < b.N/batchSize; i++ {
		wg.Add(1)
		go func(i int) {
			//log.Info("Starting insert %d", i)
			defer wg.Done()
			batch := make([]interface{}, batchSize)
			for j := 0; j < batchSize; j++ {
				age := rand.Intn(60-20) + 20
				bn := i*batchSize + j
				if bn%1000 == 0 {
					age = 50
				}
				batch[j] = &TestDoc{
					Name: "Test document " + strconv.Itoa(bn),
					Age:  age,
				}
			}
			db.Insert(batch...)
		}(i)
	}
	wg.Wait()
}
