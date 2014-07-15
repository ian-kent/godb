package godb

import(
	"sync"
	"errors"
	"strings"
	"crypto/sha1"
	"encoding/binary"
	"github.com/ian-kent/go-log/log"
	"encoding/base64"
)

var ErrNoFields = errors.New("No fields to index")
var ErrIndexAlreadyExists = errors.New("Index already exists")

type Index struct {
	Name string
	Fields []string
	Tree map[byte]*Leaf
	Count int
	Documents []*Document
	Database *Database
}

type Leaf struct {
	Index *Index
	LeafValue []byte
	Lock *sync.Mutex
	Children map[byte]*Leaf
	Documents []*Document
}

func makeIndexName(fields ...string) string {
	ixn := strings.Join(fields, "-")
	log.Trace("Index name: %s", ixn)
	return ixn
}

func NewIndex(db *Database, fields ...string) (*Index, error) {
	if len(fields) == 0 {
		return nil, ErrNoFields
	}

	indexName := makeIndexName(fields...)

	if _, ok := db.Indexes[indexName]; ok {
		return nil, ErrIndexAlreadyExists
	}

	idx := &Index {
		Fields: fields,
		Tree: make(map[byte]*Leaf),
		Count: 0,
		Documents: make([]*Document, 0),
		Database: db,
		Name: indexName,
	}

	db.WriteLock.Lock()
	for _, doc := range db.Documents {
		idx.Index(doc)
	}
	db.WriteLock.Unlock()

	return idx, nil
}

func (idx *Index) FindLeaf(fields map[string]interface{}) *Leaf {
	key := idx.GetIndexHash(fields)
	return idx.GetLeaf(key)
}

func (leaf *Leaf) GetHash() string {
	return base64.StdEncoding.EncodeToString(leaf.LeafValue)
}

func (idx *Index) GetLeaf(value []byte) *Leaf {
	if len(value) == 0 {
		return nil
	}
	if leaf, ok := idx.Tree[value[0]]; ok {
		return leaf.GetLeaf(value[1:])
	}
	idx.Tree[value[0]] = idx.NewLeaf([]byte{value[0]})
	return idx.Tree[value[0]].GetLeaf(value[1:])
}

func (leaf *Leaf) AddDocument(doc *Document) {
	leaf.Lock.Lock()
	leaf.Documents = append(leaf.Documents, doc)
	log.Trace("Adding doc to leaf: %s", leaf.GetHash())
	log.Trace("Doc count: %d", len(leaf.Documents))
	leaf.Lock.Unlock()
}

func (idx *Index) GetIndexHash(fields map[string]interface{}) []byte {
	fh := sha1.New()
	for _, f := range idx.Fields {
		v := fields[f]
		switch v.(type) {
		case string:
			fh.Write([]byte(v.(string)))
		case int:
			b := make([]byte, 8)
			binary.PutVarint(b, int64(v.(int)))
			fh.Write(b)
		default:
			// ?
		}
	}
	b := fh.Sum(nil)
	//log.Trace("Hash: %s", base64.StdEncoding.EncodeToString(b))
	return b
}

func (idx *Index) Index(doc *Document) {
	fm := make(map[string]interface{})
	for _, f := range idx.Fields {
		if v, ok := doc.Fields[f]; ok {
			fm[f] = v
		}
	}
	log.Trace("fm: %s", fm)
	leaf := idx.GetLeaf(idx.GetIndexHash(fm))
	leaf.AddDocument(doc)
	idx.Count += 1
}

func (leaf *Leaf) GetLeaf(value []byte) *Leaf {
	if len(value) == 0 {
		return leaf
	}
	if l, ok := leaf.Children[value[0]]; ok {
		if len(value) == 1 {
			return leaf.Children[value[0]]
		}
		return l.GetLeaf(value[1:])
	}
	leaf.Children[value[0]] = leaf.Index.NewLeaf(append(leaf.LeafValue, value[0]))
	if len(value) == 1 {
		return leaf.Children[value[0]]
	}
	return leaf.Children[value[0]].GetLeaf(value[1:])
}

func (idx *Index) NewLeaf(value []byte) *Leaf {
	return &Leaf{
		Children: make(map[byte]*Leaf),
		Documents: make([]*Document, 0),
		Lock: new(sync.Mutex),
		Index: idx,
		LeafValue: value,
	}
}
