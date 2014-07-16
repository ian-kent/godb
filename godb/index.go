package godb

import(
	"sync"
	"errors"
	"strings"
	"crypto/sha1"
	"encoding/binary"
	"github.com/ian-kent/go-log/log"
	"encoding/base64"
	"bytes"
)

var ErrNoFields = errors.New("No fields to index")
var ErrIndexAlreadyExists = errors.New("Index already exists")

type Index struct {
	Name string
	Fields []string
	Tree *Leaf
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
	Unsplit []byte
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
		Count: 0,
		Documents: make([]*Document, 0),
		Database: db,
		Name: indexName,
	}
	idx.Tree = idx.NewLeaf([]byte{})

	db.WriteLock.Lock()
	for _, doc := range db.Documents {
		idx.Index(doc)
	}
	db.WriteLock.Unlock()

	return idx, nil
}

func (idx *Index) FindLeaf(fields map[string]interface{}) *Leaf {
	key := idx.GetIndexHash(fields)
	return idx.Tree.GetLeaf(key, 0)
}

func bytesToHash(value []byte) string {
	return base64.StdEncoding.EncodeToString(value)
}

func (leaf *Leaf) GetHash() string {
	return bytesToHash(leaf.LeafValue)
}

func (leaf *Leaf) AddDocument(doc *Document, value []byte) *Leaf {
	leaf.Lock.Lock()
	defer leaf.Lock.Unlock()

	// FIXME shouldn't end up here, needs refactoring
	//if len(leaf.Children) > 0 {
	//	log.Error("ERROR - leaf has children")
	//	return leaf
	//}

	if leaf.Unsplit != nil && bytes.Equal(leaf.Unsplit, value) {
		// reuse this leaf
		//log.Trace("Adding doc to leaf: %s", func() string { return leaf.GetHash() })
		leaf.Documents = append(leaf.Documents, doc)
		//log.Trace("Doc count: %d", len(leaf.Documents))
		return leaf
	}

	if leaf.Unsplit != nil {
		// split this leaf
		//log.Trace("Need to split leaf %s (unsplit for %s) for value %s", func() (string, string, string) { return leaf.GetHash(), bytesToHash(leaf.Unsplit), bytesToHash(value) })
		unsplit := leaf.Unsplit
		leaf.Children[unsplit[len(leaf.LeafValue)]] = leaf.Index.NewLeaf(unsplit[:len(leaf.LeafValue)+1])
		leaf.Children[unsplit[len(leaf.LeafValue)]].Documents = leaf.Documents
		leaf.Children[unsplit[len(leaf.LeafValue)]].Unsplit = unsplit
		leaf.Unsplit = nil
		leaf.Documents = make([]*Document, 0)
		leaf.Children[value[len(leaf.LeafValue)]] = leaf.Index.NewLeaf(value[:len(leaf.LeafValue)+1])
		l := leaf.Children[value[len(leaf.LeafValue)]]		
		l.AddDocument(doc, value)
		return l
	}

	if leaf.Unsplit == nil && len(leaf.Documents) == 0 {
		// take this leaf
		//log.Trace("Adding doc to leaf: %s", func() string { return leaf.GetHash() })
		leaf.Unsplit = value
		leaf.Documents = append(leaf.Documents, doc)
		//log.Trace("Doc count: %d", len(leaf.Documents))
		return leaf
	}

	log.Error("ERROR - Doc %s doesn't in expected leaf %s", bytesToHash(value), leaf.GetHash())
	log.Error("  Unsplit: %s", bytesToHash(leaf.Unsplit))
	log.Error("  Docs: %d", len(leaf.Documents))
	log.Error("  Children: %d", len(leaf.Children))

	return leaf
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
	//log.Trace("fm: %s", fm)
	hash := idx.GetIndexHash(fm)
	leaf := idx.Tree.GetLeaf(hash, 0)
	leaf = leaf.AddDocument(doc, hash)
	idx.Count += 1
}

func (leaf *Leaf) GetLeaf(value []byte, offset int) *Leaf {	
	if len(value) <= offset || len(leaf.Children) == 0 {
		return leaf
	}

	leaf.Lock.Lock()
	if l, ok := leaf.Children[value[offset]]; ok {
		leaf.Lock.Unlock()
		if len(value) == offset + 1 {
			return leaf.Children[value[offset]]
		}
		return l.GetLeaf(value, offset + 1)
	}

	leaf.Children[value[offset]] = leaf.Index.NewLeaf(value[:offset+1])
	leaf.Lock.Unlock()

	if len(value) == offset + 1 {
		return leaf.Children[value[offset]]
	}
	return leaf.Children[value[offset]].GetLeaf(value, offset + 1)
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
