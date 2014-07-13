package godb

import(
	"sync"
	"github.com/ian-kent/go-log/log"
	"crypto/rand"
	"reflect"
	"encoding/binary"
)

type Database struct {
	Documents []*Document
	DBLock    *sync.Mutex
	Indexes   []*Index
}

type Index struct {
	Field string
	Tree map[byte]*Leaf
	Count int
	Documents []*Document
}

type Leaf struct {
	Index *Index
	LeafValue []byte
	Lock *sync.Mutex
	Children map[byte]*Leaf
	Documents []*Document
}

func (db *Database) GetIndex(field string) *Index {
	for _, idx := range db.Indexes {
		if idx.Field == field {
			return idx
		}
	}
	return nil
}

func (idx *Index) GetLeaf(value []byte) *Leaf {
	if len(value) == 0 {
		return nil
	}
	if leaf, ok := idx.Tree[value[0]]; ok {
		return leaf.GetLeaf(value[1:])
	}
	idx.Tree[value[0]] = idx.NewLeaf([]byte{value[0]})
	return idx.Tree[value[0]]
}

func (leaf *Leaf) AddDocument(doc *Document) {
	leaf.Lock.Lock()

	//if len(leaf.Documents) > 1000 {
	//	// split leaf
	//	log.Info("Splitting leaf")
	//	docs := leaf.Documents
	//	leaf.Documents = make([]*Document, 0)
	//	for _, d := range docs {
	//		f := d.Fields[leaf.Index.Field]
	//		switch f.(type) {
	//		case string:
	//			leaf.GetLeaf([]byte(f.(string))[len(leaf.LeafValue):]).AddDocument(d)
	//		case int:
	//			b := make([]byte, 8)
	//			binary.PutVarint(b, int64(f.(int)))
	//			leaf.GetLeaf(b[len(leaf.LeafValue):]).AddDocument(d)
	//		}
	//	}
	//} else {
		//log.Info("Adding document to leaf")
		leaf.Documents = append(leaf.Documents, doc)
	//}

	leaf.Lock.Unlock()
}

func (idx *Index) Index(doc *Document, value []byte) {
	leaf := idx.GetLeaf(value)
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
	return leaf.Children[value[0]]
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

func (db *Database) NewIndex(field ...string) []*Index {
	idxs := make(map[string]*Index)
	for _, f := range field {
		idxs[f] = &Index{
			Field: f,
			Tree: make(map[byte]*Leaf),
			Count: 0,
			Documents: make([]*Document, 0),
		}
	}

	db.DBLock.Lock()
	for _, doc := range db.Documents {
		for _, idx := range idxs {
			if _, ok := doc.Fields[idx.Field]; ok {
				f := doc.Fields[idx.Field]
				switch f.(type) {
				case string:
					idx.Index(doc, []byte(f.(string)))
				case int:
					b := make([]byte, 8)
					binary.PutVarint(b, int64(f.(int)))
					idx.Index(doc, b)
				}
			}
		}
	}
	idxsa := make([]*Index, 0)
	for _, idx := range idxs {
		db.Indexes = append(db.Indexes, idx)
		idxsa = append(idxsa, idx)
		log.Info("Index on %s contains %d documents", idx.Field, idx.Count)
	}
	db.DBLock.Unlock()

	return idxsa
}

type ObjectID []byte

func NewObjectID() []byte {
	uuid := make([]byte, 16)
 	rand.Read(uuid)
 	return uuid
}

type Document struct {
	ObjectID ObjectID
	Fields map[string]interface{}
}

var FieldCache = make(map[string][]reflect.StructField)

func GetFields(value interface{}) map[string]interface{} {
	fnm := GetFieldNames(value)
	fields := make(map[string]interface{}, 0)
	vl := reflect.ValueOf(value).Elem()

	for i, f := range fnm {
		fields[f.Name] = vl.Field(i).Interface()
	}

	return fields
}

func GetFieldNames(value interface{}) []reflect.StructField {
	tp := reflect.TypeOf(value).Elem()
	tn := tp.Name()

	if fields, ok := FieldCache[tn]; tn != "" && ok {
		return fields
	}

	fields := make([]reflect.StructField, tp.NumField())
	for i := 0; i < tp.NumField(); i++ {
		fields[i] = tp.Field(i)
	}
	FieldCache[tn] = fields

	return fields
}

func Marshal(value interface{}) *Document {
	doc := &Document{
		ObjectID: NewObjectID(),
		Fields: GetFields(value),
	}

	return doc
}

func (d Document) Unmarshal(value interface{}) {
	tp := reflect.TypeOf(value).Elem()
	vl := reflect.ValueOf(value).Elem()

	for i := 0; i < vl.NumField(); i++ {
		nv := reflect.ValueOf(d.Fields[tp.Field(i).Name])

		switch vl.Field(i).Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			vl.Field(i).SetInt(nv.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			vl.Field(i).SetUint(nv.Uint())
		case reflect.Float32, reflect.Float64:
			vl.Field(i).SetFloat(nv.Float())
		case reflect.String:
			vl.Field(i).SetString(nv.String())
		default:
			vl.Field(i).Set(nv)
		}
	}
}

func NewDatabase() Database {
	return Database{
		Documents: make([]*Document, 0),
		DBLock: new(sync.Mutex),
	}
}

func (db *Database) Find(query interface{}, start int, limit int) (int, []*Document) {
	fields := GetFields(query)
	//log.Info("Query: %s", fields)

	docs := db.Documents

	idxs := make(map[string]*Index, 0)
	for fn, _ := range fields {
		idx := db.GetIndex(fn)
		if idx != nil {
			idxs[fn] = idx
		}
	}

	if len(fields) == len(idxs) {
		log.Info("Using indexes");
		dm := make(map[*Document]int)
		for fn, f := range fields {
			idx := idxs[fn]
			switch f.(type) {
			case string:
				for _, d := range idx.GetLeaf([]byte(f.(string))).Documents {
					if _, ok := dm[d]; !ok {
						dm[d] = 1
					} else {
						dm[d] += 1
					}
				}
			case int:
				b := make([]byte, 8)
				binary.PutVarint(b, int64(f.(int)))
				for _, d := range idx.GetLeaf(b).Documents {
					if _, ok := dm[d]; !ok {
						dm[d] = 1
					} else {
						dm[d] += 1
					}
				}
			}
		}
		mc := 0
		docs = make([]*Document, 0)
		for d, c := range dm {
			if c == len(fields) {
				if mc >= start && len(docs) <= limit {
					docs = append(docs, d)
				}
				mc++
			}
		}
		return mc, docs
	}

	results := make([]*Document, 0)

	mc := 0
	for _, d := range docs {
		match := true
		for fn, f := range fields {
			//log.Info("Checking for [%s] in field [%s] with value [%s]", f.Value, f.Name, d.Fields[f.Name].Value)
			if d.Fields[fn] != f {
				match = false
				break;
			}
		}
		if match {
			if mc >= start && len(results) <= limit {
				results = append(results, d)
			}
			mc++
		}
	}
	return mc, results
}

func (db *Database) Index(doc *Document) {
	for _, idx := range db.Indexes {
		if _, ok := doc.Fields[idx.Field]; ok {
			idx.Index(doc, []byte(doc.Fields[idx.Field].(string)))
		}
	}
}

func (db *Database) Insert(obj ...interface{}) (int, []*Document) {
	db.DBLock.Lock()
	sindex := len(db.Documents)
	db.Documents = append(db.Documents, make([]*Document, len(obj), len(obj))...)
	eindex := len(db.Documents)
	db.DBLock.Unlock()

	for i, o := range obj {
		db.Documents[sindex + i] = Marshal(o)
		db.Index(db.Documents[sindex + i])
	}

	return eindex - sindex, db.Documents[sindex:eindex]
}
