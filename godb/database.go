package godb

import(
	"sync"
	"github.com/ian-kent/go-log/log"
)

type Database struct {
	Documents []*Document
	DBLock    *sync.Mutex
	WriteLock *sync.Mutex
	Indexes   map[string]*Index
}

func NewDatabase() Database {
	return Database{
		Documents: make([]*Document, 0),
		Indexes: make(map[string]*Index, 0),
		DBLock: new(sync.Mutex),
		WriteLock: new(sync.Mutex),
	}
}

func (db *Database) NewIndex(fields ...string) error {
	db.DBLock.Lock()
	defer db.DBLock.Unlock()

	idx, err := NewIndex(db, fields...)

	if err != nil {
		return err
	}

	db.Indexes[idx.Name] = idx

	return nil
}

func (db *Database) GetIndex(fields ...string) *Index {
	idx, _ := db.Indexes[makeIndexName(fields...)]
	return idx
}

func (db *Database) Find(query interface{}, start int, limit int) (int, []*Document) {
	fields := GetFields(query)
	log.Trace("Query: %s", fields)

	f := make([]string, 0)
	for fn, _ := range fields {
		f = append(f, fn)
	}
	if len(f) > 0 {
		//log.Trace("Query has %d fields", len(f))
		if idx := db.GetIndex(f...); idx != nil {
			//log.Trace("Using index %s", idx.Name);
			l := idx.FindLeaf(fields)
			if l == nil {
				//log.Trace("Leaf not found")
				return 0, make([]*Document, 0)
			}
			//log.Trace("Leaf: %s", l.GetHash())
			if len(l.Documents) == 0 {
				//log.Trace("Leaf contains no documents")
				return 0, make([]*Document, 0)	
			}
			mc := len(l.Documents)
			if start > mc - 1 {
				//log.Trace("Leaf contains documents, but start index too high")
				return mc, make([]*Document, 0)
			}
			if start <= mc - 1 && start + limit < mc - 1 {
				//log.Trace("Leaf contains enough documents to satisfy range: %d:%d", start, start + limit - 1)
				return mc, l.Documents[start:start+limit-1]
			}
			//log.Trace("Leaf has subset of documents in range")
			return mc, l.Documents[start:]
		} else {
			//log.Trace("Index not found")
		}
	}// else {
		//log.Trace("No fields in query")
	//}

	log.Trace("Scanning full database")

	results := make([]*Document, 0)

	mc := 0
	for _, d := range db.Documents {
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

func (db *Database) Insert(obj ...interface{}) (int, []*Document) {
	db.WriteLock.Lock()
	defer db.WriteLock.Unlock()

	sindex := len(db.Documents)
	db.Documents = append(db.Documents, make([]*Document, len(obj), len(obj))...)
	eindex := len(db.Documents)

	for i, o := range obj {
		doc := Marshal(o)
		db.Documents[sindex + i] = doc
		for _, idx := range db.Indexes {
			idx.Index(doc)
		}
	}

	return eindex - sindex, db.Documents[sindex:eindex]
}
