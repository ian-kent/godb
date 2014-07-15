package main

import(
	"github.com/ian-kent/godb/godb"
	"github.com/ian-kent/go-log/log"
	"strconv"
	"time"
	"sync"
	"runtime/pprof"
	"flag"
	"os"
	"math/rand"
)

type MyDoc struct {
	Name string
	Age int
}

var db godb.Database

func main() {
	profile := flag.String("profile", "", "profile application")
	flag.Parse()

	if *profile != "" {
		f, err := os.Create(*profile)
		if err != nil {
			log.Error("Error creating file: %s", err)
			return
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	db = godb.NewDatabase()

	//timeIt(indexStuff, "Indexed %d docs in %s")
	timeIt(insertStuff, "Inserted %d docs in %s")
	timeIt(indexName, "Indexed name field for %d docs in %s")
	timeIt(indexAge, "Indexed age field for %d docs in %s")
	timeIt(indexBoth, "Indexed name and age fields for %d docs in %s")
	//log.Info("Index now contains %d docs", db.Indexes[0].Count)
	timeIt(findByQuery, "Found %d docs on name field in %s")
	timeIt(findByQuery2, "Found %d docs on age field in %s")
	timeIt(findByQuery3, "Found %d docs on both fields in %s")
}

func timeIt(f func() int, msg string) {
	log.Info("=================================================")
	start := time.Now()
	i := f()
	end := time.Now()
	log.Info(msg, i, end.Sub(start).String())
	log.Info("")
}

func indexBoth() int {
	db.NewIndex("Name", "Age")
	return db.GetIndex("Name", "Age").Count
}

func indexName() int {
	db.NewIndex("Name")
	return db.GetIndex("Name").Count
}

func indexAge() int {
	db.NewIndex("Age")
	return db.GetIndex("Age").Count
}

func insertStuff() int {
	ins := 0
	batchSize := 1000
	//total := 10000000
	total := 1000000
	rand.Seed(time.Now().Unix())
	var wg sync.WaitGroup
	for i := 0; i < total / batchSize; i++ {
		wg.Add(1)
		go func(i int) {
			//log.Info("Starting insert %d", i)
			defer wg.Done()
			batch := make([]interface{}, batchSize)			
			for j := 0; j < batchSize; j++ {
				age := rand.Intn(60-20) + 20
				bn := i * batchSize + j
				if bn % 1000 == 0 {
					age = 50
				}
				batch[j] = &MyDoc{
					Name:"Test document " + strconv.Itoa(bn),
					Age: age,
				}
			}
			//log.Info("Created 1000 objects for insert %d", i)
			n, _ := db.Insert(batch...)
			//log.Info("Inserted %d complete", i)
			ins += n
		}(i)
	}
	wg.Wait()
	return ins
}

func findByQuery() int {
	// Find a doc using a query
	n, docs := db.Find(&struct{Name string}{Name:"Test document 123"}, 0, 10)

	if len(docs) > 0 {
		var o MyDoc
		docs[0].Unmarshal(&o)
		log.Debug("Name: %s", o.Name)
	}

	return n
}

func findByQuery2() int {
	// Find a doc using a query
	n, docs := db.Find(&struct{Age int}{Age: 45}, 0, 10)

	if len(docs) > 0 {
		var o MyDoc
		docs[0].Unmarshal(&o)
		log.Debug("Name: %s", o.Name)
	}

	return n
}

func findByQuery3() int {
	// Find a doc using a query
	n, docs := db.Find(&struct{
		Name string
		Age int
	}{
		Name:"Test document 3000",
		Age: 50,
	}, 0, 10)

	if len(docs) > 0 {
		var o MyDoc
		docs[0].Unmarshal(&o)
		log.Debug("Name: %s", o.Name)
	}

	return n
}
