/*
Package sastocols buckets and columnizes a collection of SAS datasets.

The records from the datasets are placed into buckets based on the
value of a bucketing variable (currently must be "ENROLID").  All
records with a given value of the bucketing variable are guaranteed to
be placed into the same bucket.

Within each bucket, the variables are stored as gzip-compressed
columns, in various native data formats.  A json file describing the
columns is placed into each bucket directory.

To make this efficient, code generation is used to compile a template
into a go program based on provided variable information.  See
odefs.json for an example variable description file.

TODO: Bucketing variable (enrolid) is not configurable.
*/

package sastocols

import (
	"encoding/binary"
	"fmt"
	"hash/adler32"
	"log"
	"os"
	"path"
	"sync"

	"github.com/kshedden/datareader"
	"github.com/kshedden/goclaims/config"
)

//go:generate go run gen.go ../defs/odefs.json

const (
	// Maximum number of simultaneous goroutines.
	concurrent = 100
)

var (
	conf *config.Config

	rslt_chan chan *rec

	wg  sync.WaitGroup
	hwg sync.WaitGroup

	logger *log.Logger

	sem chan bool

	buckets []*Bucket
)

// harvest retrieves data from the producers in the form of data
// records, and adds each record to the appropriate bucket.
func harvest() {

	ha := adler32.New()

	buf := make([]byte, 8)

	for r := range rslt_chan {

		binary.LittleEndian.PutUint64(buf, r.Enrolid)
		ha.Reset()
		_, err := ha.Write(buf)
		if err != nil {
			panic(err)
		}

		bucket := int(ha.Sum32() % conf.NumBuckets)
		buckets[bucket].Add(r)
	}

	hwg.Done()
}

// sendrecs drains a chunk, sending each record in the chunk to be
// harvested.
func sendrecs(c *chunk) {

	defer func() { <-sem; wg.Done() }()

	for {
		r := c.nextrec()
		if r == nil {
			break
		}
		rslt_chan <- r
	}
}

// dofile processes one SAS file.
func dofile(filename string) {

	defer func() { wg.Done() }()

	logger.Printf("Starting file %s", filename)

	fid, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer fid.Close()

	sas, err := datareader.NewSAS7BDATReader(fid)
	if err != nil {
		panic(err)
	}
	sas.TrimStrings = true

	logger.Printf("%s has %d rows", filename, sas.RowCount())

	cm := make(map[string]int)
	for k, na := range sas.ColumnNames() {
		cm[na] = k
	}

	for chunk_id := 0; ; chunk_id++ {

		logger.Printf("Starting chunk %d", chunk_id)
		if conf.MaxChunk > 0 && chunk_id > int(conf.MaxChunk) {
			logger.Printf("Read %d blocks from %s, breaking early", chunk_id, filename)
			break
		}

		chunk := new(chunk)

		data, err := sas.Read(int(conf.SASChunkSize))
		if data == nil {
			break
		}
		if err != nil {
			panic(err)
		}

		err = chunk.getcols(data, cm)
		if err != nil {
			print("In file ", filename, "\n\n")
			panic(err)
		}

		wg.Add(1)
		sem <- true
		go sendrecs(chunk)
	}
}

// nextrec finds the next valid record from the chunk and returns it.
// recs with missing enrolid values are skipped, so this may not
// return a value for every row of the SAS file chunk.  Returns nil
// when the chunk is fully processed.
func (c *chunk) nextrec() *rec {

	for {
		rec, cont := c.trynextrec()
		if rec != nil {
			return rec
		}
		if !cont {
			break
		}
	}

	return nil
}

func setup() {

	rslt_chan = make(chan *rec)
	sem = make(chan bool, concurrent)

	buckets = make([]*Bucket, conf.NumBuckets)
	for i, _ := range buckets {
		buckets[i] = new(Bucket)
		buckets[i].BucketNum = uint32(i)
	}

	err := os.MkdirAll(conf.TargetDir, 0755)
	if err != nil {
		panic(err)
	}

	fn := path.Join(conf.TargetDir, "Buckets")
	err = os.RemoveAll(fn)
	if err != nil {
		panic(err)
	}

	pa := path.Join(conf.TargetDir, "Buckets")
	os.MkdirAll(pa, 0755)
	for k := 0; k < int(conf.NumBuckets); k++ {
		bns := fmt.Sprintf("%04d", k)
		dn := path.Join(conf.TargetDir, "Buckets", bns)
		err = os.MkdirAll(dn, 0755)
		if err != nil {
			panic(err)
		}

		fn := path.Join(dn, "dtypes.json")
		fid, err := os.Create(fn)
		if err != nil {
			panic(err)
		}
		_, err = fid.Write([]byte(dtypes))
		if err != nil {
			panic(err)
		}
	}
}

func Run(cnf *config.Config, lgr *log.Logger) {

	logger = lgr
	conf = cnf

	setup()

	hwg.Add(1)
	go harvest()

	for _, fn := range conf.SASFiles {
		fn = path.Join(conf.SourceDir, fn)
		wg.Add(1)
		go dofile(fn)
	}

	wg.Wait()
	close(rslt_chan)
	hwg.Wait()

	for k := 0; k < int(conf.NumBuckets); k++ {
		buckets[k].Flush()
	}
}
