// GENERATED CODE, DO NOT EDIT
package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/adler32"
	"io"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/golang/snappy"
	"github.com/kshedden/datareader"
	"github.com/kshedden/gosascols/config"
)

var (
	// Needed to avoid errrors if no other references are made to these packages.
	_ = strings.TrimSpace
	_ = strconv.Atoi

	conf *config.Config

	rslt_chan chan *rec

	dtypes = `{"Dobyr":"uint16","Egeoloc":"uint8","Emprel":"uint8","Enrolid":"uint64","MSA":"uint32","MSwgtkey":"uint8","Memdays":"uint16","Memdays1":"uint16","Memdays10":"uint16","Memdays11":"uint16","Memdays12":"uint16","Memdays2":"uint16","Memdays3":"uint16","Memdays4":"uint16","Memdays5":"uint16","Memdays6":"uint16","Memdays7":"uint16","Memdays8":"uint16","Memdays9":"uint16","Mhsacovg":"uint8","Region":"uint8","Rx":"uint8","Seqnum":"uint64","Sex":"uint8","Wgtkey":"uint8","Year":"uint16"}
`

	wg  sync.WaitGroup
	hwg sync.WaitGroup

	sem chan bool

	buckets []*Bucket

	logger *log.Logger
)

func setupLogger() {

	fn := "sastocols_" + path.Base(conf.TargetDir) + ".log"
	fid, err := os.Create(fn)
	if err != nil {
		panic(err)
	}

	logger = log.New(fid, "", log.Ltime)
}

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

// writeconfig writes the configuration information for the gocols dataset.  This
// configuration information is intended for users of the target dataset so does
// not need to contain information about how the data were derived from the source
// SAS files.
func writeconfig() {

	type Config struct {
		NumBuckets  uint32
		Compression string
		CodesDir    string
	}

	c := Config{NumBuckets: conf.NumBuckets, Compression: "snappy", CodesDir: conf.CodesDir}

	fid, err := os.Create(path.Join(conf.TargetDir, "conf.json"))
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	enc := json.NewEncoder(fid)
	err = enc.Encode(c)
	if err != nil {
		panic(err)
	}
}

func setup() {

	rslt_chan = make(chan *rec)
	sem = make(chan bool, conf.Concurrency)

	buckets = make([]*Bucket, conf.NumBuckets)
	for i, _ := range buckets {
		buckets[i] = new(Bucket)
		buckets[i].BucketNum = uint32(i)
		buckets[i].Conf = conf
	}

	err := os.MkdirAll(conf.TargetDir, 0755)
	if err != nil {
		panic(err)
	}

	writeconfig()

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

	logger.Printf("All done")
}

// rec is a row that will be added to a Bucket.
type rec struct {
	Dobyr     uint16
	Egeoloc   uint8
	Emprel    uint8
	Enrolid   uint64
	Memdays   uint16
	Memdays1  uint16
	Memdays2  uint16
	Memdays3  uint16
	Memdays4  uint16
	Memdays5  uint16
	Memdays6  uint16
	Memdays7  uint16
	Memdays8  uint16
	Memdays9  uint16
	Memdays10 uint16
	Memdays11 uint16
	Memdays12 uint16
	Mhsacovg  uint8
	MSA       uint32
	MSwgtkey  uint8
	Region    uint8
	Rx        uint8
	Seqnum    uint64
	Sex       uint8
	Wgtkey    uint8
	Year      uint16
}

// Bucket is a memory-backed container for columnized data.  It
// contains data exactly as it will be written to disk.
type Bucket struct {
	BaseBucket

	code      []uint16
	Dobyr     []uint16
	Egeoloc   []uint8
	Emprel    []uint8
	Enrolid   []uint64
	Memdays   []uint16
	Memdays1  []uint16
	Memdays2  []uint16
	Memdays3  []uint16
	Memdays4  []uint16
	Memdays5  []uint16
	Memdays6  []uint16
	Memdays7  []uint16
	Memdays8  []uint16
	Memdays9  []uint16
	Memdays10 []uint16
	Memdays11 []uint16
	Memdays12 []uint16
	Mhsacovg  []uint8
	MSA       []uint32
	MSwgtkey  []uint8
	Region    []uint8
	Rx        []uint8
	Seqnum    []uint64
	Sex       []uint8
	Wgtkey    []uint8
	Year      []uint16
}

// chunk is a typed container for data pulled directly out of a SAS file.
// There are no type conversions or other modifications from the SAS file.
type chunk struct {
	row        int
	col        int
	Dobyr      []float64
	Dobyrm     []bool
	Egeoloc    []string
	Egeolocm   []bool
	Emprel     []string
	Emprelm    []bool
	Enrolid    []float64
	Enrolidm   []bool
	Memdays    []float64
	Memdaysm   []bool
	Memdays1   []float64
	Memdays1m  []bool
	Memdays2   []float64
	Memdays2m  []bool
	Memdays3   []float64
	Memdays3m  []bool
	Memdays4   []float64
	Memdays4m  []bool
	Memdays5   []float64
	Memdays5m  []bool
	Memdays6   []float64
	Memdays6m  []bool
	Memdays7   []float64
	Memdays7m  []bool
	Memdays8   []float64
	Memdays8m  []bool
	Memdays9   []float64
	Memdays9m  []bool
	Memdays10  []float64
	Memdays10m []bool
	Memdays11  []float64
	Memdays11m []bool
	Memdays12  []float64
	Memdays12m []bool
	Mhsacovg   []string
	Mhsacovgm  []bool
	MSA        []float64
	MSAm       []bool
	MSwgtkey   []string
	MSwgtkeym  []bool
	Region     []string
	Regionm    []bool
	Rx         []string
	Rxm        []bool
	Seqnum     []float64
	Seqnumm    []bool
	Sex        []string
	Sexm       []bool
	Wgtkey     []float64
	Wgtkeym    []bool
	Year       []float64
	Yearm      []bool
}

// Add appends a rec to the end of the Bucket.
func (bucket *Bucket) Add(r *rec) {

	bucket.Mut.Lock()

	bucket.Dobyr = append(bucket.Dobyr, r.Dobyr)
	bucket.Egeoloc = append(bucket.Egeoloc, r.Egeoloc)
	bucket.Emprel = append(bucket.Emprel, r.Emprel)
	bucket.Enrolid = append(bucket.Enrolid, r.Enrolid)
	bucket.Memdays = append(bucket.Memdays, r.Memdays)
	bucket.Memdays1 = append(bucket.Memdays1, r.Memdays1)
	bucket.Memdays2 = append(bucket.Memdays2, r.Memdays2)
	bucket.Memdays3 = append(bucket.Memdays3, r.Memdays3)
	bucket.Memdays4 = append(bucket.Memdays4, r.Memdays4)
	bucket.Memdays5 = append(bucket.Memdays5, r.Memdays5)
	bucket.Memdays6 = append(bucket.Memdays6, r.Memdays6)
	bucket.Memdays7 = append(bucket.Memdays7, r.Memdays7)
	bucket.Memdays8 = append(bucket.Memdays8, r.Memdays8)
	bucket.Memdays9 = append(bucket.Memdays9, r.Memdays9)
	bucket.Memdays10 = append(bucket.Memdays10, r.Memdays10)
	bucket.Memdays11 = append(bucket.Memdays11, r.Memdays11)
	bucket.Memdays12 = append(bucket.Memdays12, r.Memdays12)
	bucket.Mhsacovg = append(bucket.Mhsacovg, r.Mhsacovg)
	bucket.MSA = append(bucket.MSA, r.MSA)
	bucket.MSwgtkey = append(bucket.MSwgtkey, r.MSwgtkey)
	bucket.Region = append(bucket.Region, r.Region)
	bucket.Rx = append(bucket.Rx, r.Rx)
	bucket.Seqnum = append(bucket.Seqnum, r.Seqnum)
	bucket.Sex = append(bucket.Sex, r.Sex)
	bucket.Wgtkey = append(bucket.Wgtkey, r.Wgtkey)
	bucket.Year = append(bucket.Year, r.Year)

	bucket.Mut.Unlock()

	if uint64(len(bucket.Enrolid)) > conf.BufMaxRecs {
		bucket.Flush()
	}
}

// Flush writes all the data from the Bucket to disk.
func (bucket *Bucket) Flush() {

	logger.Printf("Flushing bucket %d", bucket.BucketNum)

	bucket.Mut.Lock()

	bucket.flushuint16("Dobyr", bucket.Dobyr)
	bucket.Dobyr = bucket.Dobyr[0:0]
	bucket.flushuint8("Egeoloc", bucket.Egeoloc)
	bucket.Egeoloc = bucket.Egeoloc[0:0]
	bucket.flushuint8("Emprel", bucket.Emprel)
	bucket.Emprel = bucket.Emprel[0:0]
	bucket.flushuint64("Enrolid", bucket.Enrolid)
	bucket.Enrolid = bucket.Enrolid[0:0]
	bucket.flushuint16("Memdays", bucket.Memdays)
	bucket.Memdays = bucket.Memdays[0:0]
	bucket.flushuint16("Memdays1", bucket.Memdays1)
	bucket.Memdays1 = bucket.Memdays1[0:0]
	bucket.flushuint16("Memdays2", bucket.Memdays2)
	bucket.Memdays2 = bucket.Memdays2[0:0]
	bucket.flushuint16("Memdays3", bucket.Memdays3)
	bucket.Memdays3 = bucket.Memdays3[0:0]
	bucket.flushuint16("Memdays4", bucket.Memdays4)
	bucket.Memdays4 = bucket.Memdays4[0:0]
	bucket.flushuint16("Memdays5", bucket.Memdays5)
	bucket.Memdays5 = bucket.Memdays5[0:0]
	bucket.flushuint16("Memdays6", bucket.Memdays6)
	bucket.Memdays6 = bucket.Memdays6[0:0]
	bucket.flushuint16("Memdays7", bucket.Memdays7)
	bucket.Memdays7 = bucket.Memdays7[0:0]
	bucket.flushuint16("Memdays8", bucket.Memdays8)
	bucket.Memdays8 = bucket.Memdays8[0:0]
	bucket.flushuint16("Memdays9", bucket.Memdays9)
	bucket.Memdays9 = bucket.Memdays9[0:0]
	bucket.flushuint16("Memdays10", bucket.Memdays10)
	bucket.Memdays10 = bucket.Memdays10[0:0]
	bucket.flushuint16("Memdays11", bucket.Memdays11)
	bucket.Memdays11 = bucket.Memdays11[0:0]
	bucket.flushuint16("Memdays12", bucket.Memdays12)
	bucket.Memdays12 = bucket.Memdays12[0:0]
	bucket.flushuint8("Mhsacovg", bucket.Mhsacovg)
	bucket.Mhsacovg = bucket.Mhsacovg[0:0]
	bucket.flushuint32("MSA", bucket.MSA)
	bucket.MSA = bucket.MSA[0:0]
	bucket.flushuint8("MSwgtkey", bucket.MSwgtkey)
	bucket.MSwgtkey = bucket.MSwgtkey[0:0]
	bucket.flushuint8("Region", bucket.Region)
	bucket.Region = bucket.Region[0:0]
	bucket.flushuint8("Rx", bucket.Rx)
	bucket.Rx = bucket.Rx[0:0]
	bucket.flushuint64("Seqnum", bucket.Seqnum)
	bucket.Seqnum = bucket.Seqnum[0:0]
	bucket.flushuint8("Sex", bucket.Sex)
	bucket.Sex = bucket.Sex[0:0]
	bucket.flushuint8("Wgtkey", bucket.Wgtkey)
	bucket.Wgtkey = bucket.Wgtkey[0:0]
	bucket.flushuint16("Year", bucket.Year)
	bucket.Year = bucket.Year[0:0]

	bucket.Mut.Unlock()
}

// getcols fills a chunk with data from a SAS file.
func (c *chunk) getcols(data []*datareader.Series, cm map[string]int) error {

	var err error
	var ii int
	var ok bool

	ii, ok = cm["DOBYR"]
	if ok {
		c.Dobyr, c.Dobyrm, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable DOBYR required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["EGEOLOC"]
	if ok {
		c.Egeoloc, c.Egeolocm, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable EGEOLOC required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["EMPREL"]
	if ok {
		c.Emprel, c.Emprelm, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable EMPREL required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["ENROLID"]
	if ok {
		c.Enrolid, c.Enrolidm, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable ENROLID required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["MEMDAYS"]
	if ok {
		c.Memdays, c.Memdaysm, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable MEMDAYS required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["MEMDAYS1"]
	if ok {
		c.Memdays1, c.Memdays1m, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable MEMDAYS1 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["MEMDAYS2"]
	if ok {
		c.Memdays2, c.Memdays2m, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable MEMDAYS2 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["MEMDAYS3"]
	if ok {
		c.Memdays3, c.Memdays3m, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable MEMDAYS3 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["MEMDAYS4"]
	if ok {
		c.Memdays4, c.Memdays4m, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable MEMDAYS4 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["MEMDAYS5"]
	if ok {
		c.Memdays5, c.Memdays5m, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable MEMDAYS5 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["MEMDAYS6"]
	if ok {
		c.Memdays6, c.Memdays6m, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable MEMDAYS6 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["MEMDAYS7"]
	if ok {
		c.Memdays7, c.Memdays7m, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable MEMDAYS7 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["MEMDAYS8"]
	if ok {
		c.Memdays8, c.Memdays8m, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable MEMDAYS8 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["MEMDAYS9"]
	if ok {
		c.Memdays9, c.Memdays9m, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable MEMDAYS9 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["MEMDAYS10"]
	if ok {
		c.Memdays10, c.Memdays10m, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable MEMDAYS10 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["MEMDAYS11"]
	if ok {
		c.Memdays11, c.Memdays11m, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable MEMDAYS11 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["MEMDAYS12"]
	if ok {
		c.Memdays12, c.Memdays12m, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable MEMDAYS12 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["MHSACOVG"]
	if ok {
		c.Mhsacovg, c.Mhsacovgm, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable MHSACOVG required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["MSA"]
	if ok {
		c.MSA, c.MSAm, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable MSA required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["MSWGTKEY"]
	if ok {
		c.MSwgtkey, c.MSwgtkeym, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	}

	ii, ok = cm["REGION"]
	if ok {
		c.Region, c.Regionm, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable REGION required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["RX"]
	if ok {
		c.Rx, c.Rxm, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable RX required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["SEQNUM"]
	if ok {
		c.Seqnum, c.Seqnumm, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable SEQNUM required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["SEX"]
	if ok {
		c.Sex, c.Sexm, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable SEX required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["WGTKEY"]
	if ok {
		c.Wgtkey, c.Wgtkeym, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	}

	ii, ok = cm["YEAR"]
	if ok {
		c.Year, c.Yearm, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable YEAR required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	return nil
}

func (c *chunk) trynextrec() (*rec, bool) {

	if c.row >= len(c.Enrolid) {
		return nil, false
	}

	r := new(rec)

	i := c.row

	if c.Enrolidm[i] {
		c.row++
		return nil, true
	}

	r.Dobyr = uint16(c.Dobyr[i])

	// Convert string to number
	if len(c.Egeoloc[i]) > 0 {
		x, err := strconv.Atoi(c.Egeoloc[i])
		if err == nil {
			r.Egeoloc = uint8(x)
		}
	}

	// Convert string to number
	if len(c.Emprel[i]) > 0 {
		x, err := strconv.Atoi(c.Emprel[i])
		if err == nil {
			r.Emprel = uint8(x)
		}
	}

	r.Enrolid = uint64(c.Enrolid[i])

	r.Memdays = uint16(c.Memdays[i])

	r.Memdays1 = uint16(c.Memdays1[i])

	r.Memdays2 = uint16(c.Memdays2[i])

	r.Memdays3 = uint16(c.Memdays3[i])

	r.Memdays4 = uint16(c.Memdays4[i])

	r.Memdays5 = uint16(c.Memdays5[i])

	r.Memdays6 = uint16(c.Memdays6[i])

	r.Memdays7 = uint16(c.Memdays7[i])

	r.Memdays8 = uint16(c.Memdays8[i])

	r.Memdays9 = uint16(c.Memdays9[i])

	r.Memdays10 = uint16(c.Memdays10[i])

	r.Memdays11 = uint16(c.Memdays11[i])

	r.Memdays12 = uint16(c.Memdays12[i])

	// Convert string to number
	if len(c.Mhsacovg[i]) > 0 {
		x, err := strconv.Atoi(c.Mhsacovg[i])
		if err == nil {
			r.Mhsacovg = uint8(x)
		}
	}

	r.MSA = uint32(c.MSA[i])

	if c.MSwgtkey != nil {

		// Convert string to number
		if len(c.MSwgtkey[i]) > 0 {
			x, err := strconv.Atoi(c.MSwgtkey[i])
			if err == nil {
				r.MSwgtkey = uint8(x)
			}
		}

	}

	// Convert string to number
	if len(c.Region[i]) > 0 {
		x, err := strconv.Atoi(c.Region[i])
		if err == nil {
			r.Region = uint8(x)
		}
	}

	// Convert string to number
	if len(c.Rx[i]) > 0 {
		x, err := strconv.Atoi(c.Rx[i])
		if err == nil {
			r.Rx = uint8(x)
		}
	}

	r.Seqnum = uint64(c.Seqnum[i])

	// Convert string to number
	if len(c.Sex[i]) > 0 {
		x, err := strconv.Atoi(c.Sex[i])
		if err == nil {
			r.Sex = uint8(x)
		}
	}

	if c.Wgtkey != nil {

		r.Wgtkey = uint8(c.Wgtkey[i])

	}

	r.Year = uint16(c.Year[i])

	c.row++

	return r, true
}

type BaseBucket struct {

	// The number of the bucket, corresponds to the file name in
	// the Buckets directory.
	BucketNum uint32

	// Locks for accessing the bucket's data.
	Mut sync.Mutex

	Conf *config.Config
}

// openfile opens a file for appending data in the bucket's directory.
func (bucket *BaseBucket) openfile(varname string) (io.Closer, io.WriteCloser) {

	bp := config.BucketPath(int(bucket.BucketNum), bucket.Conf)
	fn := path.Join(bp, varname+".bin.sz")
	fid, err := os.OpenFile(fn, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}

	gid := snappy.NewBufferedWriter(fid)

	return fid, gid
}

func (bucket *BaseBucket) flushstring(varname string, vec []string) {

	toclose, wtr := bucket.openfile(varname)

	nl := []byte("\n")
	for _, x := range vec {
		_, err := wtr.Write([]byte(x))
		if err != nil {
			panic(err)
		}
		_, err = wtr.Write(nl)
		if err != nil {
			panic(err)
		}
	}

	err := wtr.Close()
	if err != nil {
		panic(err)
	}
	err = toclose.Close()
	if err != nil {
		panic(err)
	}
}
func (bucket *BaseBucket) flushuint8(varname string, vec []uint8) {

	toclose, wtr := bucket.openfile(varname)

	for _, x := range vec {
		err := binary.Write(wtr, binary.LittleEndian, x)
		if err != nil {
			panic(err)
		}
	}

	err := wtr.Close()
	if err != nil {
		panic(err)
	}
	err = toclose.Close()
	if err != nil {
		panic(err)
	}
}
func (bucket *BaseBucket) flushuint16(varname string, vec []uint16) {

	toclose, wtr := bucket.openfile(varname)

	for _, x := range vec {
		err := binary.Write(wtr, binary.LittleEndian, x)
		if err != nil {
			panic(err)
		}
	}

	err := wtr.Close()
	if err != nil {
		panic(err)
	}
	err = toclose.Close()
	if err != nil {
		panic(err)
	}
}
func (bucket *BaseBucket) flushuint32(varname string, vec []uint32) {

	toclose, wtr := bucket.openfile(varname)

	for _, x := range vec {
		err := binary.Write(wtr, binary.LittleEndian, x)
		if err != nil {
			panic(err)
		}
	}

	err := wtr.Close()
	if err != nil {
		panic(err)
	}
	err = toclose.Close()
	if err != nil {
		panic(err)
	}
}
func (bucket *BaseBucket) flushuint64(varname string, vec []uint64) {

	toclose, wtr := bucket.openfile(varname)

	for _, x := range vec {
		err := binary.Write(wtr, binary.LittleEndian, x)
		if err != nil {
			panic(err)
		}
	}

	err := wtr.Close()
	if err != nil {
		panic(err)
	}
	err = toclose.Close()
	if err != nil {
		panic(err)
	}
}
func (bucket *BaseBucket) flushfloat32(varname string, vec []float32) {

	toclose, wtr := bucket.openfile(varname)

	for _, x := range vec {
		err := binary.Write(wtr, binary.LittleEndian, x)
		if err != nil {
			panic(err)
		}
	}

	err := wtr.Close()
	if err != nil {
		panic(err)
	}
	err = toclose.Close()
	if err != nil {
		panic(err)
	}
}
func (bucket *BaseBucket) flushfloat64(varname string, vec []float64) {

	toclose, wtr := bucket.openfile(varname)

	for _, x := range vec {
		err := binary.Write(wtr, binary.LittleEndian, x)
		if err != nil {
			panic(err)
		}
	}

	err := wtr.Close()
	if err != nil {
		panic(err)
	}
	err = toclose.Close()
	if err != nil {
		panic(err)
	}
}

func main() {

	if len(os.Args) != 2 {
		os.Stderr.WriteString("sastocols: Wrong number of arguments\n\n")
		msg := fmt.Sprintf("Usage: %s config.toml\n\n", os.Args[0])
		os.Stderr.WriteString(msg)
		os.Exit(1)
	}

	conf = config.ReadConfig(os.Args[1])
	setupLogger()
	logger.Printf("Read config from %s", os.Args[1])

	Run(conf, logger)

	logger.Printf("Finished, exiting")
}
