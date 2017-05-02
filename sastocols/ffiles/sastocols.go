// GENERATED CODE, DO NOT EDIT
package main

import (
	"encoding/binary"
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
	"github.com/kshedden/goclaims/config"
)

var (
	// Needed to avoid errrors if no other references are made to these packages.
	_ = strings.TrimSpace
	_ = strconv.Atoi

	conf *config.Config

	rslt_chan chan *rec

	dtypes = `{"Copay":"float32","Deduct":"float32","Dx1":"string","Dx2":"string","Dx3":"string","Dx4":"string","Dx5":"string","Dx6":"string","Dx7":"string","Dx8":"string","Dx9":"string","Enrolid":"uint64","Netpay":"float32","Proc1":"string","Proc2":"string","Proc3":"string","Proc4":"string","Proc5":"string","Proc6":"string","Seqnum":"uint64","Svcdate":"uint16"}
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

const (
	// Maximum number of simultaneous goroutines.
	concurrent = 100
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
		buckets[i].Conf = conf
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

	logger.Printf("All done")
}

// rec is a row that will be added to a Bucket.
type rec struct {
	Seqnum  uint64
	Enrolid uint64
	Svcdate uint16
	Copay   float32
	Netpay  float32
	Deduct  float32
	Dx1     string
	Dx2     string
	Dx3     string
	Dx4     string
	Dx5     string
	Dx6     string
	Dx7     string
	Dx8     string
	Dx9     string
	Proc1   string
	Proc2   string
	Proc3   string
	Proc4   string
	Proc5   string
	Proc6   string
}

// Bucket is a memory-backed container for columnized data.  It
// contains data exactly as it will be written to disk.
type Bucket struct {
	BaseBucket

	code    []uint16
	Seqnum  []uint64
	Enrolid []uint64
	Svcdate []uint16
	Copay   []float32
	Netpay  []float32
	Deduct  []float32
	Dx1     []string
	Dx2     []string
	Dx3     []string
	Dx4     []string
	Dx5     []string
	Dx6     []string
	Dx7     []string
	Dx8     []string
	Dx9     []string
	Proc1   []string
	Proc2   []string
	Proc3   []string
	Proc4   []string
	Proc5   []string
	Proc6   []string
}

// chunk is a typed container for data pulled directly out of a SAS file.
// There are no type conversions or other modifications from the SAS file.
type chunk struct {
	row      int
	col      int
	Seqnum   []float64
	Seqnumm  []bool
	Enrolid  []float64
	Enrolidm []bool
	Svcdate  []float64
	Svcdatem []bool
	Copay    []float64
	Copaym   []bool
	Netpay   []float64
	Netpaym  []bool
	Deduct   []float64
	Deductm  []bool
	Dx1      []string
	Dx1m     []bool
	Dx2      []string
	Dx2m     []bool
	Dx3      []string
	Dx3m     []bool
	Dx4      []string
	Dx4m     []bool
	Dx5      []string
	Dx5m     []bool
	Dx6      []string
	Dx6m     []bool
	Dx7      []string
	Dx7m     []bool
	Dx8      []string
	Dx8m     []bool
	Dx9      []string
	Dx9m     []bool
	Proc1    []string
	Proc1m   []bool
	Proc2    []string
	Proc2m   []bool
	Proc3    []string
	Proc3m   []bool
	Proc4    []string
	Proc4m   []bool
	Proc5    []string
	Proc5m   []bool
	Proc6    []string
	Proc6m   []bool
}

// Add appends a rec to the end of the Bucket.
func (bucket *Bucket) Add(r *rec) {

	bucket.Mut.Lock()

	bucket.Seqnum = append(bucket.Seqnum, r.Seqnum)
	bucket.Enrolid = append(bucket.Enrolid, r.Enrolid)
	bucket.Svcdate = append(bucket.Svcdate, r.Svcdate)
	bucket.Copay = append(bucket.Copay, r.Copay)
	bucket.Netpay = append(bucket.Netpay, r.Netpay)
	bucket.Deduct = append(bucket.Deduct, r.Deduct)
	bucket.Dx1 = append(bucket.Dx1, r.Dx1)
	bucket.Dx2 = append(bucket.Dx2, r.Dx2)
	bucket.Dx3 = append(bucket.Dx3, r.Dx3)
	bucket.Dx4 = append(bucket.Dx4, r.Dx4)
	bucket.Dx5 = append(bucket.Dx5, r.Dx5)
	bucket.Dx6 = append(bucket.Dx6, r.Dx6)
	bucket.Dx7 = append(bucket.Dx7, r.Dx7)
	bucket.Dx8 = append(bucket.Dx8, r.Dx8)
	bucket.Dx9 = append(bucket.Dx9, r.Dx9)
	bucket.Proc1 = append(bucket.Proc1, r.Proc1)
	bucket.Proc2 = append(bucket.Proc2, r.Proc2)
	bucket.Proc3 = append(bucket.Proc3, r.Proc3)
	bucket.Proc4 = append(bucket.Proc4, r.Proc4)
	bucket.Proc5 = append(bucket.Proc5, r.Proc5)
	bucket.Proc6 = append(bucket.Proc6, r.Proc6)

	bucket.Mut.Unlock()

	if uint64(len(bucket.Enrolid)) > conf.BufMaxRecs {
		bucket.Flush()
	}
}

// Flush writes all the data from the Bucket to disk.
func (bucket *Bucket) Flush() {

	logger.Printf("Flushing bucket %d", bucket.BucketNum)

	bucket.Mut.Lock()

	bucket.flushuint64("Seqnum", bucket.Seqnum)
	bucket.Seqnum = bucket.Seqnum[0:0]
	bucket.flushuint64("Enrolid", bucket.Enrolid)
	bucket.Enrolid = bucket.Enrolid[0:0]
	bucket.flushuint16("Svcdate", bucket.Svcdate)
	bucket.Svcdate = bucket.Svcdate[0:0]
	bucket.flushfloat32("Copay", bucket.Copay)
	bucket.Copay = bucket.Copay[0:0]
	bucket.flushfloat32("Netpay", bucket.Netpay)
	bucket.Netpay = bucket.Netpay[0:0]
	bucket.flushfloat32("Deduct", bucket.Deduct)
	bucket.Deduct = bucket.Deduct[0:0]
	bucket.flushstring("Dx1", bucket.Dx1)
	bucket.Dx1 = bucket.Dx1[0:0]
	bucket.flushstring("Dx2", bucket.Dx2)
	bucket.Dx2 = bucket.Dx2[0:0]
	bucket.flushstring("Dx3", bucket.Dx3)
	bucket.Dx3 = bucket.Dx3[0:0]
	bucket.flushstring("Dx4", bucket.Dx4)
	bucket.Dx4 = bucket.Dx4[0:0]
	bucket.flushstring("Dx5", bucket.Dx5)
	bucket.Dx5 = bucket.Dx5[0:0]
	bucket.flushstring("Dx6", bucket.Dx6)
	bucket.Dx6 = bucket.Dx6[0:0]
	bucket.flushstring("Dx7", bucket.Dx7)
	bucket.Dx7 = bucket.Dx7[0:0]
	bucket.flushstring("Dx8", bucket.Dx8)
	bucket.Dx8 = bucket.Dx8[0:0]
	bucket.flushstring("Dx9", bucket.Dx9)
	bucket.Dx9 = bucket.Dx9[0:0]
	bucket.flushstring("Proc1", bucket.Proc1)
	bucket.Proc1 = bucket.Proc1[0:0]
	bucket.flushstring("Proc2", bucket.Proc2)
	bucket.Proc2 = bucket.Proc2[0:0]
	bucket.flushstring("Proc3", bucket.Proc3)
	bucket.Proc3 = bucket.Proc3[0:0]
	bucket.flushstring("Proc4", bucket.Proc4)
	bucket.Proc4 = bucket.Proc4[0:0]
	bucket.flushstring("Proc5", bucket.Proc5)
	bucket.Proc5 = bucket.Proc5[0:0]
	bucket.flushstring("Proc6", bucket.Proc6)
	bucket.Proc6 = bucket.Proc6[0:0]

	bucket.Mut.Unlock()
}

// getcols fills a chunk with data from a SAS file.
func (c *chunk) getcols(data []*datareader.Series, cm map[string]int) error {

	var err error
	var ii int
	var ok bool

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

	ii, ok = cm["SVCDATE"]
	if ok {
		c.Svcdate, c.Svcdatem, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable SVCDATE required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["COPAY"]
	if ok {
		c.Copay, c.Copaym, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable COPAY required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["NETPAY"]
	if ok {
		c.Netpay, c.Netpaym, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable NETPAY required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["DEDUCT"]
	if ok {
		c.Deduct, c.Deductm, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable DEDUCT required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["DX1"]
	if ok {
		c.Dx1, c.Dx1m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable DX1 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["DX2"]
	if ok {
		c.Dx2, c.Dx2m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable DX2 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["DX3"]
	if ok {
		c.Dx3, c.Dx3m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable DX3 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["DX4"]
	if ok {
		c.Dx4, c.Dx4m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable DX4 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["DX5"]
	if ok {
		c.Dx5, c.Dx5m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable DX5 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["DX6"]
	if ok {
		c.Dx6, c.Dx6m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable DX6 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["DX7"]
	if ok {
		c.Dx7, c.Dx7m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable DX7 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["DX8"]
	if ok {
		c.Dx8, c.Dx8m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable DX8 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["DX9"]
	if ok {
		c.Dx9, c.Dx9m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable DX9 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["PROC1"]
	if ok {
		c.Proc1, c.Proc1m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable PROC1 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["PROC2"]
	if ok {
		c.Proc2, c.Proc2m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable PROC2 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["PROC3"]
	if ok {
		c.Proc3, c.Proc3m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable PROC3 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["PROC4"]
	if ok {
		c.Proc4, c.Proc4m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable PROC4 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["PROC5"]
	if ok {
		c.Proc5, c.Proc5m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable PROC5 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["PROC6"]
	if ok {
		c.Proc6, c.Proc6m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable PROC6 required but not found in SAS file\n")
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

	r.Seqnum = uint64(c.Seqnum[i])

	r.Enrolid = uint64(c.Enrolid[i])

	r.Svcdate = uint16(c.Svcdate[i])

	r.Copay = float32(c.Copay[i])

	r.Netpay = float32(c.Netpay[i])

	r.Deduct = float32(c.Deduct[i])

	r.Dx1 = strings.TrimSpace(c.Dx1[i])

	r.Dx2 = strings.TrimSpace(c.Dx2[i])

	r.Dx3 = strings.TrimSpace(c.Dx3[i])

	r.Dx4 = strings.TrimSpace(c.Dx4[i])

	r.Dx5 = strings.TrimSpace(c.Dx5[i])

	r.Dx6 = strings.TrimSpace(c.Dx6[i])

	r.Dx7 = strings.TrimSpace(c.Dx7[i])

	r.Dx8 = strings.TrimSpace(c.Dx8[i])

	r.Dx9 = strings.TrimSpace(c.Dx9[i])

	r.Proc1 = strings.TrimSpace(c.Proc1[i])

	r.Proc2 = strings.TrimSpace(c.Proc2[i])

	r.Proc3 = strings.TrimSpace(c.Proc3[i])

	r.Proc4 = strings.TrimSpace(c.Proc4[i])

	r.Proc5 = strings.TrimSpace(c.Proc5[i])

	r.Proc6 = strings.TrimSpace(c.Proc6[i])

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

	for _, x := range vec {
		_, err := wtr.Write([]byte(x + "\n"))
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
		msg := fmt.Sprintf("Usage: %s config.json\n\n", os.Args[0])
		os.Stderr.WriteString(msg)
		os.Exit(1)
	}

	conf = config.ReadConfig(os.Args[1])
	setupLogger()
	logger.Printf("Read config from %s", os.Args[1])

	Run(conf, logger)

	logger.Printf("Finished, exiting")
}
