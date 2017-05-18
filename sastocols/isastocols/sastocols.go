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

	dtypes = `{"Admdate":"uint16","Admtyp":"uint8","Disdate":"uint16","Dstatus":"uint8","Dx1":"string","Dx10":"string","Dx11":"string","Dx12":"string","Dx13":"string","Dx14":"string","Dx15":"string","Dx2":"string","Dx3":"string","Dx4":"string","Dx5":"string","Dx6":"string","Dx7":"string","Dx8":"string","Dx9":"string","Enrolid":"uint64","Hospnet":"float32","Hosppay":"float32","Physnet":"float32","Physpay":"float32","Proc1":"string","Proc10":"string","Proc11":"string","Proc12":"string","Proc13":"string","Proc14":"string","Proc15":"string","Proc2":"string","Proc3":"string","Proc4":"string","Proc5":"string","Proc6":"string","Proc7":"string","Proc8":"string","Proc9":"string","Seqnum":"uint64","Totcoins":"float32","Totcopay":"float32","Totded":"float32","Totnet":"float32","Totpay":"float32"}
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
	Admtyp   uint8
	Admdate  uint16
	Disdate  uint16
	Dstatus  uint8
	Dx1      string
	Dx2      string
	Dx3      string
	Dx4      string
	Dx5      string
	Dx6      string
	Dx7      string
	Dx8      string
	Dx9      string
	Dx10     string
	Dx11     string
	Dx12     string
	Dx13     string
	Dx14     string
	Dx15     string
	Enrolid  uint64
	Hospnet  float32
	Hosppay  float32
	Proc1    string
	Proc2    string
	Proc3    string
	Proc4    string
	Proc5    string
	Proc6    string
	Proc7    string
	Proc8    string
	Proc9    string
	Proc10   string
	Proc11   string
	Proc12   string
	Proc13   string
	Proc14   string
	Proc15   string
	Physnet  float32
	Physpay  float32
	Seqnum   uint64
	Totpay   float32
	Totcoins float32
	Totcopay float32
	Totded   float32
	Totnet   float32
}

// Bucket is a memory-backed container for columnized data.  It
// contains data exactly as it will be written to disk.
type Bucket struct {
	BaseBucket

	code     []uint16
	Admtyp   []uint8
	Admdate  []uint16
	Disdate  []uint16
	Dstatus  []uint8
	Dx1      []string
	Dx2      []string
	Dx3      []string
	Dx4      []string
	Dx5      []string
	Dx6      []string
	Dx7      []string
	Dx8      []string
	Dx9      []string
	Dx10     []string
	Dx11     []string
	Dx12     []string
	Dx13     []string
	Dx14     []string
	Dx15     []string
	Enrolid  []uint64
	Hospnet  []float32
	Hosppay  []float32
	Proc1    []string
	Proc2    []string
	Proc3    []string
	Proc4    []string
	Proc5    []string
	Proc6    []string
	Proc7    []string
	Proc8    []string
	Proc9    []string
	Proc10   []string
	Proc11   []string
	Proc12   []string
	Proc13   []string
	Proc14   []string
	Proc15   []string
	Physnet  []float32
	Physpay  []float32
	Seqnum   []uint64
	Totpay   []float32
	Totcoins []float32
	Totcopay []float32
	Totded   []float32
	Totnet   []float32
}

// chunk is a typed container for data pulled directly out of a SAS file.
// There are no type conversions or other modifications from the SAS file.
type chunk struct {
	row       int
	col       int
	Admtyp    []string
	Admtypm   []bool
	Admdate   []float64
	Admdatem  []bool
	Disdate   []float64
	Disdatem  []bool
	Dstatus   []string
	Dstatusm  []bool
	Dx1       []string
	Dx1m      []bool
	Dx2       []string
	Dx2m      []bool
	Dx3       []string
	Dx3m      []bool
	Dx4       []string
	Dx4m      []bool
	Dx5       []string
	Dx5m      []bool
	Dx6       []string
	Dx6m      []bool
	Dx7       []string
	Dx7m      []bool
	Dx8       []string
	Dx8m      []bool
	Dx9       []string
	Dx9m      []bool
	Dx10      []string
	Dx10m     []bool
	Dx11      []string
	Dx11m     []bool
	Dx12      []string
	Dx12m     []bool
	Dx13      []string
	Dx13m     []bool
	Dx14      []string
	Dx14m     []bool
	Dx15      []string
	Dx15m     []bool
	Enrolid   []float64
	Enrolidm  []bool
	Hospnet   []float64
	Hospnetm  []bool
	Hosppay   []float64
	Hosppaym  []bool
	Proc1     []string
	Proc1m    []bool
	Proc2     []string
	Proc2m    []bool
	Proc3     []string
	Proc3m    []bool
	Proc4     []string
	Proc4m    []bool
	Proc5     []string
	Proc5m    []bool
	Proc6     []string
	Proc6m    []bool
	Proc7     []string
	Proc7m    []bool
	Proc8     []string
	Proc8m    []bool
	Proc9     []string
	Proc9m    []bool
	Proc10    []string
	Proc10m   []bool
	Proc11    []string
	Proc11m   []bool
	Proc12    []string
	Proc12m   []bool
	Proc13    []string
	Proc13m   []bool
	Proc14    []string
	Proc14m   []bool
	Proc15    []string
	Proc15m   []bool
	Physnet   []float64
	Physnetm  []bool
	Physpay   []float64
	Physpaym  []bool
	Seqnum    []float64
	Seqnumm   []bool
	Totpay    []float64
	Totpaym   []bool
	Totcoins  []float64
	Totcoinsm []bool
	Totcopay  []float64
	Totcopaym []bool
	Totded    []float64
	Totdedm   []bool
	Totnet    []float64
	Totnetm   []bool
}

// Add appends a rec to the end of the Bucket.
func (bucket *Bucket) Add(r *rec) {

	bucket.Mut.Lock()

	bucket.Admtyp = append(bucket.Admtyp, r.Admtyp)
	bucket.Admdate = append(bucket.Admdate, r.Admdate)
	bucket.Disdate = append(bucket.Disdate, r.Disdate)
	bucket.Dstatus = append(bucket.Dstatus, r.Dstatus)
	bucket.Dx1 = append(bucket.Dx1, r.Dx1)
	bucket.Dx2 = append(bucket.Dx2, r.Dx2)
	bucket.Dx3 = append(bucket.Dx3, r.Dx3)
	bucket.Dx4 = append(bucket.Dx4, r.Dx4)
	bucket.Dx5 = append(bucket.Dx5, r.Dx5)
	bucket.Dx6 = append(bucket.Dx6, r.Dx6)
	bucket.Dx7 = append(bucket.Dx7, r.Dx7)
	bucket.Dx8 = append(bucket.Dx8, r.Dx8)
	bucket.Dx9 = append(bucket.Dx9, r.Dx9)
	bucket.Dx10 = append(bucket.Dx10, r.Dx10)
	bucket.Dx11 = append(bucket.Dx11, r.Dx11)
	bucket.Dx12 = append(bucket.Dx12, r.Dx12)
	bucket.Dx13 = append(bucket.Dx13, r.Dx13)
	bucket.Dx14 = append(bucket.Dx14, r.Dx14)
	bucket.Dx15 = append(bucket.Dx15, r.Dx15)
	bucket.Enrolid = append(bucket.Enrolid, r.Enrolid)
	bucket.Hospnet = append(bucket.Hospnet, r.Hospnet)
	bucket.Hosppay = append(bucket.Hosppay, r.Hosppay)
	bucket.Proc1 = append(bucket.Proc1, r.Proc1)
	bucket.Proc2 = append(bucket.Proc2, r.Proc2)
	bucket.Proc3 = append(bucket.Proc3, r.Proc3)
	bucket.Proc4 = append(bucket.Proc4, r.Proc4)
	bucket.Proc5 = append(bucket.Proc5, r.Proc5)
	bucket.Proc6 = append(bucket.Proc6, r.Proc6)
	bucket.Proc7 = append(bucket.Proc7, r.Proc7)
	bucket.Proc8 = append(bucket.Proc8, r.Proc8)
	bucket.Proc9 = append(bucket.Proc9, r.Proc9)
	bucket.Proc10 = append(bucket.Proc10, r.Proc10)
	bucket.Proc11 = append(bucket.Proc11, r.Proc11)
	bucket.Proc12 = append(bucket.Proc12, r.Proc12)
	bucket.Proc13 = append(bucket.Proc13, r.Proc13)
	bucket.Proc14 = append(bucket.Proc14, r.Proc14)
	bucket.Proc15 = append(bucket.Proc15, r.Proc15)
	bucket.Physnet = append(bucket.Physnet, r.Physnet)
	bucket.Physpay = append(bucket.Physpay, r.Physpay)
	bucket.Seqnum = append(bucket.Seqnum, r.Seqnum)
	bucket.Totpay = append(bucket.Totpay, r.Totpay)
	bucket.Totcoins = append(bucket.Totcoins, r.Totcoins)
	bucket.Totcopay = append(bucket.Totcopay, r.Totcopay)
	bucket.Totded = append(bucket.Totded, r.Totded)
	bucket.Totnet = append(bucket.Totnet, r.Totnet)

	bucket.Mut.Unlock()

	if uint64(len(bucket.Enrolid)) > conf.BufMaxRecs {
		bucket.Flush()
	}
}

// Flush writes all the data from the Bucket to disk.
func (bucket *Bucket) Flush() {

	logger.Printf("Flushing bucket %d", bucket.BucketNum)

	bucket.Mut.Lock()

	bucket.flushuint8("Admtyp", bucket.Admtyp)
	bucket.Admtyp = bucket.Admtyp[0:0]
	bucket.flushuint16("Admdate", bucket.Admdate)
	bucket.Admdate = bucket.Admdate[0:0]
	bucket.flushuint16("Disdate", bucket.Disdate)
	bucket.Disdate = bucket.Disdate[0:0]
	bucket.flushuint8("Dstatus", bucket.Dstatus)
	bucket.Dstatus = bucket.Dstatus[0:0]
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
	bucket.flushstring("Dx10", bucket.Dx10)
	bucket.Dx10 = bucket.Dx10[0:0]
	bucket.flushstring("Dx11", bucket.Dx11)
	bucket.Dx11 = bucket.Dx11[0:0]
	bucket.flushstring("Dx12", bucket.Dx12)
	bucket.Dx12 = bucket.Dx12[0:0]
	bucket.flushstring("Dx13", bucket.Dx13)
	bucket.Dx13 = bucket.Dx13[0:0]
	bucket.flushstring("Dx14", bucket.Dx14)
	bucket.Dx14 = bucket.Dx14[0:0]
	bucket.flushstring("Dx15", bucket.Dx15)
	bucket.Dx15 = bucket.Dx15[0:0]
	bucket.flushuint64("Enrolid", bucket.Enrolid)
	bucket.Enrolid = bucket.Enrolid[0:0]
	bucket.flushfloat32("Hospnet", bucket.Hospnet)
	bucket.Hospnet = bucket.Hospnet[0:0]
	bucket.flushfloat32("Hosppay", bucket.Hosppay)
	bucket.Hosppay = bucket.Hosppay[0:0]
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
	bucket.flushstring("Proc7", bucket.Proc7)
	bucket.Proc7 = bucket.Proc7[0:0]
	bucket.flushstring("Proc8", bucket.Proc8)
	bucket.Proc8 = bucket.Proc8[0:0]
	bucket.flushstring("Proc9", bucket.Proc9)
	bucket.Proc9 = bucket.Proc9[0:0]
	bucket.flushstring("Proc10", bucket.Proc10)
	bucket.Proc10 = bucket.Proc10[0:0]
	bucket.flushstring("Proc11", bucket.Proc11)
	bucket.Proc11 = bucket.Proc11[0:0]
	bucket.flushstring("Proc12", bucket.Proc12)
	bucket.Proc12 = bucket.Proc12[0:0]
	bucket.flushstring("Proc13", bucket.Proc13)
	bucket.Proc13 = bucket.Proc13[0:0]
	bucket.flushstring("Proc14", bucket.Proc14)
	bucket.Proc14 = bucket.Proc14[0:0]
	bucket.flushstring("Proc15", bucket.Proc15)
	bucket.Proc15 = bucket.Proc15[0:0]
	bucket.flushfloat32("Physnet", bucket.Physnet)
	bucket.Physnet = bucket.Physnet[0:0]
	bucket.flushfloat32("Physpay", bucket.Physpay)
	bucket.Physpay = bucket.Physpay[0:0]
	bucket.flushuint64("Seqnum", bucket.Seqnum)
	bucket.Seqnum = bucket.Seqnum[0:0]
	bucket.flushfloat32("Totpay", bucket.Totpay)
	bucket.Totpay = bucket.Totpay[0:0]
	bucket.flushfloat32("Totcoins", bucket.Totcoins)
	bucket.Totcoins = bucket.Totcoins[0:0]
	bucket.flushfloat32("Totcopay", bucket.Totcopay)
	bucket.Totcopay = bucket.Totcopay[0:0]
	bucket.flushfloat32("Totded", bucket.Totded)
	bucket.Totded = bucket.Totded[0:0]
	bucket.flushfloat32("Totnet", bucket.Totnet)
	bucket.Totnet = bucket.Totnet[0:0]

	bucket.Mut.Unlock()
}

// getcols fills a chunk with data from a SAS file.
func (c *chunk) getcols(data []*datareader.Series, cm map[string]int) error {

	var err error
	var ii int
	var ok bool

	ii, ok = cm["ADMTYP"]
	if ok {
		c.Admtyp, c.Admtypm, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable ADMTYP required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["ADMDATE"]
	if ok {
		c.Admdate, c.Admdatem, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable ADMDATE required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["DISDATE"]
	if ok {
		c.Disdate, c.Disdatem, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable DISDATE required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["DSTATUS"]
	if ok {
		c.Dstatus, c.Dstatusm, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable DSTATUS required but not found in SAS file\n")
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

	ii, ok = cm["DX10"]
	if ok {
		c.Dx10, c.Dx10m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable DX10 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["DX11"]
	if ok {
		c.Dx11, c.Dx11m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable DX11 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["DX12"]
	if ok {
		c.Dx12, c.Dx12m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable DX12 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["DX13"]
	if ok {
		c.Dx13, c.Dx13m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable DX13 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["DX14"]
	if ok {
		c.Dx14, c.Dx14m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable DX14 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["DX15"]
	if ok {
		c.Dx15, c.Dx15m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable DX15 required but not found in SAS file\n")
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

	ii, ok = cm["HOSPNET"]
	if ok {
		c.Hospnet, c.Hospnetm, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable HOSPNET required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["HOSPPAY"]
	if ok {
		c.Hosppay, c.Hosppaym, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable HOSPPAY required but not found in SAS file\n")
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

	ii, ok = cm["PROC7"]
	if ok {
		c.Proc7, c.Proc7m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable PROC7 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["PROC8"]
	if ok {
		c.Proc8, c.Proc8m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable PROC8 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["PROC9"]
	if ok {
		c.Proc9, c.Proc9m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable PROC9 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["PROC10"]
	if ok {
		c.Proc10, c.Proc10m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable PROC10 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["PROC11"]
	if ok {
		c.Proc11, c.Proc11m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable PROC11 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["PROC12"]
	if ok {
		c.Proc12, c.Proc12m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable PROC12 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["PROC13"]
	if ok {
		c.Proc13, c.Proc13m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable PROC13 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["PROC14"]
	if ok {
		c.Proc14, c.Proc14m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable PROC14 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["PROC15"]
	if ok {
		c.Proc15, c.Proc15m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable PROC15 required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["PHYSNET"]
	if ok {
		c.Physnet, c.Physnetm, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable PHYSNET required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["PHYSPAY"]
	if ok {
		c.Physpay, c.Physpaym, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable PHYSPAY required but not found in SAS file\n")
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

	ii, ok = cm["TOTPAY"]
	if ok {
		c.Totpay, c.Totpaym, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable TOTPAY required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["TOTCOINS"]
	if ok {
		c.Totcoins, c.Totcoinsm, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable TOTCOINS required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["TOTCOPAY"]
	if ok {
		c.Totcopay, c.Totcopaym, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable TOTCOPAY required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["TOTDED"]
	if ok {
		c.Totded, c.Totdedm, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable TOTDED required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["TOTNET"]
	if ok {
		c.Totnet, c.Totnetm, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable TOTNET required but not found in SAS file\n")
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

	// Convert string to number
	if len(c.Admtyp[i]) > 0 {
		x, err := strconv.Atoi(c.Admtyp[i])
		if err == nil {
			r.Admtyp = uint8(x)
		}
	}

	r.Admdate = uint16(c.Admdate[i])

	r.Disdate = uint16(c.Disdate[i])

	// Convert string to number
	if len(c.Dstatus[i]) > 0 {
		x, err := strconv.Atoi(c.Dstatus[i])
		if err == nil {
			r.Dstatus = uint8(x)
		}
	}

	r.Dx1 = strings.TrimSpace(c.Dx1[i])

	r.Dx2 = strings.TrimSpace(c.Dx2[i])

	r.Dx3 = strings.TrimSpace(c.Dx3[i])

	r.Dx4 = strings.TrimSpace(c.Dx4[i])

	r.Dx5 = strings.TrimSpace(c.Dx5[i])

	r.Dx6 = strings.TrimSpace(c.Dx6[i])

	r.Dx7 = strings.TrimSpace(c.Dx7[i])

	r.Dx8 = strings.TrimSpace(c.Dx8[i])

	r.Dx9 = strings.TrimSpace(c.Dx9[i])

	r.Dx10 = strings.TrimSpace(c.Dx10[i])

	r.Dx11 = strings.TrimSpace(c.Dx11[i])

	r.Dx12 = strings.TrimSpace(c.Dx12[i])

	r.Dx13 = strings.TrimSpace(c.Dx13[i])

	r.Dx14 = strings.TrimSpace(c.Dx14[i])

	r.Dx15 = strings.TrimSpace(c.Dx15[i])

	r.Enrolid = uint64(c.Enrolid[i])

	r.Hospnet = float32(c.Hospnet[i])

	r.Hosppay = float32(c.Hosppay[i])

	r.Proc1 = strings.TrimSpace(c.Proc1[i])

	r.Proc2 = strings.TrimSpace(c.Proc2[i])

	r.Proc3 = strings.TrimSpace(c.Proc3[i])

	r.Proc4 = strings.TrimSpace(c.Proc4[i])

	r.Proc5 = strings.TrimSpace(c.Proc5[i])

	r.Proc6 = strings.TrimSpace(c.Proc6[i])

	r.Proc7 = strings.TrimSpace(c.Proc7[i])

	r.Proc8 = strings.TrimSpace(c.Proc8[i])

	r.Proc9 = strings.TrimSpace(c.Proc9[i])

	r.Proc10 = strings.TrimSpace(c.Proc10[i])

	r.Proc11 = strings.TrimSpace(c.Proc11[i])

	r.Proc12 = strings.TrimSpace(c.Proc12[i])

	r.Proc13 = strings.TrimSpace(c.Proc13[i])

	r.Proc14 = strings.TrimSpace(c.Proc14[i])

	r.Proc15 = strings.TrimSpace(c.Proc15[i])

	r.Physnet = float32(c.Physnet[i])

	r.Physpay = float32(c.Physpay[i])

	r.Seqnum = uint64(c.Seqnum[i])

	r.Totpay = float32(c.Totpay[i])

	r.Totcoins = float32(c.Totcoins[i])

	r.Totcopay = float32(c.Totcopay[i])

	r.Totded = float32(c.Totded[i])

	r.Totnet = float32(c.Totnet[i])

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
