// GENERATED CODE, DO NOT EDIT
package sastocols

import (
	"github.com/kshedden/datareader"
	"strconv"
	"strings"
)

var (
	dtypes = `{"copay":"uint32","deduct":"uint32","dobyr":"uint16","dx1":"string","dx2":"string","dx3":"string","dx4":"string","egeoloc":"uint8","emprel":"uint8","enrolid":"uint64","mhsacovg":"uint8","msa":"uint32","netpay":"uint32","proc1":"string","region":"uint8","rx":"uint8","sex":"uint8","stdprov":"uint16","svcdate":"uint16","svcscat":"uint32","wgtkey":"uint8"}
`
)

// rec is a row that will be added to a Bucket.
type rec struct {
	enrolid  uint64
	dx1      string
	dx2      string
	dx3      string
	dx4      string
	proc1    string
	svcdate  uint16
	dobyr    uint16
	copay    uint32
	deduct   uint32
	stdprov  uint16
	mhsacovg uint8
	netpay   uint32
	svcscat  uint32
	region   uint8
	msa      uint32
	wgtkey   uint8
	egeoloc  uint8
	emprel   uint8
	rx       uint8
	sex      uint8
}

// Bucket is a memory-backed container for columnized data.  It
// contains data exactly as it will be written to disk.
type Bucket struct {
	BaseBucket

	code     []uint16
	enrolid  []uint64
	dx1      []string
	dx2      []string
	dx3      []string
	dx4      []string
	proc1    []string
	svcdate  []uint16
	dobyr    []uint16
	copay    []uint32
	deduct   []uint32
	stdprov  []uint16
	mhsacovg []uint8
	netpay   []uint32
	svcscat  []uint32
	region   []uint8
	msa      []uint32
	wgtkey   []uint8
	egeoloc  []uint8
	emprel   []uint8
	rx       []uint8
	sex      []uint8
}

// chunk is a typed container for data pulled directly out of a SAS file.
// There are no type conversions or other modifications from the SAS file.
type chunk struct {
	row       int
	col       int
	enrolid   []float64
	enrolidm  []bool
	dx1       []string
	dx1m      []bool
	dx2       []string
	dx2m      []bool
	dx3       []string
	dx3m      []bool
	dx4       []string
	dx4m      []bool
	proc1     []string
	proc1m    []bool
	svcdate   []float64
	svcdatem  []bool
	dobyr     []float64
	dobyrm    []bool
	copay     []float64
	copaym    []bool
	deduct    []float64
	deductm   []bool
	stdprov   []float64
	stdprovm  []bool
	mhsacovg  []string
	mhsacovgm []bool
	netpay    []float64
	netpaym   []bool
	svcscat   []string
	svcscatm  []bool
	region    []string
	regionm   []bool
	msa       []float64
	msam      []bool
	wgtkey    []float64
	wgtkeym   []bool
	egeoloc   []string
	egeolocm  []bool
	emprel    []string
	emprelm   []bool
	rx        []string
	rxm       []bool
	sex       []string
	sexm      []bool
}

// Add appends a rec to the end of the Bucket.
func (bucket *Bucket) Add(r *rec) {

	bucket.mut.Lock()

	bucket.enrolid = append(bucket.enrolid, r.enrolid)
	bucket.dx1 = append(bucket.dx1, r.dx1)
	bucket.dx2 = append(bucket.dx2, r.dx2)
	bucket.dx3 = append(bucket.dx3, r.dx3)
	bucket.dx4 = append(bucket.dx4, r.dx4)
	bucket.proc1 = append(bucket.proc1, r.proc1)
	bucket.svcdate = append(bucket.svcdate, r.svcdate)
	bucket.dobyr = append(bucket.dobyr, r.dobyr)
	bucket.copay = append(bucket.copay, r.copay)
	bucket.deduct = append(bucket.deduct, r.deduct)
	bucket.stdprov = append(bucket.stdprov, r.stdprov)
	bucket.mhsacovg = append(bucket.mhsacovg, r.mhsacovg)
	bucket.netpay = append(bucket.netpay, r.netpay)
	bucket.svcscat = append(bucket.svcscat, r.svcscat)
	bucket.region = append(bucket.region, r.region)
	bucket.msa = append(bucket.msa, r.msa)
	bucket.wgtkey = append(bucket.wgtkey, r.wgtkey)
	bucket.egeoloc = append(bucket.egeoloc, r.egeoloc)
	bucket.emprel = append(bucket.emprel, r.emprel)
	bucket.rx = append(bucket.rx, r.rx)
	bucket.sex = append(bucket.sex, r.sex)

	bucket.mut.Unlock()

	if uint64(len(bucket.enrolid)) > conf.BufMaxRecs {
		bucket.Flush()
	}
}

// Flush writes all the data from the Bucket to disk.
func (bucket *Bucket) Flush() {

	logger.Printf("Flushing bucket %d", bucket.BucketNum)

	bucket.mut.Lock()

	bucket.flushuint64("enrolid", bucket.enrolid)
	bucket.code = bucket.code[0:0]
	bucket.flushstring("dx1", bucket.dx1)
	bucket.code = bucket.code[0:0]
	bucket.flushstring("dx2", bucket.dx2)
	bucket.code = bucket.code[0:0]
	bucket.flushstring("dx3", bucket.dx3)
	bucket.code = bucket.code[0:0]
	bucket.flushstring("dx4", bucket.dx4)
	bucket.code = bucket.code[0:0]
	bucket.flushstring("proc1", bucket.proc1)
	bucket.code = bucket.code[0:0]
	bucket.flushuint16("svcdate", bucket.svcdate)
	bucket.code = bucket.code[0:0]
	bucket.flushuint16("dobyr", bucket.dobyr)
	bucket.code = bucket.code[0:0]
	bucket.flushuint32("copay", bucket.copay)
	bucket.code = bucket.code[0:0]
	bucket.flushuint32("deduct", bucket.deduct)
	bucket.code = bucket.code[0:0]
	bucket.flushuint16("stdprov", bucket.stdprov)
	bucket.code = bucket.code[0:0]
	bucket.flushuint8("mhsacovg", bucket.mhsacovg)
	bucket.code = bucket.code[0:0]
	bucket.flushuint32("netpay", bucket.netpay)
	bucket.code = bucket.code[0:0]
	bucket.flushuint32("svcscat", bucket.svcscat)
	bucket.code = bucket.code[0:0]
	bucket.flushuint8("region", bucket.region)
	bucket.code = bucket.code[0:0]
	bucket.flushuint32("msa", bucket.msa)
	bucket.code = bucket.code[0:0]
	bucket.flushuint8("wgtkey", bucket.wgtkey)
	bucket.code = bucket.code[0:0]
	bucket.flushuint8("egeoloc", bucket.egeoloc)
	bucket.code = bucket.code[0:0]
	bucket.flushuint8("emprel", bucket.emprel)
	bucket.code = bucket.code[0:0]
	bucket.flushuint8("rx", bucket.rx)
	bucket.code = bucket.code[0:0]
	bucket.flushuint8("sex", bucket.sex)
	bucket.code = bucket.code[0:0]

	bucket.mut.Unlock()
}

// getcols fills a chunk with data from a SAS file.
func (c *chunk) getcols(data []*datareader.Series, cm map[string]int) {

	var err error
	c.enrolid, c.enrolidm, err = data[cm["ENROLID"]].AsFloat64Slice()
	if err != nil {
		panic(err)
	}

	var ii int
	var ok bool

	ii, ok = cm["ENROLID"]
	if ok {
		c.enrolid, c.enrolidm, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}
	}

	ii, ok = cm["DX1"]
	if ok {
		c.dx1, c.dx1m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}
	}

	ii, ok = cm["DX2"]
	if ok {
		c.dx2, c.dx2m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}
	}

	ii, ok = cm["DX3"]
	if ok {
		c.dx3, c.dx3m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}
	}

	ii, ok = cm["DX4"]
	if ok {
		c.dx4, c.dx4m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}
	}

	ii, ok = cm["PROC1"]
	if ok {
		c.proc1, c.proc1m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}
	}

	ii, ok = cm["SVCDATE"]
	if ok {
		c.svcdate, c.svcdatem, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}
	}

	ii, ok = cm["DOBYR"]
	if ok {
		c.dobyr, c.dobyrm, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}
	}

	ii, ok = cm["COPAY"]
	if ok {
		c.copay, c.copaym, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}
	}

	ii, ok = cm["DEDUCT"]
	if ok {
		c.deduct, c.deductm, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}
	}

	ii, ok = cm["STDPROV"]
	if ok {
		c.stdprov, c.stdprovm, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}
	}

	ii, ok = cm["MHSACOVG"]
	if ok {
		c.mhsacovg, c.mhsacovgm, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}
	}

	ii, ok = cm["NETPAY"]
	if ok {
		c.netpay, c.netpaym, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}
	}

	ii, ok = cm["SVCSCAT"]
	if ok {
		c.svcscat, c.svcscatm, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}
	}

	ii, ok = cm["REGION"]
	if ok {
		c.region, c.regionm, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}
	}

	ii, ok = cm["MSA"]
	if ok {
		c.msa, c.msam, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}
	}

	ii, ok = cm["WGTKEY"]
	if ok {
		c.wgtkey, c.wgtkeym, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}
	}

	ii, ok = cm["EGEOLOC"]
	if ok {
		c.egeoloc, c.egeolocm, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}
	}

	ii, ok = cm["EMPREL"]
	if ok {
		c.emprel, c.emprelm, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}
	}

	ii, ok = cm["RX"]
	if ok {
		c.rx, c.rxm, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}
	}

	ii, ok = cm["SEX"]
	if ok {
		c.sex, c.sexm, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}
	}

}

func (c *chunk) trynextrec() (*rec, bool) {

	if c.row >= len(c.enrolid) {
		return nil, false
	}

	r := new(rec)

	i := c.row

	if c.enrolidm[i] {
		c.row++
		return nil, true
	}

	r.enrolid = uint64(c.enrolid[i])

	r.dx1 = strings.TrimSpace(c.dx1[i])

	r.dx2 = strings.TrimSpace(c.dx2[i])

	if c.dx3 != nil {

		r.dx3 = strings.TrimSpace(c.dx3[i])

	}

	if c.dx4 != nil {

		r.dx4 = strings.TrimSpace(c.dx4[i])

	}

	r.proc1 = strings.TrimSpace(c.proc1[i])

	r.svcdate = uint16(c.svcdate[i])

	r.dobyr = uint16(c.dobyr[i])

	r.copay = uint32(c.copay[i])

	r.deduct = uint32(c.deduct[i])

	r.stdprov = uint16(c.stdprov[i])

	// Convert string to number
	if len(c.mhsacovg[i]) > 0 {
		x, err := strconv.Atoi(c.mhsacovg[i])
		if err == nil {
			r.mhsacovg = uint8(x)
		}
	}

	r.netpay = uint32(c.netpay[i])

	// Convert string to number
	if len(c.svcscat[i]) > 0 {
		x, err := strconv.Atoi(c.svcscat[i])
		if err == nil {
			r.svcscat = uint32(x)
		}
	}

	// Convert string to number
	if len(c.region[i]) > 0 {
		x, err := strconv.Atoi(c.region[i])
		if err == nil {
			r.region = uint8(x)
		}
	}

	r.msa = uint32(c.msa[i])

	r.wgtkey = uint8(c.wgtkey[i])

	// Convert string to number
	if len(c.egeoloc[i]) > 0 {
		x, err := strconv.Atoi(c.egeoloc[i])
		if err == nil {
			r.egeoloc = uint8(x)
		}
	}

	// Convert string to number
	if len(c.emprel[i]) > 0 {
		x, err := strconv.Atoi(c.emprel[i])
		if err == nil {
			r.emprel = uint8(x)
		}
	}

	// Convert string to number
	if len(c.rx[i]) > 0 {
		x, err := strconv.Atoi(c.rx[i])
		if err == nil {
			r.rx = uint8(x)
		}
	}

	// Convert string to number
	if len(c.sex[i]) > 0 {
		x, err := strconv.Atoi(c.sex[i])
		if err == nil {
			r.sex = uint8(x)
		}
	}

	c.row++

	return r, true
}
