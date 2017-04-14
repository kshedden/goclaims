// GENERATED CODE, DO NOT EDIT
package sastocols

import (
	"fmt"
	"github.com/kshedden/datareader"
	"strconv"
	"strings"
)

var (
	dtypes = `{"Copay":"uint32","Deduct":"uint32","Dobyr":"uint16","Dx1":"string","Dx2":"string","Dx3":"string","Dx4":"string","Egeoloc":"uint8","Emprel":"uint8","Enrolid":"uint64","MHSAcovg":"uint8","MSA":"uint32","MSwgtkey":"uint8","Netpay":"uint32","Proc1":"string","Region":"uint8","Rx":"uint8","Sex":"uint8","Stdprov":"uint16","Svcdate":"uint16","Svcscat":"uint32","Wgtkey":"uint8"}
`
)

// rec is a row that will be added to a Bucket.
type rec struct {
	Enrolid  uint64
	Dx1      string
	Dx2      string
	Dx3      string
	Dx4      string
	Proc1    string
	Svcdate  uint16
	Dobyr    uint16
	Copay    uint32
	Deduct   uint32
	Stdprov  uint16
	MHSAcovg uint8
	Netpay   uint32
	Svcscat  uint32
	Region   uint8
	MSA      uint32
	Wgtkey   uint8
	MSwgtkey uint8
	Egeoloc  uint8
	Emprel   uint8
	Rx       uint8
	Sex      uint8
}

// Bucket is a memory-backed container for columnized data.  It
// contains data exactly as it will be written to disk.
type Bucket struct {
	BaseBucket

	code     []uint16
	Enrolid  []uint64
	Dx1      []string
	Dx2      []string
	Dx3      []string
	Dx4      []string
	Proc1    []string
	Svcdate  []uint16
	Dobyr    []uint16
	Copay    []uint32
	Deduct   []uint32
	Stdprov  []uint16
	MHSAcovg []uint8
	Netpay   []uint32
	Svcscat  []uint32
	Region   []uint8
	MSA      []uint32
	Wgtkey   []uint8
	MSwgtkey []uint8
	Egeoloc  []uint8
	Emprel   []uint8
	Rx       []uint8
	Sex      []uint8
}

// chunk is a typed container for data pulled directly out of a SAS file.
// There are no type conversions or other modifications from the SAS file.
type chunk struct {
	row       int
	col       int
	Enrolid   []float64
	Enrolidm  []bool
	Dx1       []string
	Dx1m      []bool
	Dx2       []string
	Dx2m      []bool
	Dx3       []string
	Dx3m      []bool
	Dx4       []string
	Dx4m      []bool
	Proc1     []string
	Proc1m    []bool
	Svcdate   []float64
	Svcdatem  []bool
	Dobyr     []float64
	Dobyrm    []bool
	Copay     []float64
	Copaym    []bool
	Deduct    []float64
	Deductm   []bool
	Stdprov   []float64
	Stdprovm  []bool
	MHSAcovg  []string
	MHSAcovgm []bool
	Netpay    []float64
	Netpaym   []bool
	Svcscat   []string
	Svcscatm  []bool
	Region    []string
	Regionm   []bool
	MSA       []float64
	MSAm      []bool
	Wgtkey    []float64
	Wgtkeym   []bool
	MSwgtkey  []float64
	MSwgtkeym []bool
	Egeoloc   []string
	Egeolocm  []bool
	Emprel    []string
	Emprelm   []bool
	Rx        []string
	Rxm       []bool
	Sex       []string
	Sexm      []bool
}

// Add appends a rec to the end of the Bucket.
func (bucket *Bucket) Add(r *rec) {

	bucket.mut.Lock()

	bucket.Enrolid = append(bucket.Enrolid, r.Enrolid)
	bucket.Dx1 = append(bucket.Dx1, r.Dx1)
	bucket.Dx2 = append(bucket.Dx2, r.Dx2)
	bucket.Dx3 = append(bucket.Dx3, r.Dx3)
	bucket.Dx4 = append(bucket.Dx4, r.Dx4)
	bucket.Proc1 = append(bucket.Proc1, r.Proc1)
	bucket.Svcdate = append(bucket.Svcdate, r.Svcdate)
	bucket.Dobyr = append(bucket.Dobyr, r.Dobyr)
	bucket.Copay = append(bucket.Copay, r.Copay)
	bucket.Deduct = append(bucket.Deduct, r.Deduct)
	bucket.Stdprov = append(bucket.Stdprov, r.Stdprov)
	bucket.MHSAcovg = append(bucket.MHSAcovg, r.MHSAcovg)
	bucket.Netpay = append(bucket.Netpay, r.Netpay)
	bucket.Svcscat = append(bucket.Svcscat, r.Svcscat)
	bucket.Region = append(bucket.Region, r.Region)
	bucket.MSA = append(bucket.MSA, r.MSA)
	bucket.Wgtkey = append(bucket.Wgtkey, r.Wgtkey)
	bucket.MSwgtkey = append(bucket.MSwgtkey, r.MSwgtkey)
	bucket.Egeoloc = append(bucket.Egeoloc, r.Egeoloc)
	bucket.Emprel = append(bucket.Emprel, r.Emprel)
	bucket.Rx = append(bucket.Rx, r.Rx)
	bucket.Sex = append(bucket.Sex, r.Sex)

	bucket.mut.Unlock()

	if uint64(len(bucket.Enrolid)) > conf.BufMaxRecs {
		bucket.Flush()
	}
}

// Flush writes all the data from the Bucket to disk.
func (bucket *Bucket) Flush() {

	logger.Printf("Flushing bucket %d", bucket.BucketNum)

	bucket.mut.Lock()

	bucket.flushuint64("Enrolid", bucket.Enrolid)
	bucket.Enrolid = bucket.Enrolid[0:0]
	bucket.flushstring("Dx1", bucket.Dx1)
	bucket.Dx1 = bucket.Dx1[0:0]
	bucket.flushstring("Dx2", bucket.Dx2)
	bucket.Dx2 = bucket.Dx2[0:0]
	bucket.flushstring("Dx3", bucket.Dx3)
	bucket.Dx3 = bucket.Dx3[0:0]
	bucket.flushstring("Dx4", bucket.Dx4)
	bucket.Dx4 = bucket.Dx4[0:0]
	bucket.flushstring("Proc1", bucket.Proc1)
	bucket.Proc1 = bucket.Proc1[0:0]
	bucket.flushuint16("Svcdate", bucket.Svcdate)
	bucket.Svcdate = bucket.Svcdate[0:0]
	bucket.flushuint16("Dobyr", bucket.Dobyr)
	bucket.Dobyr = bucket.Dobyr[0:0]
	bucket.flushuint32("Copay", bucket.Copay)
	bucket.Copay = bucket.Copay[0:0]
	bucket.flushuint32("Deduct", bucket.Deduct)
	bucket.Deduct = bucket.Deduct[0:0]
	bucket.flushuint16("Stdprov", bucket.Stdprov)
	bucket.Stdprov = bucket.Stdprov[0:0]
	bucket.flushuint8("MHSAcovg", bucket.MHSAcovg)
	bucket.MHSAcovg = bucket.MHSAcovg[0:0]
	bucket.flushuint32("Netpay", bucket.Netpay)
	bucket.Netpay = bucket.Netpay[0:0]
	bucket.flushuint32("Svcscat", bucket.Svcscat)
	bucket.Svcscat = bucket.Svcscat[0:0]
	bucket.flushuint8("Region", bucket.Region)
	bucket.Region = bucket.Region[0:0]
	bucket.flushuint32("MSA", bucket.MSA)
	bucket.MSA = bucket.MSA[0:0]
	bucket.flushuint8("Wgtkey", bucket.Wgtkey)
	bucket.Wgtkey = bucket.Wgtkey[0:0]
	bucket.flushuint8("MSwgtkey", bucket.MSwgtkey)
	bucket.MSwgtkey = bucket.MSwgtkey[0:0]
	bucket.flushuint8("Egeoloc", bucket.Egeoloc)
	bucket.Egeoloc = bucket.Egeoloc[0:0]
	bucket.flushuint8("Emprel", bucket.Emprel)
	bucket.Emprel = bucket.Emprel[0:0]
	bucket.flushuint8("Rx", bucket.Rx)
	bucket.Rx = bucket.Rx[0:0]
	bucket.flushuint8("Sex", bucket.Sex)
	bucket.Sex = bucket.Sex[0:0]

	bucket.mut.Unlock()
}

// getcols fills a chunk with data from a SAS file.
func (c *chunk) getcols(data []*datareader.Series, cm map[string]int) error {

	var err error
	var ii int
	var ok bool

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

	}

	ii, ok = cm["DX4"]
	if ok {
		c.Dx4, c.Dx4m, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

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

	ii, ok = cm["STDPROV"]
	if ok {
		c.Stdprov, c.Stdprovm, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable STDPROV required but not found in SAS file\n")
		return fmt.Errorf(msg)
	}

	ii, ok = cm["MHSACOVG"]
	if ok {
		c.MHSAcovg, c.MHSAcovgm, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable MHSACOVG required but not found in SAS file\n")
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

	ii, ok = cm["SVCSCAT"]
	if ok {
		c.Svcscat, c.Svcscatm, err = data[ii].AsStringSlice()
		if err != nil {
			panic(err)
		}

	} else {
		msg := fmt.Sprintf("Variable SVCSCAT required but not found in SAS file\n")
		return fmt.Errorf(msg)
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

	ii, ok = cm["WGTKEY"]
	if ok {
		c.Wgtkey, c.Wgtkeym, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

	}

	ii, ok = cm["MSWGTKEY"]
	if ok {
		c.MSwgtkey, c.MSwgtkeym, err = data[ii].AsFloat64Slice()
		if err != nil {
			panic(err)
		}

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

	r.Enrolid = uint64(c.Enrolid[i])

	r.Dx1 = strings.TrimSpace(c.Dx1[i])

	r.Dx2 = strings.TrimSpace(c.Dx2[i])

	if c.Dx3 != nil {

		r.Dx3 = strings.TrimSpace(c.Dx3[i])

	}

	if c.Dx4 != nil {

		r.Dx4 = strings.TrimSpace(c.Dx4[i])

	}

	r.Proc1 = strings.TrimSpace(c.Proc1[i])

	r.Svcdate = uint16(c.Svcdate[i])

	r.Dobyr = uint16(c.Dobyr[i])

	r.Copay = uint32(c.Copay[i])

	r.Deduct = uint32(c.Deduct[i])

	r.Stdprov = uint16(c.Stdprov[i])

	// Convert string to number
	if len(c.MHSAcovg[i]) > 0 {
		x, err := strconv.Atoi(c.MHSAcovg[i])
		if err == nil {
			r.MHSAcovg = uint8(x)
		}
	}

	r.Netpay = uint32(c.Netpay[i])

	// Convert string to number
	if len(c.Svcscat[i]) > 0 {
		x, err := strconv.Atoi(c.Svcscat[i])
		if err == nil {
			r.Svcscat = uint32(x)
		}
	}

	// Convert string to number
	if len(c.Region[i]) > 0 {
		x, err := strconv.Atoi(c.Region[i])
		if err == nil {
			r.Region = uint8(x)
		}
	}

	r.MSA = uint32(c.MSA[i])

	if c.Wgtkey != nil {

		r.Wgtkey = uint8(c.Wgtkey[i])

	}

	if c.MSwgtkey != nil {

		r.MSwgtkey = uint8(c.MSwgtkey[i])

	}

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

	// Convert string to number
	if len(c.Rx[i]) > 0 {
		x, err := strconv.Atoi(c.Rx[i])
		if err == nil {
			r.Rx = uint8(x)
		}
	}

	// Convert string to number
	if len(c.Sex[i]) > 0 {
		x, err := strconv.Atoi(c.Sex[i])
		if err == nil {
			r.Sex = uint8(x)
		}
	}

	c.row++

	return r, true
}
