// Code generation script for sastocols.  Takes a variable definition
// file in toml format and generates code for bucketing and
// columnizing a set of SAS files, each of which contains the
// variables as described in the variable definition file.
//
// To generate a go script, run this script as follows:
//
// go run generate_sastocols.go vdefs.toml > sastocols.go
//
// The resulting sastocols.go script is a Go script that can be run
// as follows:
//
// go run sastocols.go config.json
//
// where config.json is a configuration script as in
// github.com/kshedden/gosascols/config

package main

import (
	"bytes"
	"encoding/json"
	"go/format"
	"os"
	"strings"
	"text/template"

	"github.com/kshedden/gosascols/config"
)

const script = `
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
	"sync"
	"strconv"
	"strings"

	"github.com/kshedden/gosascols/config"
	"github.com/kshedden/datareader"
	"github.com/golang/snappy"
)

var (
    // Needed to avoid errrors if no other references are made to these packages.
    _ = strings.TrimSpace
	_ = strconv.Atoi

    conf *config.Config

	rslt_chan chan *rec

    // Later replace triple " with back ticks
    dtypes = """{{ .Dtypes }}"""

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

		binary.LittleEndian.PutUint64(buf, r.{{ .KeyVar }})
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
// recs with missing KeyVar values are skipped, so this may not
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
        NumBuckets uint32
		Compression string
		CodesDir string
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
    {{ range .NameType }}
        {{ .Name }} {{ .GoType }}
    {{- end }}
}

// Bucket is a memory-backed container for columnized data.  It
// contains data exactly as it will be written to disk.
type Bucket struct {
    BaseBucket

    code []uint16
    {{- range .NameType }}
        {{ .Name }} []{{ .GoType }}
    {{- end }}
}

// chunk is a typed container for data pulled directly out of a SAS file.
// There are no type conversions or other modifications from the SAS file.
type chunk struct {
    row int
    col int

    {{- range .NameType }}
        {{ .Name }} []{{ .SASType }}
        {{ .Name }}m []bool
    {{- end }}
}


// Add appends a rec to the end of the Bucket.
func (bucket *Bucket) Add(r *rec) {

	bucket.Mut.Lock()

    {{ range .NameType }}
        bucket.{{ .Name }} = append(bucket.{{ .Name }}, r.{{  .Name }})
    {{- end }}

	bucket.Mut.Unlock()

	if uint64(len(bucket.{{ .KeyVar }})) > conf.BufMaxRecs {
		bucket.Flush()
	}
}

// Flush writes all the data from the Bucket to disk.
func (bucket *Bucket) Flush() {

	logger.Printf("Flushing bucket %d", bucket.BucketNum)

	bucket.Mut.Lock()

        {{ range .NameType }}
	        bucket.flush{{ .GoType }}("{{ .Name }}", bucket.{{ .Name }})
	        bucket.{{ .Name }} = bucket.{{ .Name }}[0:0]
        {{- end }}

	bucket.Mut.Unlock()
}

// getcols fills a chunk with data from a SAS file.
func (c *chunk) getcols(data []*datareader.Series, cm map[string]int) error {

	var err error
    var ii int
    var ok bool
    {{ range .NameType }}
        ii, ok = cm["{{ .SASName }}"]
        if ok {
	        c.{{ .Name }}, c.{{ .Name }}m, err = data[ii].As{{ .SASTypeU }}Slice()
	        if err != nil {
		        panic(err)
	        }
        {{ if .Must }}
	        } else {
	                msg := fmt.Sprintf("Variable {{ .SASName }} required but not found in SAS file\n")
                        return fmt.Errorf(msg)
        {{ end }}
        }
    {{ end }}

    return nil
}

func (c *chunk) trynextrec() (*rec, bool) {

	if c.row >= len(c.{{ .KeyVar }}) {
		return nil, false
	}

	r := new(rec)

	i := c.row

    // Check if key variable is missing
	if c.{{ .KeyVar }}m[i] {
	        c.row++
	        return nil, true
    }

    {{ range .NameType }}
        {{ if not .Must }}
            if c.{{ .Name }} != nil {
        {{ end }}
        {{ if and (eq .SASType "string") (ne .GoType "string") }}
            // Convert string to number
            if len(c.{{ .Name }}[i]) > 0 {
                x, err := strconv.Atoi(c.{{ .Name }}[i])
		        if err == nil {
                    r.{{ .Name }} = {{ .GoType }}(x)
		        }
            }
        {{ else if eq .GoType "string" }}
            r.{{ .Name }} = strings.TrimSpace(c.{{ .Name }}[i])
        {{ else }}
            r.{{ .Name }} = {{ .GoType }}(c.{{ .Name }}[i])
        {{ end }}
        {{ if not .Must }}
            }
        {{ end }}
    {{- end }}

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

{{- range .Rtypes }}
    func (bucket *BaseBucket) flush{{ . }}(varname string, vec []{{ . }}) {

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
{{- end }}

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
`

// tvals contains values that are to be inserted into the code
// template.
type tvals struct {
	Rtypes   []string
	Dtypes   string
	NameType []*config.VarDesc
	KeyVar   string
}

// getdtypes returns a json encoded map describing the dtypes, based
// on the array of variable descriptions.
func getdtypes(nametype []*config.VarDesc) string {

	mp := make(map[string]string)

	for _, v := range nametype {
		mp[v.Name] = v.GoType
	}

	var bbuf bytes.Buffer
	enc := json.NewEncoder(&bbuf)
	err := enc.Encode(mp)
	if err != nil {
		panic(err)
	}

	return string(bbuf.Bytes())
}

func main() {

	if len(os.Args) != 2 {
		panic("wrong number of arguments")
	}

	vdesca := config.GetVarDefs(os.Args[1])

	tmpl, err := template.New("script").Parse(script)
	if err != nil {
		panic(err)
	}

	rtypes := []string{"uint8", "uint16", "uint32", "uint64", "float32", "float64"}

	tval := &tvals{
		Rtypes:   rtypes,
		NameType: vdesca,
		Dtypes:   getdtypes(vdesca),
	}

	// Set the key variable
	found := false
	for _, v := range vdesca {
		if v.KeyVar {
			tval.KeyVar = v.Name
			found = true
		}
	}
	if !found {
		panic("No key variable found")
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, tval)
	if err != nil {
		panic(err)
	}

	src := string(buf.Bytes())
	src = strings.Replace(src, "\"\"\"", "`", -1)

	p, err := format.Source([]byte(src))
	if err != nil {
		panic(err)
	}

	os.Stdout.WriteString("// GENERATED CODE, DO NOT EDIT\n")
	_, err = os.Stdout.Write(p)
	if err != nil {
		panic(err)
	}
}
