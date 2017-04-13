/*
Generic methods used by all buckets.
*/

package sastocols

import (
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
)

type BaseBucket struct {
	BucketNum uint32

	mut sync.Mutex
}

func (bucket *BaseBucket) openfile(varname string) (io.Closer, io.WriteCloser) {

	vnl := strings.ToLower(varname)
	bns := fmt.Sprintf("%04d", bucket.BucketNum)
	fn := path.Join(conf.TargetDir, "Buckets", bns, vnl+".bin.gz")
	fid, err := os.OpenFile(fn, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}

	gid := gzip.NewWriter(fid)

	return fid, gid
}

func (bucket *BaseBucket) flushbyte(varname string, vec []byte) {

	toclose, wtr := bucket.openfile(varname)

	for _, x := range vec {
		err := binary.Write(wtr, binary.LittleEndian, x)
		if err != nil {
			panic(err)
		}
	}

	wtr.Close()
	toclose.Close()
}

func (bucket *BaseBucket) flushstring(varname string, vec []string) {

	toclose, wtr := bucket.openfile(varname)

	for _, x := range vec {
		_, err := wtr.Write([]byte(x + "\n"))
		if err != nil {
			panic(err)
		}
	}

	wtr.Close()
	toclose.Close()
}

func (bucket *BaseBucket) flushuint8(varname string, vec []uint8) {

	toclose, wtr := bucket.openfile(varname)

	for _, x := range vec {
		err := binary.Write(wtr, binary.LittleEndian, x)
		if err != nil {
			panic(err)
		}
	}

	wtr.Close()
	toclose.Close()
}

func (bucket *BaseBucket) flushuint16(varname string, vec []uint16) {

	toclose, wtr := bucket.openfile(varname)

	for _, x := range vec {
		err := binary.Write(wtr, binary.LittleEndian, x)
		if err != nil {
			panic(err)
		}
	}

	wtr.Close()
	toclose.Close()
}

func (bucket *BaseBucket) flushuint32(varname string, vec []uint32) {

	toclose, wtr := bucket.openfile(varname)

	for _, x := range vec {
		err := binary.Write(wtr, binary.LittleEndian, x)
		if err != nil {
			panic(err)
		}
	}

	wtr.Close()
	toclose.Close()
}

func (bucket *BaseBucket) flushuint64(varname string, vec []uint64) {

	toclose, wtr := bucket.openfile(varname)

	for _, x := range vec {
		err := binary.Write(wtr, binary.LittleEndian, x)
		if err != nil {
			panic(err)
		}
	}

	wtr.Close()
	toclose.Close()
}

func (bucket *BaseBucket) flushfloat64(varname string, vec []float64) {

	toclose, wtr := bucket.openfile(varname)

	for _, x := range vec {
		err := binary.Write(wtr, binary.LittleEndian, x)
		if err != nil {
			panic(err)
		}
	}

	wtr.Close()
	toclose.Close()
}
