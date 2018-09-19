package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"unicode"

	"github.com/kshedden/gosascols/config"
	"github.com/kshedden/gosascols/factorize"
)

var (
	conf []*config.Config

	// If multi=true, all variables named {prefix}# are unified
	// into a single variable of uvarint type.  Otherwise, only
	// the variable named prefix is factorized and converted to
	// uvarint.
	multi bool

	logger *log.Logger
)

func getfilenames(tp string) ([]string, map[string][]string) {
	var files []string
	vnames := make(map[string][]string)
	for _, cnf := range conf {
		for k := 0; k < int(cnf.NumBuckets); k++ {
			px := config.BucketPath(k, cnf)
			fl, err := ioutil.ReadDir(px)
			if err != nil {
				panic(err)
			}
			for _, f := range fl {
				fn := f.Name()
				if !strings.HasSuffix(fn, ".bin.sz") {
					// Not a data column
					continue
				}

				if strings.HasSuffix(fn, "_string.bin.sz") {
					// This is the backup copy of the original text data
					continue
				}

				if !strings.HasPrefix(fn, tp) {
					// Not a matching variable
					continue
				}

				if multi && !(len(fn) > len(tp) && unicode.IsDigit(rune(fn[len(tp)]))) {
					// In multi mode, file names must have the form {prefix}#.
					continue
				}

				vname := strings.Replace(fn, ".bin.sz", "", 1)
				vnames[cnf.TargetDir] = append(vnames[cnf.TargetDir], vname)
				files = append(files, path.Join(px, fn))
			}
		}
	}

	return files, vnames
}

func setupLogger(prefix string) {
	fid, err := os.Create(fmt.Sprintf("factorize_%s.log", prefix))
	if err != nil {
		panic(err)
	}
	logger = log.New(fid, "", log.Ltime)
}

// revert undoes all factorizations in the directories containing thre
// provided files.  Note that the reversion applies to all factorized
// files, not only the files with the given prefix type.
func revert(files []string) {

	print("reverting all factorized files with all prefixes...")

	dm := make(map[string]bool)
	for _, f := range files {
		d, _ := path.Split(f)
		dm[d] = true
	}

	for d, _ := range dm {
		files, err := ioutil.ReadDir(d)
		if err != nil {
			panic(err)
		}
		for _, file := range files {
			fn := file.Name()
			if strings.HasSuffix(fn, "_string.bin.sz") || strings.HasSuffix(fn, "_string.json") {
				nn := strings.Replace(fn, "_string", "", 1)
				px1 := path.Join(d, fn)
				px2 := path.Join(d, nn)
				print(px1, " -> ", px2, "\n")
				err := os.Rename(px1, px2)
				if err != nil {
					panic(err)
				}
			}
		}
	}
}

func main() {

	if len(os.Args) < 2 {
		os.Stderr.WriteString("factorize: wrong number of arguments\n\n")
		os.Stderr.WriteString("Usage:\n  factorize (run|revert) prefix config...\n")
		os.Exit(1)
	}

	prefix := os.Args[2]

	if strings.HasSuffix(prefix, "*") {
		multi = true
		prefix = prefix[0 : len(prefix)-1]
	}

	setupLogger(prefix)

	for _, f := range os.Args[3:len(os.Args)] {
		c := config.ReadConfig(f)
		conf = append(conf, c)
		logger.Printf("Read config file from %s", f)
	}

	files, vninfo := getfilenames(prefix)

	if strings.ToLower(os.Args[1]) == "revert" {
		logger.Printf("Reverting to string values")
		revert(files)
		logger.Printf("Done reverting")
		os.Exit(0)
	}

	logger.Printf("Processing %d files with prefix %s + digit", len(files), prefix)

	codefile := prefix + "Codes.json"
	codefile = path.Join(conf[0].CodesDir, codefile)
	os.MkdirAll(conf[0].CodesDir, 0755)

	factorize.Run(files, codefile, prefix, vninfo, logger)
}
