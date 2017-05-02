package main

import (
	"fmt"
	"log"
	"os"
	"path"

	"github.com/kshedden/goclaims/config"
	"github.com/kshedden/goclaims/sastocols/XXXXXXXX"
)

var (
	conf *config.Config

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

	XXXXXXXX.Run(conf, logger)

	logger.Printf("Finished, exiting")
}
