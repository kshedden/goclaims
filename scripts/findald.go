package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/kshedden/gosascols/config"
	"github.com/kshedden/statmodel/dataprovider"
)

const (
	// Chunk size for reading raw data
	csize = 1000000

	// Number of weight strata
	nstrat = 33

	// Exclude a person for one year if they do not have this many
	// days of coverage in the year
	mincoverage = 360

	// Process this number of buckets in parallel
	concurrency = 10
)

type rec struct {
	numer     [][]int
	denom     [][]int
	cirrhosis []crec
}

type crec struct {
	enrolid uint64
	nclaim  []int
}

var (
	// If true, only count ALD people, otherwise count all cirrhotics
	aldonly bool

	// If true, after meeting cirrhosis/ALD criteria, the person
	// is treated as having cirrhosis/ALD in all subsequent years,
	// regardless of whether additional codes are present.
	carryforward bool

	dix []string

	dxcodes map[string]int

	// icodes[0] contains integer codes for cirrhosis
	// icodes[1] contains integer codes for cirrhosis complications
	// icodes[2] contains integer codes for ALD
	icodes [][]uint64

	td []uint16
	tl uint16
	tm uint16

	logger *log.Logger

	rslt chan rec
	ck   chan bool

	// Configuration files, supplied by caller
	aconf *config.Config
	oconf *config.Config
	sconf *config.Config
	iconf *config.Config
	fconf *config.Config

	sem chan bool

	ALD2Codes = []string{"57120", "79030", "42550", "53530",
		"53531", "V1130", "35750", "29100", "29110", "29120",
		"29130", "29140", "29150", "29181", "29182", "29189",
		"29190", "30300", "30301", "30302", "30303", "30390",
		"30391", "30392", "30393", "98000", "98010", "98020",
		"98030", "98080", "98090", "E8600", "E8601", "E8602",
		"E8603", "E8604", "30500", "30501", "30502", "30503",
		"30510", "30520", "30521", "30522", "30523", "30530",
		"30531", "30532", "30533", "30540", "30541", "30542",
		"30543", "30550", "30551", "30552", "30553", "30560",
		"30561", "30562", "30563", "30570", "30571", "30572",
		"30573", "30580", "30581", "30582", "30583", "30590",
		"30591", "30592", "30593", "F10159", "F1014",
		"F10239", "F10921", "F10959", "F10221", "F10182",
		"F10259", "F10280", "F10250", "F1097", "F10229",
		"F1029", "F10129", "F10120", "F10150", "F10982",
		"F10281", "F10231", "F10230", "F1021", "F1027",
		"F10981", "F10980", "F10951", "F1026", "F10950",
		"F10188", "F10251", "F1094", "F10151", "F10181",
		"F10920", "F10288", "F10282", "F1024", "F1099",
		"F10121", "F10232", "F10988", "F10220", "F1010",
		"F1020", "F10180", "F10929", "F1096", "F1019",
		"K7010", "K7011", "K7041", "K7030", "K7020", "K7000",
		"K7090", "K7031", "K7040"}

	CirrhosisCodes = []string{"57120", "57150", "K7460", "K7469", "K7030", "K7031"}

	CirrhosisComorbidCodes = []string{"57240", "57220", "45600",
		"45620", "45610", "45621", "56723", "78959", "57230",
		"K7670", "K7291", "I8501", "I8511", "K6520", "R1880",
		"K7151", "K7660"}
)

func matchset(x []uint64, y uint64) bool {
	for _, v := range x {
		if y == v {
			return true
		}
	}
	return false
}

func getwgtkey(adobyr uint16, asex uint8, aemprel uint8, aregion uint8, amsa uint32) int {

	// age (0=younger, 1=older)
	var age int
	switch {
	case adobyr < 2009-65:
		return 0 // too old
	case adobyr < 2012-45:
		age = 1
	case adobyr < 2012-18:
		age = 0
	default:
		return 0 // too young
	}

	// sex (0=male, 1=female)
	if asex != 1 && asex != 2 {
		print("sex??\n")
		return 0
	}
	sex := int(asex) - 1

	// region (0=NE, 1=NC, 2=S, 3=W)
	region := int(aregion) - 1
	if region < 0 || region > 3 {
		return 0 // missing region
	}

	// emprel (0=policy holder, 1=dependent)
	emprel := int(aemprel)
	if emprel == 4 {
		return 0 // missing emprel
	}
	if emprel >= 2 {
		emprel = 2
	}
	emprel -= 1

	// msa (0=no msa, 1=msa)
	//var msa int
	//if amsa > 0 {
	//		msa = 1
	//	}

	//return 1 + sex + 2*emprel + 4*msa + 8*age + 16*region
	return 1 + sex + 2*emprel + 4*age + 8*region
}

func dobucket(k int) {

	numer := make([][]int, nstrat)
	denom := make([][]int, nstrat)
	for s := 0; s < nstrat; s++ {
		numer[s] = make([]int, len(td))
		denom[s] = make([]int, len(td))
	}
	numer1 := make([]int, len(td))
	denom1 := make([]int, len(td))

	var cirrhosis []crec

	abp := config.BucketPath(k, aconf)
	obp := config.BucketPath(k, oconf)
	sbp := config.BucketPath(k, sconf)
	ibp := config.BucketPath(k, iconf)
	fbp := config.BucketPath(k, fconf)

	include := []string{"Enrolid", "Year", "Memdays", "Dobyr", "MSA", "Region", "Emprel", "Sex"}
	adata := dataprovider.NewBCols(abp, csize, include, nil)
	adata = dataprovider.Segment(adata, []string{"Enrolid"})

	include = []string{"Enrolid", "Svcdate", "Dx1", "Dx2", "Dx3", "Dx4"}
	odata := dataprovider.NewBCols(obp, csize, include, nil)
	odata = dataprovider.Segment(odata, []string{"Enrolid"})

	include = []string{"Enrolid", "Svcdate", "Dx1", "Dx2"}
	sdata := dataprovider.NewBCols(sbp, csize, include, nil)
	sdata = dataprovider.Segment(sdata, []string{"Enrolid"})

	include = []string{"Enrolid", "Admdate"}
	for j := 1; j <= 15; j++ {
		include = append(include, fmt.Sprintf("Dx%d", j))
	}
	idata := dataprovider.NewBCols(ibp, csize, include, nil)
	idata = dataprovider.Segment(idata, []string{"Enrolid"})

	include = []string{"Enrolid", "Svcdate"}
	for j := 1; j <= 9; j++ {
		include = append(include, fmt.Sprintf("Dx%d", j))
	}
	fdata := dataprovider.NewBCols(fbp, csize, include, nil)
	fdata = dataprovider.Segment(fdata, []string{"Enrolid"})

	data := []dataprovider.Data{adata, odata, sdata, idata, fdata}

	var vpos []map[string]int
	for j, _ := range dix {
		vpos = append(vpos, dataprovider.VarPos(data[j]))
	}

	// Positions of the A table variables we need.
	amemdayspos := vpos[0]["Memdays"]
	ayearpos := vpos[0]["Year"]
	aenrolidpos := vpos[0]["Enrolid"]
	adobyrpos := vpos[0]["Dobyr"]
	asexpos := vpos[0]["Sex"]
	aemprelpos := vpos[0]["Emprel"]
	aregionpos := vpos[0]["Region"]
	amsapos := vpos[0]["MSA"]

	// Positions of all diagnosis code variables in each file
	var dpx [][]int
	for j, _ := range dix {
		var u []int
		for k, v := range vpos[j] {
			if strings.HasPrefix(k, "Dx") {
				u = append(u, v)
			}
		}
		dpx = append(dpx, u)
	}

	datepos := []int{vpos[0]["Year"], vpos[1]["Svcdate"], vpos[2]["Svcdate"], vpos[3]["Admdate"], vpos[4]["Svcdate"]}
	wk := dataprovider.NewWalk(data, []string{"Enrolid", "Enrolid", "Enrolid", "Enrolid", "Enrolid"})

	//TEMP
	var nx1 [7]int
	var nx2 int

	for wk.Next() {

		// Update denominator with everyone who is eligible
		amemdays := adata.Get(amemdayspos).([]uint16)
		ayear := adata.Get(ayearpos).([]uint16)
		aenrolid := adata.Get(aenrolidpos).([]uint64)

		// Variables used to define the weight strata
		adobyr := adata.Get(adobyrpos).([]uint16)
		asex := adata.Get(asexpos).([]uint8)
		aemprel := adata.Get(aemprelpos).([]uint8)
		aregion := adata.Get(aregionpos).([]uint8)
		amsa := adata.Get(amsapos).([]uint32)

		wgtkey := getwgtkey(adobyr[0], asex[0], aemprel[0], aregion[0], amsa[0])

		zero(denom1)
		for i, y := range ayear {
			age := int(y) - int(adobyr[i])
			if age <= 18 || age >= 65 {
				continue
			}

			if amemdays[i] >= mincoverage {
				denom1[y-2009] = 1
			}
		}

		for k, v := range denom1 {
			denom[wgtkey][k] += v
		}

		// The numerator contains indicators of when the person had cirrhosis
		zero(numer1)

		// First check for the main cirrhosis codes to see if this is a case
		dx := false
		ald := false
		ccnt := make([]int, len(dix)-1)
		for j, vb := range dix {

			if vb == "A" || !wk.Status[j] {
				continue
			}

			// The time values
			sd := data[j].Get(datepos[j]).([]uint16)

			// Loop over the Dx variables
			for _, k := range dpx[j] {

				// Loop through time
				x := data[j].Get(k).([]uint64)
				for i, y := range x {

					yr := int((sd[i] - tm) / 365) // 0 = 2009
					age := 2009 + int(yr) - int(adobyr[0])
					if age <= 18 || age >= 65 {
						continue
					}

					// Check for cirrhosis
					if matchset(icodes[0], y) {
						dx = true

						if carryforward {
							for j := yr; j < len(numer1); j++ {
								numer1[j] = denom1[j]
							}
						} else {
							if yr >= 0 && yr < len(numer1) {
								numer1[yr] = denom1[yr]
							}
						}

						fl := false
						for _, x := range numer1 {
							if x > 0 {
								fl = true
							}
						}
						if fl {
							ccnt[j-1]++
						}

					}

					// Check for ALD
					if aldonly {
						ald = ald || matchset(icodes[2], y)
					}
				}
			}
		}

		if !dx {
			// Not a cirrhosis case
			continue
		}
		if aldonly && !ald {
			// Not an ALD case
			continue
		}

		cirrhosis = append(cirrhosis, crec{aenrolid[0], ccnt})

		// Check the cirrhosis comorbidity codes to possibly backdate the diagosis
		for j, vb := range dix {

			if vb == "A" || !wk.Status[j] {
				continue
			}
			sd := data[j].Get(datepos[j]).([]uint16)

			// Loop over the Dx variables
			for _, k := range dpx[j] {

				// Loop through time
				x := data[j].Get(k).([]uint64)

				for i, y := range x {
					if matchset(icodes[1], y) {
						yr := int((sd[i] - tm) / 365)
						if carryforward {
							for j := yr; j < len(numer1); j++ {
								numer1[j] = denom1[j]
							}
						} else {
							if yr >= 0 && yr < len(numer1) {
								numer1[yr] = denom1[yr]
							}
						}
					}
				}
			}
		}

		//TEMP
		if denom1[len(denom1)-1] == 1 {
			nx2++
			for j := 0; j < 7; j++ {
				for i := j; i < 7; i++ {
					if numer1[i] == 1 {
						nx1[j]++
						break
					}
				}
			}
		}

		for k, v := range numer1 {
			numer[wgtkey][k] += int(v)
		}
	}

	rslt <- rec{
		numer:     numer,
		denom:     denom,
		cirrhosis: cirrhosis,
	}
	<-sem

	fmt.Printf("%v\n", nx1)
	fmt.Printf("%v\n", nx2)
}

func zero(x []int) {
	for j, _ := range x {
		x[j] = 0
	}
}

func setuplog() {
	fid, err := os.Create("findald.log")
	if err != nil {
		panic(err)
	}
	logger = log.New(fid, "", log.Ltime)
}

// Get the codes for cirrhosis/complications in each file type
func getcodes() {

	// All the *conf files point to the same codes
	dxcodes = config.ReadFactorCodes("DxCodes", oconf)

	for k, v := range [][]string{CirrhosisCodes, CirrhosisComorbidCodes, ALD2Codes} {
		for _, x := range v {
			ky, ok := dxcodes[x]
			if !ok {
				fmt.Printf("Can't find code %s\n", x)
				os.Exit(1)
			}
			icodes[k] = append(icodes[k], uint64(ky))
		}
	}

	logger.Printf("Cirrhosis codes: %v\n", icodes[0])
	logger.Printf("Complication codes: %v\n", icodes[1])
	logger.Printf("ALD codes: %v\n", icodes[2])
}

func harvest() {

	// Per-stratum totals
	num := make([][]int, nstrat)
	den := make([][]int, nstrat)
	for k := 0; k < nstrat; k++ {
		num[k] = make([]int, len(td))
		den[k] = make([]int, len(td))
	}

	cx := make(map[uint64][]int)

	// Collect data from all buckets
	for r := range rslt {
		for s := 0; s < nstrat; s++ {
			for k := 0; k < len(td); k++ {
				num[s][k] += r.numer[s][k]
				den[s][k] += r.denom[s][k]
			}
		}

		for _, x := range r.cirrhosis {
			cx[x.enrolid] = x.nclaim
		}
	}

	// Save the cirrhosis ids
	fid, err := os.Create("cirrhosis_ids.txt")
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	for id, c := range cx {
		m := []string{fmt.Sprintf("%d", id)}
		for _, x := range c {
			m = append(m, fmt.Sprintf("%d", x))
		}
		_, err := fid.Write([]byte(strings.Join(m, ",") + "\n"))
		if err != nil {
			panic(err)
		}
	}

	// Save the case/noncase results
	var fname string
	if aldonly {
		fname = "ald"
	} else {
		fname = "cir"
	}
	if carryforward {
		fname += "_cf"
	}
	fname += ".csv"

	fid, err = os.Create(fname)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	for s := 0; s < nstrat; s++ {
		for k := 0; k < len(td); k++ {
			m := fmt.Sprintf("%d,%d,", num[s][k], den[s][k])
			_, err := fid.Write([]byte(m))
			if err != nil {
				panic(err)
			}
		}
		_, err := fid.Write([]byte("\n"))
		if err != nil {
			panic(err)
		}
	}
	<-ck
}

func main() {

	flag.BoolVar(&aldonly, "aldonly", true, "Only count ALD subjects")
	flag.BoolVar(&carryforward, "carryforward", true, "Carry diagnosis forward to future years")
	flag.Parse()
	arg := flag.Args()

	if len(arg) != 5 {
		os.Stderr.WriteString("Usage:\nfindald aconfig oconfig sconfig iconfig fconfig\n")
		os.Exit(1)
	}

	icodes = make([][]uint64, 3)

	setuplog()

	aconf = config.ReadConfig(arg[0])
	oconf = config.ReadConfig(arg[1])
	sconf = config.ReadConfig(arg[2])
	iconf = config.ReadConfig(arg[3])
	fconf = config.ReadConfig(arg[4])

	dix = []string{"A", "O", "S", "I", "F"}

	sem = make(chan bool, concurrency)
	rslt = make(chan rec, 200)

	// Elapsed days from 1-1-1960.
	d0 := time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC)
	for y := 2009; y <= 2015; y++ {
		da := time.Date(y, 1, 1, 0, 0, 0, 0, time.UTC)
		dr := da.Sub(d0)
		td = append(td, uint16(dr.Hours()/24))
	}
	tl = td[1] - td[0]
	tm = td[0]

	getcodes()

	ck = make(chan bool, 1)

	ck <- true
	go harvest()

	for k := 0; k < int(oconf.NumBuckets); k++ {
		//for k := 0; k < 2; k++ {
		sem <- true
		go dobucket(k)
	}

	for k := 0; k < concurrency; k++ {
		sem <- true
	}

	close(rslt)
	ck <- true
}
