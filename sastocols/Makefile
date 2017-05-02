.PHONY: ofiles afiles ifiles ffiles sfiles

ofiles:
	go run gen.go o defs/odefs.json
	sed 's/XXXXXXXX/ofiles/g' procsas.go > ofiles/procsas.go
	sed 's/XXXXXXXX/ofiles/g' cmd.go > ofiles/osastocols/cmd.go
	cd ofiles; go install
	cd ofiles/osastocols; go install

ifiles:
	go run gen.go i defs/idefs.json
	goimports ifiles/generated_defs.go > ifiles/generated_defsx.go
	rm -f ifiles/generated_defs.go
	sed 's/XXXXXXXX/ifiles/g' procsas.go > ifiles/procsas.go
	sed 's/XXXXXXXX/ifiles/g' cmd.go > ifiles/isastocols/cmd.go
	cd ifiles; go install
	cd ifiles/isastocols; go install

ffiles:
	go run gen.go f defs/fdefs.json
	goimports ffiles/generated_defs.go > ffiles/generated_defsx.go
	rm -f ffiles/generated_defs.go
	sed 's/XXXXXXXX/ffiles/g' procsas.go > ffiles/procsas.go
	sed 's/XXXXXXXX/ffiles/g' cmd.go > ffiles/fsastocols/cmd.go
	cd ffiles; go install
	cd ffiles/fsastocols; go install

sfiles:
	go run gen.go s defs/sdefs.json
	goimports sfiles/generated_defs.go > sfiles/generated_defsx.go
	rm -f sfiles/generated_defs.go
	sed 's/XXXXXXXX/sfiles/g' procsas.go > sfiles/procsas.go
	sed 's/XXXXXXXX/sfiles/g' cmd.go > sfiles/ssastocols/cmd.go
	cd sfiles; go install
	cd sfiles/ssastocols; go install

afiles:
	go run gen.go a defs/adefs.json
	goimports afiles/generated_defs.go > afiles/generated_defsx.go
	rm -f afiles/generated_defs.go
	sed 's/XXXXXXXX/afiles/g' procsas.go > afiles/procsas.go
	sed 's/XXXXXXXX/afiles/g' cmd.go > afiles/asastocols/cmd.go
	cd afiles; go install
	cd afiles/asastocols; go install

all: ofiles afiles ifiles ffiles sfiles
