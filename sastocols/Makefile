.PHONY: afiles ffiles ifiles sfiles ofiles dfiles

afiles:
	go run gen.go defs/adefs.toml > asastocols/sastocols.go

ffiles:
	go run gen.go defs/fdefs.toml > fsastocols/sastocols.go

ifiles:
	go run gen.go defs/idefs.toml > isastocols/sastocols.go

sfiles:
	go run gen.go defs/sdefs.toml > ssastocols/sastocols.go

ofiles:
	go run gen.go defs/odefs.toml > osastocols/sastocols.go

dfiles:
	go run gen.go defs/ddefs.toml > dsastocols/sastocols.go

all: afiles ffiles ifiles sfiles ofiles dfiles
