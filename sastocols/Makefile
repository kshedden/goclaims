.PHONY: afiles ffiles ifiles sfiles ofiles

afiles:
	go run gen.go defs/adefs.toml > afiles/sastocols.go

ffiles:
	go run gen.go defs/fdefs.toml > ffiles/sastocols.go

ifiles:
	go run gen.go defs/idefs.toml > ifiles/sastocols.go

sfiles:
	go run gen.go defs/sdefs.toml > sfiles/sastocols.go

ofiles:
	go run gen.go defs/odefs.toml > ofiles/sastocols.go

all: afiles ffiles ifiles sfiles ofiles
