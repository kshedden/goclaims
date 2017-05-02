.PHONY: afiles ffiles ifiles sfiles ofiles

afiles:
	go run gen.go defs/adefs.json > afiles/sastocols.go

ffiles:
	go run gen.go defs/fdefs.json > ffiles/sastocols.go

ifiles:
	go run gen.go defs/idefs.json > ifiles/sastocols.go

sfiles:
	go run gen.go defs/sdefs.json > sfiles/sastocols.go

ofiles:
	go run gen.go defs/odefs.json > ofiles/sastocols.go

all: afiles ffiles ifiles sfiles ofiles
