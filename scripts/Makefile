
BASE=${GOPATH}/src/github.com/kshedden/goclaims

B1=${BASE}/sastocols/sastocols
sastocols:
	cd ${B1}; go install

B2=${BASE}/factorize/factorizedx
factorizedx:
	cd ${B2}; go install

B3=${BASE}/sortbuckets/sortbuckets
sortbuckets:
	cd ${B3}; go install

B4=${BASE}/walk/owalk
owalk:
	cd ${B4}; go install


all: sastocols factorizedx sortbuckets owalk
