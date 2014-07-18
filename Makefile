all:
	go install

deps:
	#go get launchpad.net/gommap

test:
	go test ./...

bench:
	go test ./... -bench .

vtest:
	go test ./... -v

vbench:
	go test ./... -bench . -v

.phony: deps all test
