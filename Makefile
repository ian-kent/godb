all:
	go install

deps:
	#go get launchpad.net/gommap

test:
	go test ./...

.phony: deps all test
