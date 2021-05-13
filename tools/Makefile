all: failpoint-ctl gocovmerge goveralls golangci-lint mockgen protoc-gen-gogofaster protoc-gen-grpc-gateway statik gofumports shfmt

failpoint-ctl:
	go build -o bin/$@ github.com/pingcap/failpoint/failpoint-ctl

gocovmerge:
	go build -o bin/$@ github.com/zhouqiang-cl/gocovmerge

goveralls:
	go build -o bin/$@ github.com/mattn/goveralls

golangci-lint:
	go build -o bin/$@ github.com/golangci/golangci-lint/cmd/golangci-lint

mockgen:
	go build -o bin/$@ github.com/golang/mock/mockgen

protoc-gen-gogofaster:
	go build -o bin/$@ github.com/gogo/protobuf/protoc-gen-gogofaster

protoc-gen-grpc-gateway:
	go build -o bin/$@ github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway

statik:
	go build -o bin/$@ github.com/rakyll/statik

gofumports:
	go build -o bin/$@ mvdan.cc/gofumpt/gofumports

shfmt:
	go build -o bin/$@ mvdan.cc/sh/v3/cmd/shfmt
