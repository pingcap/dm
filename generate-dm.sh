#!/usr/bin/env bash

cd dm/proto

echo "generate dm protobuf code..."
GOGO_ROOT=/Users/imtbkcat/go/src/github.com/gogo/protobuf
if [ ! -d $GOGO_ROOT ]; then
		echo "please use the following command to get gogo."
		echo "go get -u github.com/gogo/protobuf/protoc-gen-gogofaster"
		exit 1
fi

which protoc-gen-grpc-gateway
if [ $? -ne 0 ]; then
	echo "please use the following command to get grpc-gateway."
	echo "go get -u github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway"
	exit 1
fi

GOOGLE_API_ROOT=/Users/imtbkcat/go/src/github.com/googleapis/googleapis
if [ ! -d $GOOGLE_API_ROOT ]; then
	echo "please use the following command to get google apis."
	echo "go get -u github.com/googleapis/googleapis/google/api"
	exit 1
fi

cp -r /Users/imtbkcat/go/src/github.com/googleapis/googleapis/google ./

protoc -I.:${GOGO_ROOT}:${GOGO_ROOT}/protobuf --gogofaster_out=plugins=grpc:../pb/ *.proto

protoc -I.:${GOGO_ROOT}:${GOGO_ROOT}/protobuf --grpc-gateway_out=logtostderr=true:../pb/ dmmaster.proto

rm -r ./google

cd ../pb
sed -i.bak -E 's/import _ \"gogoproto\"//g' *.pb.go
sed -i.bak -E 's/import fmt \"fmt\"//g' *.pb.go
rm -f *.bak
goimports -w *.pb.go
