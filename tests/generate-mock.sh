#!/bin/bash

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
package="pbmock"

which mockgen >/dev/null
if [ "$?" != 0 ]; then
    echo "please install mockgen first, simplify run \`go get github.com/golang/mock/mockgen\`"
    exit 1
fi

echo "generate grpc mock code..."

cd $cur/../dm/pb
for file in ./*pb.go; do
    prefix=$(echo $file | awk -F"/" '{print $(NF)}'| awk -F"." '{print $1}')
    if [[ "$prefix" =~ ^tracer.* ]]; then
        # NOTE: have problem processing tracer related pb file, fix it later
        continue
    fi
    # extract public interface from pb source file
    ifs=$(grep -E "type [[:upper:]].*interface" $file|awk '{print $2}' 'ORS=,'|head -c -1)
    echo "generate mock for file $file"
    mockgen -destination $cur/../dm/pbmock/$prefix.go -package $package github.com/pingcap/dm/dm/pb $ifs
done

echo "generate grpc mock code successfully"
