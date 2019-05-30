#!/bin/bash

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
package="pbmock"

which mockgen >/dev/null
if [ "$?" != 0 ]; then
    echo "please install mockgen first"
    exit 1
fi

echo "generate grpc mock code..."

cd $cur/../dm/pb
for file in ./*; do
    prefix=$(echo $file | awk -F"/" '{print $(NF)}'| awk -F"." '{print $1}')
    if [[ "$prefix" =~ ^tracer.* ]]; then
        # NOTE: have problem processing tracer related pb file, fix it later
        continue
    fi
    echo "generate mock for file $file"
    mockgen -source $file -destination $cur/../dm/pbmock/$prefix.go -package $package
done

echo "generate grpc mock code successfully"
