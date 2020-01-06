#!/bin/bash

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
package="pbmock"

which retool >/dev/null
if [ "$?" != 0 ]; then
    echo "'retool' does not exist, please run 'make retool_setup' first"
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
    ifs=$(grep -E "type [[:upper:]].*interface" $file|awk '{print $2}' 'ORS=,'|rev|cut -c 2-|rev)
    echo "generate mock for file $file"
    retool do mockgen -destination $cur/../dm/pbmock/$prefix.go -package $package github.com/pingcap/dm/dm/pb $ifs
done

echo "generate grpc mock code successfully"
