#!/bin/bash

set -eu

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

trap "cd $PWD" EXIT
GO111MODULE=on go mod tidy
git diff --exit-code -- go.mod go.sum
