#!/usr/bin/env bash
set -euo pipefail

export GO111MODULE=off
go get github.com/twitchtv/retool

# go failpoint, it better to match the version of `github.com/pingcap/failpoint` in go.mod.
retool add github.com/pingcap/failpoint/failpoint-ctl e7b1061e6e81
