LDFLAGS += -X "github.com/pingcap/dm/pkg/utils.ReleaseVersion=$(shell git describe --tags --dirty="-dev")"
LDFLAGS += -X "github.com/pingcap/dm/pkg/utils.BuildTS=$(shell date -u '+%Y-%m-%d %H:%M:%S')"
LDFLAGS += -X "github.com/pingcap/dm/pkg/utils.GitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "github.com/pingcap/dm/pkg/utils.GitBranch=$(shell git rev-parse --abbrev-ref HEAD)"
LDFLAGS += -X "github.com/pingcap/dm/pkg/utils.GoVersion=$(shell go version)"


CURDIR   := $(shell pwd)
GO       := GO111MODULE=on go
GOBUILD  := CGO_ENABLED=0 $(GO) build
GOTEST   := CGO_ENABLED=1 $(GO) test
PACKAGES := $$(go list ./... | grep -vE 'vendor|cmd|tests')
FILES    := $$(find . -name "*.go" | grep -vE "vendor")
TOPDIRS  := $$(ls -d */ | grep -vE "vendor")
SHELL    := /usr/bin/env bash
TEST_DIR := /tmp/dm_test
GOFAIL_DIR := $$(for p in $(PACKAGES); do echo $${p\#"github.com/pingcap/dm/"}; done)

RACE_FLAG =
ifeq ("$(WITH_RACE)", "1")
	RACE_FLAG = -race
	GOBUILD   = CGO_ENABLED=1 $(GO) build
endif

GOFAIL_ENABLE  := $$(echo $(GOFAIL_DIR) | xargs gofail enable)
GOFAIL_DISABLE := $$(find $(GOFAIL_DIR) | xargs gofail disable)

ARCH      := "`uname -s`"
LINUX     := "Linux"
MAC       := "Darwin"

.PHONY: build test dm_integration_test_build integration_test coverage check \
	dm-worker dm-master dmctl

build: check test dm-worker dm-master dmctl

dm-worker:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/dm-worker ./cmd/dm-worker

dm-master:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/dm-master ./cmd/dm-master

dmctl:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/dmctl ./cmd/dm-ctl

test:
	bash -x ./tests/wait_for_mysql.sh
	mkdir -p $(TEST_DIR)
	@export log_level=error; \
	$(GOTEST) -covermode=atomic -coverprofile="$(TEST_DIR)/cov.unit_test.out" -race $(PACKAGES)

check: fmt lint vet

fmt:
	@echo "gofmt (simplify)"
	@ gofmt -s -l -w $(FILES) 2>&1 | awk '{print} END{if(NR>0) {exit 1}}'

errcheck:
	GO111MODULE=off go get github.com/kisielk/errcheck
	@echo "errcheck"
	@ errcheck -blank $(PACKAGES) | grep -v "_test\.go" | awk '{print} END{if(NR>0) {exit 1}}'

lint:
	GO111MODULE=off go get golang.org/x/lint/golint
	@echo "golint"
	@ golint -set_exit_status $(PACKAGES)

vet:
	@echo "vet"
	@ $(GO) tool vet -all -shadow $(TOPDIRS) 2>&1 | tee /dev/stderr | awk '/shadows declaration/{next}{count+=1} END{if(count>0) {exit 1}}'

dm_integration_test_build:
	GO111MODULE=off go get github.com/pingcap/gofail
	$(GOFAIL_ENABLE)
	$(GOTEST) -c -race -cover -covermode=atomic \
		-coverpkg=github.com/pingcap/dm/... \
		-o bin/dm-worker.test github.com/pingcap/dm/cmd/dm-worker
	$(GOTEST) -c -race -cover -covermode=atomic \
		-coverpkg=github.com/pingcap/dm/... \
		-o bin/dm-master.test github.com/pingcap/dm/cmd/dm-master
	$(GOFAIL_DISABLE)

integration_test:
	@which bin/tidb-server
	@which bin/sync_diff_inspector
	@which bin/mydumper
	@which bin/dm-master.test
	@which bin/dm-worker.test
	tests/run.sh

coverage:
	GO111MODULE=off go get github.com/wadey/gocovmerge
	gocovmerge "$(TEST_DIR)"/cov.* > "$(TEST_DIR)/all_cov.out"
	go tool cover -html "$(TEST_DIR)/all_cov.out" -o "$(TEST_DIR)/all_cov.html"
