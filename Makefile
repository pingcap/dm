LDFLAGS += -X "github.com/pingcap/dm/pkg/utils.ReleaseVersion=$(shell git describe --tags --dirty="-dev")"
LDFLAGS += -X "github.com/pingcap/dm/pkg/utils.BuildTS=$(shell date -u '+%Y-%m-%d %H:%M:%S')"
LDFLAGS += -X "github.com/pingcap/dm/pkg/utils.GitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "github.com/pingcap/dm/pkg/utils.GitBranch=$(shell git rev-parse --abbrev-ref HEAD)"
LDFLAGS += -X "github.com/pingcap/dm/pkg/utils.GoVersion=$(shell go version)"

CURDIR   := $(shell pwd)
GO       := GO111MODULE=on go
GOBUILD  := CGO_ENABLED=0 $(GO) build
GOTEST   := CGO_ENABLED=1 $(GO) test
PACKAGES  := $$(go list ./... | grep -vE 'tests|cmd|vendor|pbmock|_tools')
PACKAGES_RELAY := $$(go list ./... | grep 'github.com/pingcap/dm/relay')
PACKAGES_SYNCER := $$(go list ./... | grep 'github.com/pingcap/dm/syncer')
PACKAGES_PKG_BINLOG := $$(go list ./... | grep 'github.com/pingcap/dm/pkg/binlog')
PACKAGES_OTHERS  := $$(go list ./... | grep -vE 'tests|cmd|vendor|pbmock|_tools|github.com/pingcap/dm/relay|github.com/pingcap/dm/syncer|github.com/pingcap/dm/pkg/binlog')
FILES    := $$(find . -name "*.go" | grep -vE "vendor|_tools")
TOPDIRS  := $$(ls -d */ | grep -vE "vendor|_tools")
SHELL    := /usr/bin/env bash
TEST_DIR := /tmp/dm_test
FAILPOINT_DIR := $$(for p in $(PACKAGES); do echo $${p\#"github.com/pingcap/dm/"}; done)
FAILPOINT := retool do failpoint-ctl

RACE_FLAG =
TEST_RACE_FLAG = -race
ifeq ("$(WITH_RACE)", "1")
	RACE_FLAG = -race
	GOBUILD   = CGO_ENABLED=1 $(GO) build
endif
ifeq ("$(WITH_RACE)", "0")
	TEST_RACE_FLAG =
	GOTEST         = CGO_ENABLED=0 $(GO) test
endif

FAILPOINT_ENABLE  := $$(echo $(FAILPOINT_DIR) | xargs $(FAILPOINT) enable >/dev/null)
FAILPOINT_DISABLE := $$(find $(FAILPOINT_DIR) | xargs $(FAILPOINT) disable >/dev/null)

ARCH      := "$(shell uname -s)"
LINUX     := "Linux"
MAC       := "Darwin"

ifeq ($(ARCH), $(LINUX))
	LDFLAGS += -X "github.com/pingcap/dm/dm/worker.SampleConfigFile=$(shell cat dm/worker/dm-worker.toml | base64 -w 0)"
	LDFLAGS += -X "github.com/pingcap/dm/dm/master.SampleConfigFile=$(shell cat dm/master/dm-master.toml | base64 -w 0)"
else
	LDFLAGS += -X "github.com/pingcap/dm/dm/worker.SampleConfigFile=$(shell cat dm/worker/dm-worker.toml | base64)"
	LDFLAGS += -X "github.com/pingcap/dm/dm/master.SampleConfigFile=$(shell cat dm/master/dm-master.toml | base64)"
endif

.PHONY: build retool_setup test unit_test dm_integration_test_build integration_test \
	coverage check dm-worker dm-master dm-tracer dmctl debug-tools

build: check dm-worker dm-master dm-tracer dmctl dm-portal dm-syncer

dm-worker:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/dm-worker ./cmd/dm-worker

dm-master:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/dm-master ./cmd/dm-master

dm-tracer:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/dm-tracer ./cmd/dm-tracer

dmctl:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/dmctl ./cmd/dm-ctl

dm-portal:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/dm-portal ./cmd/dm-portal

dm-syncer:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/dm-syncer ./cmd/dm-syncer

debug-tools:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/binlog-event-blackhole ./debug-tools/binlog-event-blackhole

retool_setup:
	@echo "setup retool"
	go get github.com/twitchtv/retool
	GO111MODULE=off retool sync

generate_proto: retool_setup
	./generate-dm.sh

generate_mock: retool_setup
	./tests/generate-mock.sh

test: unit_test integration_test

define run_unit_test
	@echo "running unit test for packages:" $(1)
	bash -x ./tests/wait_for_mysql.sh
	mkdir -p $(TEST_DIR)
	$(FAILPOINT_ENABLE)
	@export log_level=error; \
	$(GOTEST) -covermode=atomic -coverprofile="$(TEST_DIR)/cov.$(2).out" $(TEST_RACE_FLAG) $(1) \
	|| { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)
endef

unit_test: retool_setup
	$(call run_unit_test,$(PACKAGES),unit_test)

unit_test_relay: retool_setup
	$(call run_unit_test,$(PACKAGES_RELAY),unit_test_relay)

unit_test_syncer: retool_setup
	$(call run_unit_test,$(PACKAGES_SYNCER),unit_test_syncer)

unit_test_pkg_binlog: retool_setup
	$(call run_unit_test,$(PACKAGES_PKG_BINLOG),unit_test_pkg_binlog)

unit_test_others: retool_setup
	$(call run_unit_test,$(PACKAGES_OTHERS),unit_test_others)

check: retool_setup fmt lint vet terror_check

fmt:
	@echo "gofmt (simplify)"
	@ gofmt -s -l -w $(FILES) 2>&1 | awk '{print} END{if(NR>0) {exit 1}}'

errcheck: retool_setup
	@echo "errcheck"
	@retool do errcheck -blank $(PACKAGES) | grep -v "_test\.go" | awk '{print} END{if(NR>0) {exit 1}}'

lint: retool_setup
	@echo "golint"
	@retool do golint -set_exit_status $(PACKAGES)

vet:
	$(GO) build -o bin/shadow golang.org/x/tools/go/analysis/passes/shadow/cmd/shadow
	@echo "vet"
	@$(GO) vet -composites=false $(PACKAGES)
	@$(GO) vet -vettool=$(CURDIR)/bin/shadow $(PACKAGES) || true

terror_check:
	@echo "check terror conflict"
	_utils/terror_gen/check.sh

dm_integration_test_build: retool_setup
	$(FAILPOINT_ENABLE)
	$(GOTEST) -c $(TEST_RACE_FLAG) -cover -covermode=atomic \
		-coverpkg=github.com/pingcap/dm/... \
		-o bin/dm-worker.test github.com/pingcap/dm/cmd/dm-worker \
		|| { $(FAILPOINT_DISABLE); exit 1; }
	$(GOTEST) -c $(TEST_RACE_FLAG) -cover -covermode=atomic \
		-coverpkg=github.com/pingcap/dm/... \
		-o bin/dm-master.test github.com/pingcap/dm/cmd/dm-master \
		|| { $(FAILPOINT_DISABLE); exit 1; }
	$(GOTEST) -c -cover -covermode=count \
		-coverpkg=github.com/pingcap/dm/... \
		-o bin/dmctl.test github.com/pingcap/dm/cmd/dm-ctl \
		|| { $(FAILPOINT_DISABLE); exit 1; }
	$(GOTEST) -c $(TEST_RACE_FLAG) -cover -covermode=atomic \
		-coverpkg=github.com/pingcap/dm/... \
		-o bin/dm-tracer.test github.com/pingcap/dm/cmd/dm-tracer \
		|| { $(FAILPOINT_DISABLE); exit 1; }
	$(GOTEST) -c $(TEST_RACE_FLAG) -cover -covermode=atomic \
		-coverpkg=github.com/pingcap/dm/... \
		-o bin/dm-syncer.test github.com/pingcap/dm/cmd/dm-syncer \
		|| { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)
	tests/prepare_tools.sh

check_third_party_binary:
	@which bin/tidb-server
	@which bin/sync_diff_inspector
	@which bin/mydumper

integration_test: check_third_party_binary
	@which bin/dm-master.test
	@which bin/dm-worker.test
	@which bin/dm-tracer.test
	@which bin/dm-syncer.test
	tests/run.sh $(CASE)

compatibility_test: check_third_party_binary
	@which bin/dm-tracer.test
	@which bin/dm-master.test.current
	@which bin/dm-worker.test.current
	@which bin/dm-master.test.previous
	@which bin/dm-worker.test.previous
	tests/compatibility_run.sh ${CASE}

# unify cover mode in coverage files, more details refer to tests/_utils/run_dm_ctl
coverage_fix_cover_mode:
	sed -i "s/mode: count/mode: atomic/g" $(TEST_DIR)/cov.*.dmctl.*.out

coverage: coverage_fix_cover_mode retool_setup
	retool do gocovmerge "$(TEST_DIR)"/cov.* | grep -vE ".*.pb.go|.*.__failpoint_binding__.go" > "$(TEST_DIR)/all_cov.out"
	retool do gocovmerge "$(TEST_DIR)"/cov.unit_test*.out | grep -vE ".*.pb.go|.*.__failpoint_binding__.go" > $(TEST_DIR)/unit_test.out
ifeq ("$(JenkinsCI)", "1")
	@retool do goveralls -coverprofile=$(TEST_DIR)/all_cov.out -service=jenkins-ci -repotoken $(COVERALLS_TOKEN)
	@bash <(curl -s https://codecov.io/bash) -f $(TEST_DIR)/unit_test.out -t $(CODECOV_TOKEN)
else
	go tool cover -html "$(TEST_DIR)/all_cov.out" -o "$(TEST_DIR)/all_cov.html"
	go tool cover -html "$(TEST_DIR)/unit_test.out" -o "$(TEST_DIR)/unit_test_cov.html"
endif

check-static:
	@echo "gometalinter"
	gometalinter --disable-all --deadline 120s \
	  --enable misspell \
	  --enable megacheck \
	  --enable ineffassign \
	  ./...

failpoint-enable: retool_setup
	$(FAILPOINT_ENABLE)

failpoint-disable: retool_setup
	$(FAILPOINT_DISABLE)
