
LDFLAGS += -X "github.com/pingcap/tidb-enterprise-tools/pkg/utils.ReleaseVersion=$(shell git describe --tags --dirty="-dev")"
LDFLAGS += -X "github.com/pingcap/tidb-enterprise-tools/pkg/utils.BuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "github.com/pingcap/tidb-enterprise-tools/pkg/utils.GitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "github.com/pingcap/tidb-enterprise-tools/pkg/utils.GitBranch=$(shell git rev-parse --abbrev-ref HEAD)"
LDFLAGS += -X "github.com/pingcap/tidb-enterprise-tools/pkg/utils.GoVersion=$(shell go version)"


CURDIR   := $(shell pwd)
GO       := GO15VENDOREXPERIMENT="1" go
GOTEST   := GOPATH=$(CURDIR)/_vendor:$(GOPATH) CGO_ENABLED=1 $(GO) test
PACKAGES := $$(go list ./... | grep -vE 'vendor')
FILES     := $$(find . -name "*.go" | grep -vE "vendor")
TOPDIRS   := $$(ls -d */ | grep -vE "vendor")

.PHONY: build syncer loader test check deps dm-worker dm-master dmctl 

build: syncer loader check test dm-worker dm-master dmctl

dm-worker:
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/dm-worker ./cmd/dm-worker

dm-master:
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/dm-master ./cmd/dm-master

dmctl:
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/dmctl ./cmd/dm-ctl

syncer:
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/syncer ./cmd/syncer

loader:
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/loader ./cmd/loader

test:
	@export log_level=error; \
	$(GOTEST) -cover $(PACKAGES)

check: fmt lint vet

fmt:
	@echo "gofmt (simplify)"
	@ gofmt -s -l -w $(FILES) 2>&1 | awk '{print} END{if(NR>0) {exit 1}}'

errcheck:
	go get github.com/kisielk/errcheck
	@echo "errcheck"
	@ GOPATH=$(GOPATH) errcheck -blank $(PACKAGES) | grep -v "_test\.go" | awk '{print} END{if(NR>0) {exit 1}}'

lint:
	go get github.com/golang/lint/golint
	@echo "golint"
	@ golint -set_exit_status $(PACKAGES)

vet:
	@echo "vet"
	@ go tool vet -all -shadow $(TOPDIRS) 2>&1 | awk '{print} END{if(NR>0) {exit 1}}'
	

update:
	@which glide >/dev/null || curl https://glide.sh/get | sh
	@which glide-vc || go get -v -u github.com/sgotti/glide-vc
ifdef PKG
	@glide get -s -v --skip-test ${PKG}
else
	@glide update -s -v -u --skip-test
endif
	@echo "removing test files"
	@glide vc --use-lock-file --only-code --no-tests
