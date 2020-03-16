module github.com/pingcap/dm

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/DATA-DOG/go-sqlmock v1.3.3
	github.com/Masterminds/semver v1.5.0 // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20190719114852-fd7a80b32e1f // indirect
	github.com/dustin/go-humanize v1.0.0
	github.com/go-sql-driver/mysql v1.4.1
	github.com/gogo/protobuf v1.3.1
	github.com/golang/groupcache v0.0.0-20191027212112-611e8accdfc9 // indirect
	github.com/golang/mock v1.3.1
	github.com/golang/protobuf v1.3.4
	github.com/gorilla/websocket v1.4.1 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.12.1
	github.com/kami-zh/go-capturer v0.0.0-20171211120116-e492ea43421d
	github.com/klauspost/cpuid v1.2.1 // indirect
	github.com/montanaflynn/stats v0.5.0 // indirect
	github.com/onsi/ginkgo v1.9.0 // indirect
	github.com/onsi/gomega v1.6.0 // indirect
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errcode v0.3.0 // indirect
	github.com/pingcap/errors v0.11.5-0.20190809092503-95897b64e011
	github.com/pingcap/failpoint v0.0.0-20200210140405-f8f9fb234798
	github.com/pingcap/log v0.0.0-20200117041106-d28c14d3b1cd
	github.com/pingcap/parser v0.0.0-20200305120128-bde9faa0df84
	github.com/pingcap/pd/v4 v4.0.0-beta.1.0.20200305072537-61d9f9cc35d3
	github.com/pingcap/tidb v1.1.0-beta.0.20200311125547-6c67561ee0df
	github.com/pingcap/tidb-tools v4.0.0-beta.1.0.20200315095020-6bea09b23e42+incompatible
	github.com/prometheus/client_golang v1.2.1
	github.com/prometheus/procfs v0.0.6 // indirect
	github.com/rakyll/statik v0.1.6
	github.com/satori/go.uuid v1.2.0
	github.com/shopspring/decimal v0.0.0-20191125035519-b054a8dfd10d // indirect
	github.com/siddontang/go v0.0.0-20180604090527-bdc77568d726
	github.com/siddontang/go-log v0.0.0-20190221022429-1e957dd83bed // indirect
	github.com/siddontang/go-mysql v0.0.0-20200222075837-12e89848f047
	github.com/soheilhy/cmux v0.1.4
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/cobra v0.0.5
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/syndtr/goleveldb v1.0.1-0.20190318030020-c3a204f8e965 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	github.com/twitchtv/retool v1.3.8-0.20180918173430-41330f8b4e07
	github.com/uber-go/atomic v1.4.0 // indirect
	github.com/uber/jaeger-client-go v2.16.0+incompatible // indirect
	github.com/uber/jaeger-lib v2.0.0+incompatible // indirect
	github.com/unrolled/render v1.0.1
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.uber.org/zap v1.14.0
	golang.org/x/sys v0.0.0-20200113162924-86b910548bc1
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0
	google.golang.org/appengine v1.6.1 // indirect
	google.golang.org/genproto v0.0.0-20191114150713-6bbd007550de
	google.golang.org/grpc v1.25.1
	gopkg.in/yaml.v2 v2.2.8
	sourcegraph.com/sourcegraph/appdash v0.0.0-20190731080439-ebfcffb1b5c0 // indirect
)

go 1.13

replace github.com/pingcap/tidb-tools v4.0.0-beta.1.0.20200315095020-6bea09b23e42+incompatible => github.com/WangXiangUSTC/tidb-tools v3.0.12-0.20200316030416-2edca86db1f2+incompatible
