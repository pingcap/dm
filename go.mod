module github.com/pingcap/dm

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/DATA-DOG/go-sqlmock v1.4.1
	github.com/Masterminds/semver v1.5.0 // indirect
	github.com/chaos-mesh/go-sqlsmith v0.0.0-00010101000000-000000000000
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e
	github.com/coreos/go-semver v0.3.0
	github.com/docker/go-units v0.4.0
	github.com/dustin/go-humanize v1.0.0
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gogo/gateway v1.1.0
	github.com/gogo/protobuf v1.3.1
	github.com/golang/mock v1.3.1
	github.com/golang/protobuf v1.3.4
	github.com/gorilla/websocket v1.4.1 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.14.3
	github.com/kami-zh/go-capturer v0.0.0-20171211120116-e492ea43421d
	github.com/lance6716/retool v1.3.8-0.20200806070832-3469f70b2afe
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/dumpling v0.0.0-20200909092728-a45daad655d1
	github.com/pingcap/errors v0.11.5-0.20200917111840-a15ef68f753d
	github.com/pingcap/failpoint v0.0.0-20200702092429-9f69995143ce
	github.com/pingcap/log v0.0.0-20200828042413-fce0951f1463
	github.com/pingcap/parser v0.0.0-20200924053142-5d7e8ebf605e
	github.com/pingcap/tidb v1.1.0-beta.0.20200927065602-486e473a86e9
	github.com/pingcap/tidb-tools v4.0.7-0.20200927084250-e47e0e12c7f3+incompatible
	github.com/prometheus/client_golang v1.5.1
	github.com/rakyll/statik v0.1.6
	github.com/satori/go.uuid v1.2.0
	github.com/shopspring/decimal v0.0.0-20191125035519-b054a8dfd10d // indirect
	github.com/siddontang/go v0.0.0-20180604090527-bdc77568d726
	github.com/siddontang/go-log v0.0.0-20190221022429-1e957dd83bed // indirect
	github.com/siddontang/go-mysql v0.0.0-20200222075837-12e89848f047
	github.com/soheilhy/cmux v0.1.4
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5
	github.com/syndtr/goleveldb v1.0.1-0.20190625010220-02440ea7a285
	github.com/tikv/pd v1.1.0-beta.0.20200907085700-5b04bec39b99
	github.com/uber-go/atomic v1.4.0 // indirect
	github.com/unrolled/render v1.0.1
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.uber.org/zap v1.16.0
	golang.org/x/sync v0.0.0-20200625203802-6e8e738ad208
	golang.org/x/sys v0.0.0-20200824131525-c12d262b63d8
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0
	google.golang.org/genproto v0.0.0-20191230161307-f3c370f40bfb
	google.golang.org/grpc v1.26.0
	gopkg.in/yaml.v2 v2.3.0
	sourcegraph.com/sourcegraph/appdash v0.0.0-20190731080439-ebfcffb1b5c0 // indirect
)

go 1.13

replace github.com/chaos-mesh/go-sqlsmith => github.com/csuzhangxc/go-sqlsmith v0.0.0-20200928023709-4ad8903babdd
