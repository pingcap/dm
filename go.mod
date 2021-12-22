module github.com/pingcap/dm

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/DATA-DOG/go-sqlmock v1.5.0
	github.com/chaos-mesh/go-sqlsmith v0.0.0-20210914111832-b3d328d69449
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e
	github.com/coreos/go-semver v0.3.0
	github.com/docker/go-units v0.4.0
	github.com/dustin/go-humanize v1.0.0
	github.com/go-mysql-org/go-mysql v1.4.1-0.20211222092441-0bb0b8d64e34
	github.com/go-sql-driver/mysql v1.6.0
	github.com/gogo/gateway v1.1.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/mock v1.6.0
	github.com/golang/protobuf v1.5.2
	github.com/google/uuid v1.1.2
	github.com/grpc-ecosystem/grpc-gateway v1.16.0
	github.com/kami-zh/go-capturer v0.0.0-20171211120116-e492ea43421d
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/dumpling v0.0.0-20210914144241-99aca9186bc8
	github.com/pingcap/errors v0.11.5-0.20210513014640-40f9a1999b3b
	github.com/pingcap/failpoint v0.0.0-20210316064728-7acb0f0a3dfd
	github.com/pingcap/log v0.0.0-20210906054005-afc726e70354
	github.com/pingcap/parser v0.0.0-20210907051057-948434fa20e4
	github.com/pingcap/tidb v1.1.0-beta.0.20210914112841-6ebfe8aa4257
	github.com/pingcap/tidb-tools v5.2.0-alpha.0.20210727084616-915b22e4d42c+incompatible
	github.com/prometheus/client_golang v1.5.1
	github.com/rakyll/statik v0.1.6
	github.com/shopspring/decimal v0.0.0-20200105231215-408a2507e114
	github.com/soheilhy/cmux v0.1.4
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5
	github.com/syndtr/goleveldb v1.0.1-0.20190625010220-02440ea7a285
	github.com/tidwall/gjson v1.6.1
	github.com/tidwall/sjson v1.1.2
	github.com/tikv/pd v1.1.0-beta.0.20210818082359-acba1da0018d
	github.com/unrolled/render v1.0.1
	go.etcd.io/etcd v0.5.0-alpha.5.0.20200824191128-ae9734ed278b
	go.uber.org/atomic v1.9.0
	go.uber.org/zap v1.19.0
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20210806184541-e5e7981a1069
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba
	google.golang.org/genproto v0.0.0-20210825212027-de86158e7fda
	google.golang.org/grpc v1.40.0
	gopkg.in/yaml.v2 v2.4.0
)

replace google.golang.org/grpc => google.golang.org/grpc v1.29.1

go 1.16
