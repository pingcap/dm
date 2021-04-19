module github.com/pingcap/dm

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/DATA-DOG/go-sqlmock v1.5.0
	github.com/chaos-mesh/go-sqlsmith v0.0.0-20201120053641-47c50b530c01
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e
	github.com/coreos/go-semver v0.3.0
	github.com/docker/go-units v0.4.0
	github.com/dustin/go-humanize v1.0.0
	github.com/go-mysql-org/go-mysql v1.1.2-0.20210419035833-609df4e7b974
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gogo/gateway v1.1.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/mock v1.4.4
	github.com/golang/protobuf v1.3.4
	github.com/google/uuid v1.1.1
	github.com/grpc-ecosystem/grpc-gateway v1.14.3
	github.com/kami-zh/go-capturer v0.0.0-20171211120116-e492ea43421d
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/dumpling v0.0.0-20210407092432-e1cfe4ce0a53
	github.com/pingcap/errors v0.11.5-0.20201126102027-b0a155152ca3
	github.com/pingcap/failpoint v0.0.0-20210316064728-7acb0f0a3dfd
	github.com/pingcap/log v0.0.0-20201112100606-8f1e84a3abc8
	github.com/pingcap/parser v0.0.0-20210324190955-ab6d0f2c18ee
	github.com/pingcap/tidb v1.1.0-beta.0.20210330094614-60111e1c4b6f
	github.com/pingcap/tidb-tools v5.0.0-rc.0.20210318094904-51a9e0c86386+incompatible
	github.com/prometheus/client_golang v1.5.1
	github.com/rakyll/statik v0.1.6
	github.com/siddontang/go v0.0.0-20180604090527-bdc77568d726
	github.com/soheilhy/cmux v0.1.4
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5
	github.com/syndtr/goleveldb v1.0.1-0.20190625010220-02440ea7a285
	github.com/tidwall/gjson v1.6.1
	github.com/tidwall/sjson v1.1.2
	github.com/tikv/pd v1.1.0-beta.0.20210323121136-78679e5e209d
	github.com/unrolled/render v1.0.1
	go.etcd.io/etcd v0.5.0-alpha.5.0.20200824191128-ae9734ed278b
	go.uber.org/zap v1.16.0
	golang.org/x/exp v0.0.0-20200513190911-00229845015e // indirect
	golang.org/x/sync v0.0.0-20201020160332-67f06af15bc9
	golang.org/x/sys v0.0.0-20210320140829-1e4c9ba3b0c4
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0
	google.golang.org/genproto v0.0.0-20200224152610-e50cd9704f63
	google.golang.org/grpc v1.27.1
	gopkg.in/yaml.v2 v2.4.0
)

go 1.13

replace github.com/siddontang/go v0.0.0-20180604090527-bdc77568d726 => github.com/lance6716/go v0.0.0-20210312094856-8a1d496ae7d4
