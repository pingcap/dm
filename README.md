# Data Migration Platform

[![Build Status](https://internal.pingcap.net/idc-jenkins/job/build_dm/badge/icon)](https://internal.pingcap.net/idc-jenkins/job/build_dm/)
![GitHub release](https://img.shields.io/github/tag-pre/pingcap/dm.svg)
[![Coverage Status](https://coveralls.io/repos/github/pingcap/dm/badge.svg)](https://coveralls.io/github/pingcap/dm)
[![Go Report Card](https://goreportcard.com/badge/github.com/pingcap/dm)](https://goreportcard.com/report/github.com/pingcap/dm)

**DM** is an integrated platform, supports migrating data from MySQL/MariaDB to TiDB.

## Architecture

![architecture](https://pingcap.com/images/docs/dm-architecture.png)

## Documentation

* [Detailed documentation](https://pingcap.com/docs/tools/data-migration-overview/)
* [简体中文文档](https://github.com/pingcap/tidb-tools/blob/docs/docs/dm/zh_CN/README.md)

## Building

To check the source code, run test cases and build binaries, you can simply run:

```bash
make build
```

If you only want to build binaries, you can run:
```bash
make dm-worker  # build DM-worker

make dm-master  # build DM-master

make dmctl      # build dmctl
``` 

When DM is built successfully, you can find binaries in the `bin` directory.

## Contributing
Contributions are welcomed and greatly appreciated. See [CONTRIBUTING.md](./CONTRIBUTING.md)
for details on submitting patches and the contribution workflow.

## License
DM is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
