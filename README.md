# Data Migration Platform

[![Build Status](https://internal.pingcap.net/idc-jenkins/job/build_dm_master/badge/icon)](https://internal.pingcap.net/idc-jenkins/job/build_dm_master/)
![GitHub release](https://img.shields.io/github/tag/pingcap/dm.svg)
[![Coverage Status](https://coveralls.io/repos/github/pingcap/dm/badge.svg)](https://coveralls.io/github/pingcap/dm)
[![codecov](https://codecov.io/gh/pingcap/dm/branch/master/graph/badge.svg)](https://codecov.io/gh/pingcap/dm)
[![Go Report Card](https://goreportcard.com/badge/github.com/pingcap/dm)](https://goreportcard.com/report/github.com/pingcap/dm)

**DM** is an integrated platform, supports migrating data from MySQL/MariaDB to TiDB.

## Architecture

![architecture](https://pingcap.com/images/docs/dm-architecture.png)

## Documentation

* [Detailed documentation](https://pingcap.com/docs/tools/dm/overview/)
* [简体中文文档](https://pingcap.com/docs-cn/tools/dm/overview/)

## Building

To check the code style and build binaries, you can simply run:

```bash
make build
```

Notice DM supports building with Go version `Go >= 1.11.4`, and unit test preparation can be found in [Running/Unit Test](tests/README.md#Unit-Test)

If you only want to build binaries, you can run:
```bash
make dm-worker  # build DM-worker

make dm-master  # build DM-master

make dmctl      # build dmctl
``` 

When DM is built successfully, you can find binaries in the `bin` directory.

## Run Test

Run all tests, including unit test and integration test

```bash
make test
```

## Installing

* The best way to install DM is via [DM-Ansible](https://pingcap.com/docs/tools/dm/deployment/)
* deploy DM manually
  ```
  # Download the DM package.
  wget http://download.pingcap.org/dm-latest-linux-amd64.tar.gz
  wget http://download.pingcap.org/dm-latest-linux-amd64.sha256

  # Check the file integrity. If the result is OK, the file is correct.
  sha256sum -c dm-latest-linux-amd64.sha256

  # Extract the package.
  tar -xzf dm-latest-linux-amd64.tar.gz
  cd dm-latest-linux-amd64
  ```

## Config File

* all sample config files can be found in directory `conf` of dm tarball
* sample config file of dm-master: `bin/dm-master -print-sample-config`
* sample config file of dm-worker: `bin/dm-worker -print-sample-config`


## Contributing
Contributions are welcomed and greatly appreciated. See [CONTRIBUTING.md](./CONTRIBUTING.md)
for details on submitting patches and the contribution workflow.

## License
DM is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
