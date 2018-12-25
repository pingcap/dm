## Loader
Loader is a multi-threaded restoration tool for [mydumper](https://github.com/maxbube/mydumper), built specially for TiDB.

Loader doesn't support MySQL now. 

## Options
Loader has the following available options:
```
  -L string
    	Loader log level: debug, info, warn, error, fatal (default "info")
  -P int
    	TCP/IP port to connect to (default 4000)
  -d string
    	Directory of the dump to import (default "./")
  -h string
    	The host to connect to (default "127.0.0.1")
  -p string
    	User password
  -pprof-addr string
    	Loader pprof addr (default ":8272")
  -q int
    	Number of queries per transaction (default 1000)
  -t int
    	Number of threads to use (default 4)
  -u string
    	Username with privileges to run the dump (default "root")
```

## Config
```
# Loader log level
log-level = "info"

# Loader log file
log-file = ""

# Directory of the dump to import
dir = "./"

# Loader pprof addr
pprof-addr = ":8272"

# Number of threads to use
worker = 4

# DB config
[db]
host = "127.0.0.1"
user = "root"
password = ""
port = 4000
```

## Example
You can run the following commands:
```
./bin/loader -d ./test -h 127.0.0.1 -u root -P 4000
```
Or use the "config.toml" file:
```
./bin/loader -c=config.toml
```

## Note
When loading the `mydumper` SQL file, Loader writes the file names and offsets to tidb by creating a database named `tidb_loader` and table named `checkpoint`. If Loader exits unexpectedly in the process of loading, we can recover it according to the checkpoint and Loader will automatically ignore the files that have been loaded.

e.g. 

```
mysql> select * from tidb_loader.checkpoint;

+--------+-------------------------------+-------------+------------+--------+---------+---------------------+
| id     | filename                      | cp_schema   | cp_table   | offset | end_pos | create_time         |
+--------+-------------------------------+-------------+------------+--------+---------+---------------------+
| 1b626f | shard_db_01.shard_0001.01.sql | shard_db_01 | shard_0001 |   2316 |    2316 | 2017-06-23 11:38:57 |
| 1b626f | shard_db_01.shard_0002.sql    | shard_db_01 | shard_0002 |   4688 |    4688 | 2017-06-23 11:38:57 |
| 1b626f | shard_db_01.shard_0001.02.sql | shard_db_01 | shard_0001 |   2290 |    2290 | 2017-06-23 11:38:57 |
+--------+-------------------------------+-------------+------------+--------+---------+---------------------+
2 rows in set (0.12 sec)
```

## License
Apache 2.0 license. See the [LICENSE](../LICENSE) file for details.
