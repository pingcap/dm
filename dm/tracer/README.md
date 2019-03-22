# DM tracer server

## Introduction

DM tracer server is used for tracing events collect, query and manage.

## HTTP API doc

`TracerIP` is the ip of the DM tracer server. `8263` is the default status port, and you can edit it in dm-tracer.toml when starting the DM tracer server.

1. Get the current status of tracer server, including the version and git_hash

    ```shell
    curl http://{TracerIP}:8263/status
    ```

    ```shell
    $curl http://127.0.0.1:8263/status
    {
        "version":"v1.0.0-alpha-46-g12b9422",
        "git_hash":"12b94225f45f69a58efd441ba0876f8f08e354f1"
    }
    ```

1. Query tracing event bundle by traceID

    ```shell
    curl http://{TracerIP}:8263/events/query?trace_id={trace_id}
    ```

    ```shell
    curl http://127.0.0.1:8263/events/query?trace_id=mysql-replica-01.syncer.test.1
    [
        {
            "type": 1,
            "event": {
                "base": {
                    "filename": "/root/code/gopath/src/github.com/pingcap/dm/syncer/syncer.go",
                    "line": 1130,
                    "tso": 1552371525597270410,
                    "traceID": "mysql-replica-01.syncer.test.1",
                    "type": 1
                },
                "state": {
                    "safeMode": true,
                    "tryReSync": true,
                    "lastPos": {
                    "name": "bin|000001.000004",
                    "pos": 68073
                    }
                },
                "eventType": 4,
                "opType": 8,
                "currentPos": {
                    "name": "bin|000001.000004",
                    "pos": 68073
                }
            }
        },
        {
            ...
        },
        {
            ...
        }
    ]
    ```
1. Delete tracing event bundle by traceID

    ```shell
    curl http://{TracerIP}:8263/events/delete?trace_id={trace_id}
    ```

    ```shell
    curl http://127.0.0.1:8263/events/delete?trace_id=mysql-replica-01.syncer.test.1

    {
        "trace_id": "mysql-replica-01.syncer.test.1",
        "result": true
    }
    ```

1. Scan tracing event based on offset and limit

    ```shell
    curl http://{TracerIP}:8263/events/scan?offset=1&limit=3
    [
      [
        {...},
        {...}
      ],
      [
        {...}
      ],
      [
        {...},
        {...},
        {...}
      ]
    ]
