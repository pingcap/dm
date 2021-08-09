import sys
import requests

TASK_NAME = "test"
SOURCE1_NAME = "mysql-01"
SOURCE2_NAME = "mysql-02"


API_ENDPOINT = "http://127.0.0.1:1323/api/v1/tasks"


def start_task_success():
    req = {
        "name": "test",
        "task_mode": "all",
        "shard_mode": "pessimistic",
        "meta_schema": "dm-meta",
        "remove_meta": True,
        "enhance_online_schema_change": True,
        "on_duplication": "error",
        "target_config": {
            "host": "127.0.0.1",
            "port": 4000,
            "user": "root",
            "password": "",
        },
        "table_migrate_rule": [
            {
                "source": {
                    "source_name": SOURCE1_NAME,
                    "schema": "openapi",
                    "table": "*",
                },
                "target": {"schema": "openapi", "table": "t"},
            },
            {
                "source": {
                    "source_name": SOURCE2_NAME,
                    "schema": "openapi",
                    "table": "*",
                },
                "target": {"schema": "openapi", "table": "t"},
            },
        ],
        "source_config": {
            "source_conf": [
                {"source_name": SOURCE1_NAME},
                {"source_name": SOURCE2_NAME},
            ],
        },
    }
    resp = requests.post(url=API_ENDPOINT, json=req)
    assert resp.status_code == 201
    print("start_task_success resp=", resp.json())


def stop_task_failed(task_name):
    resp = requests.delete(url=API_ENDPOINT + "/" + task_name)
    assert resp.status_code == 400
    print("stop_task_failed resp=", resp.json())


def stop_task_success(task_name):
    resp = requests.delete(url=API_ENDPOINT + "/" + task_name)
    assert resp.status_code == 204
    print("stop_task_success")


if __name__ == "__main__":
    FUNC_MAP = {
        "start_task_success": start_task_success,
        "stop_task_failed": stop_task_failed,
        "stop_task_success": stop_task_success,
    }

    func = FUNC_MAP[sys.argv[1]]
    if len(sys.argv) >= 2:
        func(*sys.argv[2:])
    else:
        func()
