import sys
import requests

NO_SHARD_TASK_NAME = "test-no-shard"
SHARD_TASK_NAME = "test-shard"
SOURCE1_NAME = "mysql-01"
SOURCE2_NAME = "mysql-02"


API_ENDPOINT = "http://127.0.0.1:1323/api/v1/tasks"


def start_task_failed():
    req = {
        "name": "test",
        "task_mode": "all",
        "shard_mode": "pessimistic_xxd",  # pessimistic_xxd is not a valid shard mode
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
    assert resp.status_code == 400
    print("start_task_failed resp=", resp.json())


def start_noshard_task_success():
    # start
    req = {
        "name": NO_SHARD_TASK_NAME,
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
                "target": {"schema": "openapi", "table": ""},
            },
            {
                "source": {
                    "source_name": SOURCE2_NAME,
                    "schema": "openapi",
                    "table": "*",
                },
                "target": {"schema": "openapi", "table": ""},
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
    print("start_noshard_task_success resp=", resp.json())


def start_shard_task_success():
    # start
    req = {
        "name": SHARD_TASK_NAME,
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
                "event_filter_name": ["rule-1"],
            },
            {
                "source": {
                    "source_name": SOURCE2_NAME,
                    "schema": "openapi",
                    "table": "*",
                },
                "target": {"schema": "openapi", "table": "t"},
                "event_filter_name": ["rule-2"],
            },
        ],
        "source_config": {
            "source_conf": [
                {"source_name": SOURCE1_NAME},
                {"source_name": SOURCE2_NAME},
            ],
        },
        "event_filter_rule": [
            {
                "rule_name": "rule-1",
                "ignore_event": ["delete"],
            },
            {
                "rule_name": "rule-2",
                "ignore_sql": ["alter table .* add column `aaa` int"],
            },
        ],
    }
    resp = requests.post(url=API_ENDPOINT, json=req)
    assert resp.status_code == 201
    print("start_shard_task_success resp=", resp.json())


def get_task_status_failed(task_name):
    url = API_ENDPOINT + "/" + task_name + "/status"
    resp = requests.get(url=url)
    assert resp.status_code == 400
    print("get_task_status_failed resp=", resp.json())


def get_task_status_success(task_name, total):
    url = API_ENDPOINT + "/" + task_name + "/status"
    resp = requests.get(url=url)
    data = resp.json()
    assert resp.status_code == 200
    assert data["total"] == int(total)
    print("get_task_status_failed resp=", data)


def get_task_list(task_count):
    url = API_ENDPOINT
    resp = requests.get(url=url)
    data = resp.json()
    assert resp.status_code == 200
    assert data["total"] == int(task_count)
    print("get_task_list resp=", data)


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
        "start_task_failed": start_task_failed,
        "start_noshard_task_success": start_noshard_task_success,
        "start_shard_task_success": start_shard_task_success,
        "stop_task_failed": stop_task_failed,
        "stop_task_success": stop_task_success,
        "get_task_list": get_task_list,
        "get_task_status_failed": get_task_status_failed,
        "get_task_status_success": get_task_status_success,
    }

    func = FUNC_MAP[sys.argv[1]]
    if len(sys.argv) >= 2:
        func(*sys.argv[2:])
    else:
        func()
