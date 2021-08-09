import sys
import requests

SOURCE1_NAME = "mysql-01"
SOURCE2_NAME = "mysql-02"
WORKER1_NAME = "worker1"
WORKER2_NAME = "worker2"


API_ENDPOINT = "http://127.0.0.1:1323/api/v1/sources"
API_ENDPOINT_NOT_LEADER = "http://127.0.0.1:1324/api/v1/sources"


def create_source_failed():
    resp = requests.post(url=API_ENDPOINT)
    assert resp.status_code == 400
    print("create_source_failed resp=", resp.json())


def create_source1_success():
    req = {
        "case_sensitive": False,
        "enable_gtid": False,
        "host": "127.0.0.1",
        "password": "123456",
        "port": 3306,
        "source_name": SOURCE1_NAME,
        "user": "root",
    }
    resp = requests.post(url=API_ENDPOINT, json=req)
    assert resp.status_code == 201
    print("create_source1_success resp=", resp.json())


def create_source2_success():
    req = {
        "case_sensitive": False,
        "enable_gtid": False,
        "host": "127.0.0.1",
        "password": "123456",
        "port": 3307,
        "source_name": SOURCE2_NAME,
        "user": "root",
    }
    resp = requests.post(url=API_ENDPOINT, json=req)
    assert resp.status_code == 201
    print("create_source1_success resp=", resp.json())


def list_source_success(source_count):
    resp = requests.get(url=API_ENDPOINT)
    assert resp.status_code == 200
    data = resp.json()
    # only create one source
    assert data["total"] == int(source_count)
    print("list_source_by_openapi_success resp=", data)


def list_source_with_redirect(source_count):
    resp = requests.get(url=API_ENDPOINT_NOT_LEADER)
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == int(source_count)
    print("list_source_by_openapi_redirect resp=", data)


def delete_source_success(source_name):
    resp = requests.delete(url=API_ENDPOINT + "/" + source_name)
    assert resp.status_code == 204
    print("delete_source_success")


def delete_source_failed(source_name):
    resp = requests.delete(url=API_ENDPOINT + "/" + source_name)
    assert resp.status_code == 400
    print("delete_source_failed msg=", resp.json())


def start_relay_failed(source_name, worker_name):
    url = API_ENDPOINT + "/" + source_name + "/start-relay"
    req = {
        "worker_name": worker_name,
    }
    resp = requests.patch(url=url, json=req)
    assert resp.status_code == 400
    print("start_relay_failed resp=", resp.json())


def start_relay_success(source_name, worker_name):
    url = API_ENDPOINT + "/" + source_name + "/start-relay"
    req = {
        "worker_name": worker_name,
        "purge": {"interval": 3600, "expires": 0, "remain_space": 15},
    }
    resp = requests.patch(url=url, json=req)
    assert resp.status_code == 200


def get_source_status_failed(source_name):
    url = API_ENDPOINT + "/" + source_name + "/status"
    resp = requests.get(url=url)
    assert resp.status_code == 400
    print("get_source_status_failed resp=", resp.json())


def get_source_status_success(source_name):
    url = API_ENDPOINT + "/" + source_name + "/status"
    resp = requests.get(url=url)
    assert resp.status_code == 200
    print("get_source_status_success resp=", resp.json())


def stop_relay_failed(source_name, worker_name):
    url = API_ENDPOINT + "/" + source_name + "/stop-relay"
    req = {
        "worker_name": worker_name,
    }
    resp = requests.patch(url=url, json=req)
    assert resp.status_code == 400
    print("stop_relay_failed resp=", resp.json())


def stop_relay_success(source_name, worker_name):
    url = API_ENDPOINT + "/" + source_name + "/stop-relay"
    req = {
        "worker_name": worker_name,
    }
    resp = requests.patch(url=url, json=req)
    assert resp.status_code == 200


if __name__ == "__main__":
    FUNC_MAP = {
        "create_source_failed": create_source_failed,
        "create_source1_success": create_source1_success,
        "create_source2_success": create_source2_success,
        "list_source_success": list_source_success,
        "list_source_with_redirect": list_source_with_redirect,
        "delete_source_failed": delete_source_failed,
        "delete_source_success": delete_source_success,
        "start_relay_failed": start_relay_failed,
        "start_relay_success": start_relay_success,
        "get_source_status_failed": get_source_status_failed,
        "get_source_status_success": get_source_status_success,
        "stop_relay_failed": stop_relay_failed,
        "stop_relay_success": stop_relay_success,
    }

    func = FUNC_MAP[sys.argv[1]]
    if len(sys.argv) >= 2:
        func(*sys.argv[2:])
    else:
        func()
