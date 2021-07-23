import sys
import requests

API_ENDPOINT = "http://127.0.0.1:1323/api/v1/sources"
SOURCE_NAME = "mysql-01"

create_source_req = {
    "case_sensitive": False,
    "enable_gtid": False,
    "host": "127.0.0.1",
    "password": "123456",
    "port": 3306,
    "source_name": SOURCE_NAME,
    "user": "root",
}


def create_source_by_openapi_success():
    resp = requests.post(url=API_ENDPOINT, json=create_source_req)
    assert resp.status_code == 201
    print("create_source_by_openapi_success resp=", resp.json())


def create_source_by_openapi_faild():
    resp = requests.post(url=API_ENDPOINT, json=create_source_req)
    assert resp.status_code == 400
    print("create_source_by_openapi_faild resp=", resp.json())


def list_source_by_openapi_success(soucce_count):
    resp = requests.get(url=API_ENDPOINT)
    assert resp.status_code == 200
    data = resp.json()
    # only create one source
    assert len(data) == soucce_count
    print("list_source_by_openapi_success resp=", data)


def delete_source_success(source_name):
    resp = requests.delete(url=API_ENDPOINT + "/" + source_name)
    assert resp.status_code == 204
    print("delete_source_success")


def delete_source_failed(source_name):
    resp = requests.delete(url=API_ENDPOINT + "/" + source_name)
    assert resp.status_code == 400
    print("delete_source_failed msg=", resp.json())


if __name__ == "__main__":
    func = sys.argv[1]

    if func == "source_success":
        create_source_by_openapi_success()
    elif func == "source_failed":
        create_source_by_openapi_faild()
    elif func == "source_list_success":
        list_source_by_openapi_success(int(sys.argv[2]))
    elif func == "delete_source_success":
        delete_source_success(SOURCE_NAME)
    elif func == "delete_source_failed":
        delete_source_failed(SOURCE_NAME)
