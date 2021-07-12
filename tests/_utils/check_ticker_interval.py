import sys


def check_worker_ticker_interval(logpath, min_interval):
    logs = []
    with open(logpath, "r") as f:
        for line in f.readlines()[::-1]:
            if len(logs) == 2:
                break
            if "no job in queue, update lag to zero" in line:
                logs.append(line)
    ts1 = logs[0].split('"current ts"=')[1].split("]")[0]
    ts2 = logs[1].split('"current ts"=')[1].split("]")[0]
    if abs(int(ts2) - int(ts1)) < int(min_interval):
        raise Exception(
            "check_worker_ticker_interval faild ts1={} ts2={} min_interval={}".format(
                ts1, ts2, min_interval
            )
        )


if __name__ == "__main__":
    check_worker_ticker_interval(sys.argv[1], sys.argv[2])
