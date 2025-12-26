import os
import time
import socket
import platform
import requests
import psutil

# 控制器的上报地址，默认指向集群内服务
REPORT_ENDPOINT = os.getenv(
    "REPORT_ENDPOINT",
    "http://yw-controller.yw-insitu.svc.cluster.local:8080/report"
)

# 节点名称、角色可通过环境覆盖
NODE_NAME = os.getenv("NODE_NAME") or socket.gethostname()
NODE_ROLE = os.getenv("NODE_ROLE", "edge")


def get_temp_c() -> float | None:
    try:
        temps = psutil.sensors_temperatures()
        if not temps:
            return None
        # 取第一个传感器的当前值
        first_key = next(iter(temps))
        entries = temps[first_key]
        if entries:
            return entries[0].current
    except Exception:
        return None
    return None


def collect_and_report():
    cpu = psutil.cpu_percent(interval=1) / 100.0
    mem = psutil.virtual_memory().percent / 100.0
    disk = psutil.disk_usage("/")
    disk_free_pct = disk.free / disk.total if disk.total else None

    payload = {
        "node": NODE_NAME,
        "cpu_usage": cpu,
        "mem_usage": mem,
        "arch": platform.machine(),
        "role": NODE_ROLE,
        "disk_free_pct": disk_free_pct,
        "temp_c": get_temp_c(),
        "extra": {},
    }

    try:
        resp = requests.post(REPORT_ENDPOINT, json=payload, timeout=3)
        print("report ok:", resp.status_code, payload, flush=True)
    except Exception as e:
        print("report failed:", e, flush=True)


if __name__ == "__main__":
    while True:
        collect_and_report()
        time.sleep(5)
