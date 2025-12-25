import os
import time
import socket
import requests
import psutil  # 用来取 CPU / 内存

# 控制器的上报接口（用 k8s 服务域名）
REPORT_ENDPOINT = os.getenv(
    "REPORT_ENDPOINT",
    "http://yw-controller.yw-insitu.svc.cluster.local:8080/report"
)

# 当前节点的名字，从环境变量里拿，拿不到就用主机名
NODE_NAME = os.getenv("NODE_NAME") or socket.gethostname()


def collect_and_report():
    # CPU 使用率：0~1 之间
    cpu = psutil.cpu_percent(interval=1) / 100.0
    # 内存使用率：0~1 之间
    mem = psutil.virtual_memory().percent / 100.0

    payload = {
        "node": NODE_NAME,
        "cpu_usage": cpu,
        "mem_usage": mem,
        "extra": {}
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
