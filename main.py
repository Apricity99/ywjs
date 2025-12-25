# main.py
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Dict
import time

app = FastAPI()

# 用内存简单存一下节点状态（重启会丢，先不管）
node_states: Dict[str, dict] = {}

class Report(BaseModel):
    node: str
    cpu_usage: float
    mem_usage: float
    extra: dict | None = None

@app.post("/report")
def report_state(r: Report):
    node_states[r.node] = {
        "cpu_usage": r.cpu_usage,
        "mem_usage": r.mem_usage,
        "extra": r.extra or {},
        "ts": time.time(),
    }
    return {"ok": True}

@app.get("/nodes")
def list_nodes():
    return node_states
