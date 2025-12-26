import os
import time
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException
import requests
from kubernetes import client, config
from kubernetes.client import ApiException
from kubernetes.config.config_exception import ConfigException
from pydantic import BaseModel, Field

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------
NAMESPACE = os.getenv("NAMESPACE", "yw-insitu")
NODE_NAME_PREFIX = os.getenv("NODE_NAME_PREFIX", "raspberrypi")
STATE_TTL_SECONDS = 300
TASK_LABEL_KEY = "yw.insitu/task"
KANIKO_IMAGE = os.getenv("KANIKO_IMAGE", "gcr.io/kaniko-project/executor:latest")
KANIKO_INSECURE = os.getenv("KANIKO_INSECURE", "true").lower() in ("1", "true", "yes")
INSECURE_REGISTRIES = [
    r.strip()
    for r in os.getenv("INSECURE_REGISTRIES", "121.250.211.145:5000").split(",")
    if r.strip()
]
REGISTRY_ADDR = os.getenv("REGISTRY_ADDR", "121.250.211.145:5000")
REGISTRY_SCHEME = os.getenv("REGISTRY_SCHEME", "http")

app = FastAPI(title="YW Controller", version="1.0.0")

# ------------------------------------------------------------------------------
# In-memory node state (agent reports)
# ------------------------------------------------------------------------------
# node_states[node_name] = {
#   "cpu_usage": float,
#   "mem_usage": float,
#   "extra": dict,
#   "ts": float
# }
node_states: Dict[str, dict] = {}


def _cleanup_stale_nodes(now: float) -> None:
    stale = [n for n, st in node_states.items() if now - st.get("ts", 0) > STATE_TTL_SECONDS]
    for n in stale:
        node_states.pop(n, None)


# ------------------------------------------------------------------------------
# Pydantic models
# ------------------------------------------------------------------------------
class Report(BaseModel):
    node: str
    cpu_usage: float = Field(..., ge=0.0, le=1.0)
    mem_usage: float = Field(..., ge=0.0, le=1.0)
    extra: Optional[dict] = None


class NodeRequirement(BaseModel):
    max_cpu_usage: float = 0.8
    max_mem_usage: float = 0.8
    min_alive_seconds: float = 15.0
    node_name_prefix: Optional[str] = NODE_NAME_PREFIX


class SelectRequest(BaseModel):
    num_nodes: int = 1
    requirement: NodeRequirement = NodeRequirement()


class SelectedNode(BaseModel):
    node: str
    cpu_usage: float
    mem_usage: float
    score: float


class SelectResponse(BaseModel):
    nodes: List[SelectedNode]


class Resources(BaseModel):
    requests: Optional[Dict[str, str]] = None
    limits: Optional[Dict[str, str]] = None


class TaskCreateRequest(BaseModel):
    name: str
    image: str
    replicas: int = 1
    command: Optional[List[str]] = None
    args: Optional[List[str]] = None
    env: Optional[Dict[str, str]] = None
    resources: Optional[Resources] = None
    node_names: Optional[List[str]] = None  # explicit node list
    node_selector: Optional[Dict[str, str]] = None  # label selector
    select_num_nodes: int = 1  # used when auto-select
    requirement: NodeRequirement = NodeRequirement()


class TaskInfo(BaseModel):
    name: str
    namespace: str
    image: str
    replicas: int
    available_replicas: int = 0
    ready_replicas: int = 0
    node_selector: Optional[Dict[str, str]] = None
    allowed_nodes: Optional[List[str]] = None
    labels: Dict[str, str] = {}


class PodBrief(BaseModel):
    name: str
    phase: str
    ready: bool
    node: Optional[str] = None
    start_time: Optional[float] = None


class TaskStatus(BaseModel):
    task: TaskInfo
    pods: List[PodBrief]


class RegistryCatalog(BaseModel):
    repositories: List[str]


class RegistryTags(BaseModel):
    name: str
    tags: List[str] | None


class BuildSpec(BaseModel):
    context: str  # git repo URL or tarball URL supported by kaniko
    dockerfile: str = "Dockerfile"
    destination: str
    tag: str = "arm64-latest"
    build_args: Optional[Dict[str, str]] = None
    node_selector: Dict[str, str] = Field(default_factory=lambda: {"kubernetes.io/arch": "arm64"})
    timeout_seconds: int = 600
    wait_for_completion: bool = True
    init_image: str = os.getenv("INIT_IMAGE", "121.250.211.145:5000/python:3.11-slim")


class DeploySpec(TaskCreateRequest):
    image: Optional[str] = None


class BuildAndDeployRequest(BaseModel):
    build: BuildSpec
    deploy: DeploySpec


# ------------------------------------------------------------------------------
# Kubernetes client helpers
# ------------------------------------------------------------------------------
def get_k8s_clients():
    if hasattr(app.state, "apps_v1"):
        return app.state.apps_v1, app.state.core_v1
    try:
        config.load_incluster_config()
        mode = "in-cluster"
    except ConfigException:
        config.load_kube_config(config_file=os.getenv("KUBECONFIG"))
        mode = "kubeconfig"
    app.state.apps_v1 = client.AppsV1Api()
    app.state.core_v1 = client.CoreV1Api()
    app.state.k8s_mode = mode
    return app.state.apps_v1, app.state.core_v1


def get_batch_client():
    if hasattr(app.state, "batch_v1"):
        return app.state.batch_v1
    if not hasattr(app.state, "apps_v1"):
        get_k8s_clients()
    app.state.batch_v1 = client.BatchV1Api()
    return app.state.batch_v1


def build_node_affinity(node_names: List[str]) -> client.V1NodeAffinity:
    term = client.V1NodeSelectorTerm(
        match_expressions=[
            client.V1NodeSelectorRequirement(
                key="kubernetes.io/hostname",
                operator="In",
                values=node_names,
            )
        ]
    )
    return client.V1NodeAffinity(
        required_during_scheduling_ignored_during_execution=client.V1NodeSelector(
            node_selector_terms=[term]
        )
    )


def build_resources(res: Optional[Resources]) -> Optional[client.V1ResourceRequirements]:
    if not res:
        return None
    return client.V1ResourceRequirements(
        requests=res.requests,
        limits=res.limits,
    )


def deployment_to_task_info(dep: client.V1Deployment) -> TaskInfo:
    tmpl = dep.spec.template
    containers = tmpl.spec.containers or []
    image = containers[0].image if containers else ""
    node_selector = tmpl.spec.node_selector
    affinity = tmpl.spec.affinity
    allowed_nodes: Optional[List[str]] = None

    if affinity and affinity.node_affinity:
        req = affinity.node_affinity.required_during_scheduling_ignored_during_execution
        if req and req.node_selector_terms:
            for term in req.node_selector_terms:
                for expr in term.match_expressions or []:
                    if expr.key == "kubernetes.io/hostname":
                        allowed_nodes = expr.values

    return TaskInfo(
        name=dep.metadata.name,
        namespace=dep.metadata.namespace,
        image=image,
        replicas=dep.spec.replicas or 0,
        available_replicas=dep.status.available_replicas or 0,
        ready_replicas=dep.status.ready_replicas or 0,
        node_selector=node_selector,
        allowed_nodes=allowed_nodes,
        labels=dep.metadata.labels or {},
    )


def list_pods_for_task(task_name: str) -> List[PodBrief]:
    _, core_v1 = get_k8s_clients()
    selector = f"{TASK_LABEL_KEY}={task_name}"
    try:
        pods = core_v1.list_namespaced_pod(namespace=NAMESPACE, label_selector=selector)
    except ApiException as e:
        raise HTTPException(status_code=e.status or 500, detail=e.reason)

    result: List[PodBrief] = []
    for pod in pods.items:
        cs = pod.status.container_statuses or []
        ready = all(c.ready for c in cs) if cs else False
        start_time = pod.status.start_time.timestamp() if pod.status.start_time else None
        result.append(
            PodBrief(
                name=pod.metadata.name,
                phase=pod.status.phase or "Unknown",
                ready=ready,
                node=(pod.spec.node_name or None),
                start_time=start_time,
            )
        )
    return result


def registry_base_url() -> str:
    return f"{REGISTRY_SCHEME}://{REGISTRY_ADDR}"


def fetch_registry_catalog() -> RegistryCatalog:
    url = f"{registry_base_url()}/v2/_catalog"
    try:
        resp = requests.get(url, timeout=5, verify=False)
        resp.raise_for_status()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Registry catalog fetch failed: {e}")
    data = resp.json()
    repos = data.get("repositories", [])
    return RegistryCatalog(repositories=repos)


def fetch_registry_tags(repo: str) -> RegistryTags:
    url = f"{registry_base_url()}/v2/{repo}/tags/list"
    try:
        resp = requests.get(url, timeout=5, verify=False)
        resp.raise_for_status()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Registry tags fetch failed: {e}")
    data = resp.json()
    return RegistryTags(name=data.get("name", repo), tags=data.get("tags"))


def create_kaniko_job(build: BuildSpec, task_name: str) -> str:
    batch_v1 = get_batch_client()
    timestamp = int(time.time())
    job_name = f"build-{task_name}-{timestamp}"

    # If context is http (not https), Kaniko does not support it directly.
    # Use an init container to download and extract into /workspace/src, then use dir:// context.
    use_http_init = build.context.startswith("http://")
    kaniko_context = build.context
    context_mounts = []
    volumes = []
    init_containers = []

    if use_http_init:
        kaniko_context = "dir:///workspace/src"
        context_mounts = [client.V1VolumeMount(name="context", mount_path="/workspace/src")]
        volumes.append(client.V1Volume(name="context", empty_dir=client.V1EmptyDirVolumeSource()))
        init_cmd = (
            "import urllib.request, tarfile, io, os; "
            f"url='{build.context}'; "
            "data=urllib.request.urlopen(url).read(); "
            "os.makedirs('/workspace/src', exist_ok=True); "
            "tarfile.open(fileobj=io.BytesIO(data), mode='r:gz').extractall('/workspace/src')"
        )
        init_containers.append(
            client.V1Container(
                name="fetch-context",
                image=build.init_image,
                command=["python", "-c", init_cmd],
                volume_mounts=context_mounts,
            )
        )

    args = [
        f"--context={kaniko_context}",
        f"--dockerfile={build.dockerfile}",
        f"--destination={build.destination}:{build.tag}",
        "--verbosity=info",
    ]
    if KANIKO_INSECURE:
        args.extend(["--insecure", "--skip-tls-verify", "--insecure-pull", "--skip-tls-verify-pull"])
        for reg in INSECURE_REGISTRIES:
            args.append(f"--insecure-registry={reg}")
            args.append(f"--skip-tls-verify-registry={reg}")
    for k, v in (build.build_args or {}).items():
        args.append(f"--build-arg={k}={v}")

    container = client.V1Container(
        name="kaniko",
        image=KANIKO_IMAGE,
        args=args,
        env=[client.V1EnvVar(name="DOCKER_CONFIG", value="/kaniko/.docker/")],
        volume_mounts=context_mounts,
    )

    pod_spec = client.V1PodSpec(
        init_containers=init_containers,
        containers=[container],
        restart_policy="Never",
        service_account_name="yw-controller",
        node_selector=build.node_selector,
        volumes=volumes,
    )

    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={"job": job_name}),
        spec=pod_spec,
    )

    job_spec = client.V1JobSpec(
        template=template,
        backoff_limit=1,
    )

    job = client.V1Job(
        metadata=client.V1ObjectMeta(name=job_name, namespace=NAMESPACE),
        spec=job_spec,
    )

    try:
        batch_v1.create_namespaced_job(namespace=NAMESPACE, body=job)
    except ApiException as e:
        raise HTTPException(status_code=e.status or 500, detail=e.reason)

    return job_name


def wait_for_job(job_name: str, timeout_seconds: int) -> None:
    batch_v1 = get_batch_client()
    start = time.time()
    while True:
        try:
            job = batch_v1.read_namespaced_job(job_name, NAMESPACE)
        except ApiException as e:
            raise HTTPException(status_code=e.status or 500, detail=e.reason)

        status = job.status
        if status.succeeded and status.succeeded > 0:
            return
        if status.failed and status.failed > 0:
            raise HTTPException(status_code=500, detail=f"Build job {job_name} failed")

        if time.time() - start > timeout_seconds:
            raise HTTPException(status_code=504, detail=f"Build job {job_name} timed out")

        time.sleep(3)


# ------------------------------------------------------------------------------
# Core endpoints
# ------------------------------------------------------------------------------
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
    """Return current node states (from agents)."""
    now = time.time()
    _cleanup_stale_nodes(now)
    result = {}
    for node, st in node_states.items():
        alive = now - st.get("ts", 0) <= STATE_TTL_SECONDS
        result[node] = {**st, "alive": alive}
    return result


def _select_nodes(req: SelectRequest) -> List[SelectedNode]:
    now = time.time()
    _cleanup_stale_nodes(now)
    candidates: List[SelectedNode] = []
    for node, st in node_states.items():
        prefix = req.requirement.node_name_prefix
        if prefix and not node.startswith(prefix):
            continue
        cpu = st.get("cpu_usage", 1.0)
        mem = st.get("mem_usage", 1.0)
        ts = st.get("ts", 0)
        if now - ts > req.requirement.min_alive_seconds:
            continue
        if cpu > req.requirement.max_cpu_usage:
            continue
        if mem > req.requirement.max_mem_usage:
            continue
        score = (1.0 - cpu) + (1.0 - mem)
        candidates.append(
            SelectedNode(node=node, cpu_usage=cpu, mem_usage=mem, score=score)
        )
    if not candidates:
        raise HTTPException(status_code=400, detail="No suitable nodes found")
    candidates.sort(key=lambda x: x.score, reverse=True)
    return candidates[: req.num_nodes]


@app.post("/select_nodes", response_model=SelectResponse)
def select_nodes(req: SelectRequest):
    selected = _select_nodes(req)
    return SelectResponse(nodes=selected)


# ------------------------------------------------------------------------------
# Task lifecycle (Deployment-based)
# ------------------------------------------------------------------------------
@app.post("/tasks", response_model=TaskInfo)
def create_task(req: TaskCreateRequest):
    apps_v1, _ = get_k8s_clients()

    # Decide placement
    node_names = req.node_names
    affinity = None
    node_selector = req.node_selector
    if not node_names and not node_selector:
        auto_req = SelectRequest(num_nodes=req.select_num_nodes, requirement=req.requirement)
        selected = _select_nodes(auto_req)
        node_names = [n.node for n in selected]
    if node_names:
        affinity = client.V1Affinity(node_affinity=build_node_affinity(node_names))

    labels = {
        TASK_LABEL_KEY: req.name,
        "app": req.name,
    }

    container = client.V1Container(
        name=req.name,
        image=req.image,
        command=req.command,
        args=req.args,
        env=[client.V1EnvVar(name=k, value=v) for k, v in (req.env or {}).items()],
        resources=build_resources(req.resources),
    )

    pod_spec = client.V1PodSpec(
        containers=[container],
        restart_policy="Always",
        node_selector=node_selector,
        affinity=affinity,
    )

    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels=labels),
        spec=pod_spec,
    )

    dep_spec = client.V1DeploymentSpec(
        replicas=req.replicas,
        selector=client.V1LabelSelector(match_labels={"app": req.name}),
        template=template,
    )

    dep = client.V1Deployment(
        metadata=client.V1ObjectMeta(name=req.name, labels=labels, namespace=NAMESPACE),
        spec=dep_spec,
    )

    try:
        created = apps_v1.create_namespaced_deployment(namespace=NAMESPACE, body=dep)
    except ApiException as e:
        raise HTTPException(status_code=e.status or 500, detail=e.reason)

    return deployment_to_task_info(created)


@app.get("/tasks", response_model=List[TaskInfo])
def list_tasks():
    apps_v1, _ = get_k8s_clients()
    try:
        deps = apps_v1.list_namespaced_deployment(
            namespace=NAMESPACE,
            label_selector=f"{TASK_LABEL_KEY}",
        )
    except ApiException as e:
        raise HTTPException(status_code=e.status or 500, detail=e.reason)
    return [deployment_to_task_info(d) for d in deps.items]


@app.get("/tasks/{name}", response_model=TaskInfo)
def get_task(name: str):
    apps_v1, _ = get_k8s_clients()
    try:
        dep = apps_v1.read_namespaced_deployment(name=name, namespace=NAMESPACE)
    except ApiException as e:
        raise HTTPException(status_code=e.status or 500, detail=e.reason)
    return deployment_to_task_info(dep)


@app.delete("/tasks/{name}")
def delete_task(name: str):
    apps_v1, _ = get_k8s_clients()
    try:
        apps_v1.delete_namespaced_deployment(name=name, namespace=NAMESPACE)
    except ApiException as e:
        raise HTTPException(status_code=e.status or 500, detail=e.reason)
    return {"ok": True}


@app.get("/tasks/{name}/status", response_model=TaskStatus)
def get_task_status(name: str):
    task = get_task(name)
    pods = list_pods_for_task(name)
    return TaskStatus(task=task, pods=pods)


# ------------------------------------------------------------------------------
# Registry helpers
# ------------------------------------------------------------------------------
@app.get("/registry/catalog", response_model=RegistryCatalog)
def registry_catalog():
    return fetch_registry_catalog()


@app.get("/registry/{repo}/tags", response_model=RegistryTags)
def registry_tags(repo: str):
    return fetch_registry_tags(repo)


# ------------------------------------------------------------------------------
# Build + Deploy (Kaniko Job -> Deployment)
# ------------------------------------------------------------------------------
@app.post("/tasks/build-and-deploy", response_model=TaskInfo)
def build_and_deploy(req: BuildAndDeployRequest):
    # 1) create kaniko build job
    job_name = create_kaniko_job(req.build, req.deploy.name)
    if req.build.wait_for_completion:
        wait_for_job(job_name, req.build.timeout_seconds)

    # 2) deploy using built image
    image_ref = f"{req.build.destination}:{req.build.tag}"
    deploy_payload = req.deploy.model_dump()
    deploy_payload["image"] = image_ref
    task_req = TaskCreateRequest(**deploy_payload)
    return create_task(task_req)
