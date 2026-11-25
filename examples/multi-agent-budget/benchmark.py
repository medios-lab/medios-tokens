#!/usr/bin/env python3
"""Live side-by-side multi-agent token budget benchmark on Medios.

Runs TWO schedulers against the EXACT same unmodified medios-controller,
medios-node, and medios-tokens (starting with token_budget=100):

  1. Naive queue contention: FIFO queue, blanket worst-case reservations (25),
     no priority, no chain admission, no model-tier downgrading.
  2. Smart budget orchestrator: priority + full-chain admission, right-sized
     step reservations with immediate settlement recycling, and watermark-based
     model-tier routing for low-priority tasks.

Every task launches for real through Medios offers, debits on TASK_ADDED, and
settles actual spend via POST /settle.
"""
import argparse
import getpass
import json
import os
import queue
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def http_get_json(url):
    with urllib.request.urlopen(url, timeout=5) as r:
        return json.loads(r.read().decode("utf-8"))


def http_post_json(url, body, headers=None):
    hdrs = {"Content-Type": "application/json", "Accept": "application/json"}
    if headers:
        hdrs.update(headers)
    req = urllib.request.Request(
        url, data=json.dumps(body).encode("utf-8"), headers=hdrs, method="POST"
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return r.status, r.read().decode("utf-8")


def wait_http(url, timeout=15.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1):
                return
        except Exception:
            time.sleep(0.1)
    raise RuntimeError(f"timed out waiting for {url}")


class SchedulerClient:
    """Minimal RecordIO HTTP v1 scheduler client."""

    def __init__(self, controller_url, name):
        self.controller_url = controller_url
        self.name = name
        self.framework_id = None
        self.stream_id = None
        self.events = queue.Queue()
        self.stop_ev = threading.Event()
        self._thread = None

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        deadline = time.time() + 15
        while time.time() < deadline:
            if self.framework_id and self.stream_id:
                return
            time.sleep(0.05)
        raise RuntimeError(f"scheduler {self.name} failed to subscribe")

    def _run(self):
        payload = json.dumps({
            "type": "SUBSCRIBE",
            "subscribe": {
                "framework_info": {
                    "user": getpass.getuser(),
                    "name": self.name,
                }
            },
        }).encode("utf-8")
        req = urllib.request.Request(
            f"{self.controller_url}/api/v1/scheduler",
            data=payload,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                self.stream_id = resp.headers.get("Medios-Stream-Id")
                while not self.stop_ev.is_set():
                    line = resp.readline()
                    if not line:
                        break
                    line = line.strip()
                    if not line:
                        continue
                    size = int(line)
                    raw = b""
                    while len(raw) < size:
                        chunk = resp.read(size - len(raw))
                        if not chunk:
                            return
                        raw += chunk
                    ev = json.loads(raw.decode("utf-8"))
                    if ev.get("type") == "SUBSCRIBED":
                        self.framework_id = ev["subscribed"]["framework_id"]["value"]
                    else:
                        self.events.put(ev)
        except Exception:
            pass

    def call(self, body):
        body = dict(body)
        body["framework_id"] = {"value": self.framework_id}
        return http_post_json(
            f"{self.controller_url}/api/v1/scheduler",
            body,
            headers={"Medios-Stream-Id": self.stream_id},
        )

    def decline(self, offer_id, refuse_seconds=0.2):
        self.call({
            "type": "DECLINE",
            "decline": {
                "offer_ids": [offer_id],
                "filters": {"refuse_seconds": refuse_seconds},
            },
        })

    def launch_task(self, offer, task_id, reserve_budget, actual_spend, admin_url):
        tb_res = None
        for r in offer.get("resources", []):
            if r.get("name") == "token_budget" and r.get("type") == "SCALAR":
                tb_res = r
                break
        assert tb_res is not None, "offer missing token_budget"
        # Task simulates LLM call and immediately settles actual spend via POST /settle.
        cmd = (
            f"sleep 0.15 && "
            f"curl -sfS -X POST {admin_url}/settle "
            f"-H 'Content-Type: application/json' "
            f"-d '{{\"task_id\":\"{task_id}\",\"actual\":{actual_spend}}}' >/dev/null"
        )
        task = {
            "name": task_id,
            "task_id": {"value": task_id},
            "node_id": offer["node_id"],
            "resources": [
                {"name": "cpus", "type": "SCALAR", "scalar": {"value": 0.2}},
                {"name": "mem", "type": "SCALAR", "scalar": {"value": 64}},
                {
                    "name": "token_budget",
                    "type": "SCALAR",
                    "scalar": {"value": reserve_budget},
                    "provider_id": tb_res["provider_id"],
                },
            ],
            "command": {"shell": True, "value": cmd},
        }
        self.call({
            "type": "ACCEPT",
            "accept": {
                "offer_ids": [offer["id"]],
                "operations": [{"type": "LAUNCH", "launch": {"task_infos": [task]}}],
            },
        })

    def ack(self, status):
        if status.get("uuid"):
            self.call({
                "type": "ACKNOWLEDGE",
                "acknowledge": {
                    "node_id": status["node_id"],
                    "task_id": status["task_id"],
                    "uuid": status["uuid"],
                },
            })

    def teardown(self):
        self.stop_ev.set()
        if self.framework_id and self.stream_id:
            try:
                self.call({"type": "TEARDOWN"})
            except Exception:
                pass


def offer_token_budget(offer):
    for r in offer.get("resources", []):
        if r.get("name") == "token_budget" and r.get("type") == "SCALAR":
            return float(r["scalar"]["value"])
    return 0.0


def run_cluster_case(mode, medios_build, provider_bin):
    ctrl_bin = os.path.join(medios_build, "src/controller/medios-controller")
    node_bin = os.path.join(medios_build, "src/node/medios-node")
    launcher_dir = os.path.join(medios_build, "src")

    ctrl_port = free_port()
    node_port = free_port()
    admin_port = free_port()
    ctrl_url = f"http://127.0.0.1:{ctrl_port}"
    node_url = f"http://127.0.0.1:{node_port}"
    admin_url = f"http://127.0.0.1:{admin_port}"

    env = os.environ.copy()
    env["LIBPROCESS_IP"] = "127.0.0.1"
    procs = []

    with tempfile.TemporaryDirectory(prefix=f"medios-multi-{mode}-") as tmpdir:
        os.makedirs(f"{tmpdir}/ctrl", exist_ok=True)
        os.makedirs(f"{tmpdir}/node", exist_ok=True)
        try:
            procs.append(subprocess.Popen(
                [
                    ctrl_bin,
                    "--ip=127.0.0.1",
                    f"--port={ctrl_port}",
                    "--registry=in_memory",
                    f"--work_dir={tmpdir}/ctrl",
                    "--allocation_interval=200ms",
                ],
                env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            ))
            wait_http(f"{ctrl_url}/health")

            procs.append(subprocess.Popen(
                [
                    node_bin,
                    "--ip=127.0.0.1",
                    f"--port={node_port}",
                    f"--controller=127.0.0.1:{ctrl_port}",
                    f"--work_dir={tmpdir}/node",
                    f"--launcher_dir={launcher_dir}",
                    "--containerizers=medios",
                    "--launcher=posix",
                    "--isolation=",
                    "--resources=cpus:4;mem:2048;disk:2048",
                    "--switch_user=false",
                    '--executor_environment_variables={"LIBPROCESS_IP":"127.0.0.1"}',
                ],
                env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            ))
            wait_http(f"{node_url}/health")
            time.sleep(0.8)

            procs.append(subprocess.Popen(
                [
                    provider_bin,
                    f"--controller={ctrl_url}",
                    f"--node={node_url}",
                    "--resource=token_budget",
                    "--initial=100",
                    "--cap=100",
                    f"--state-file={tmpdir}/ledger.json",
                    f"--admin-addr=127.0.0.1:{admin_port}",
                ],
                env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            ))
            wait_http(f"{admin_url}/balance")
            time.sleep(0.5)

            if mode == "naive":
                return run_naive_queue(ctrl_url, admin_url)
            else:
                return run_smart_orchestrator(ctrl_url, admin_url)
        finally:
            for p in reversed(procs):
                p.terminate()
            for p in reversed(procs):
                try:
                    p.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    p.kill()


def make_workflows():
    """6 workflows: 4 high-value 3-step research chains + 2 low-value digests."""
    high = []
    for i in range(1, 5):
        high.append({
            "id": f"research-{i}",
            "priority": "high",
            "steps": [
                {"name": "plan", "tight_res": 8.0, "actual": 5.0},
                {"name": "gather", "tight_res": 10.0, "actual": 8.0},
                {"name": "synthesize", "tight_res": 9.0, "actual": 7.0},
            ],
            "next_step": 0,
            "spent": 0.0,
            "done": False,
        })
    low = []
    for i in range(1, 3):
        low.append({
            "id": f"digest-{i}",
            "priority": "low",
            "steps": [
                {"name": "summarize", "tight_res": 20.0, "actual": 18.0, "cheap_res": 8.0, "cheap_actual": 6.0},
            ],
            "next_step": 0,
            "spent": 0.0,
            "done": False,
        })
    return high, low


def run_naive_queue(ctrl_url, admin_url):
    """Case 1: FIFO queue with blanket worst-case reservations (25) and no coordination."""
    sched = SchedulerClient(ctrl_url, "naive-fifo-queue")
    sched.start()
    high, low = make_workflows()
    # Interleaved FIFO arrival: background digests and research steps contend directly.
    ready = [low[0], high[0], high[1], low[1], high[2], high[3]]
    in_flight = {}
    wf_by_id = {w["id"]: w for w in high + low}
    t0 = time.time()
    last_progress = t0
    blanket_reserve = 25.0

    try:
        while time.time() - last_progress < 3.5:
            try:
                ev = sched.events.get(timeout=0.2)
            except queue.Empty:
                if not ready and not in_flight:
                    break
                continue

            etype = ev.get("type")
            if etype == "UPDATE":
                st = ev["update"]["status"]
                sched.ack(st)
                tid = st["task_id"]["value"]
                if st["state"] in ("TASK_FINISHED", "TASK_FAILED", "TASK_KILLED") and tid in in_flight:
                    wf_id, step_actual = in_flight.pop(tid)
                    wf = wf_by_id[wf_id]
                    wf["spent"] += step_actual
                    wf["next_step"] += 1
                    if wf["next_step"] >= len(wf["steps"]):
                        wf["done"] = True
                    else:
                        ready.append(wf)
                    last_progress = time.time()

            elif etype == "OFFERS":
                for offer in ev["offers"]["offers"]:
                    tb = offer_token_budget(offer)
                    if not ready or tb < blanket_reserve:
                        sched.decline(offer["id"])
                        continue
                    wf = ready.pop(0)
                    step = wf["steps"][wf["next_step"]]
                    tid = f"{wf['id']}-{step['name']}"
                    sched.launch_task(offer, tid, blanket_reserve, step["actual"], admin_url)
                    in_flight[tid] = (wf["id"], step["actual"])
                    last_progress = time.time()
    finally:
        sched.teardown()

    bal = http_get_json(f"{admin_url}/balance")["balance"]
    high_done = sum(1 for w in high if w["done"])
    low_done = sum(1 for w in low if w["done"])
    stranded_wasted = sum(w["spent"] for w in high + low if not w["done"])
    return {
        "mode": "Naive FIFO Queue Contention",
        "high_done": f"{high_done}/{len(high)}",
        "low_done": f"{low_done}/{len(low)}",
        "total_done": f"{high_done + low_done}/{len(high) + len(low)}",
        "stranded_wasted": round(stranded_wasted, 2),
        "remaining_budget": round(bal, 2),
        "elapsed_s": round(time.time() - t0, 2),
    }


def run_smart_orchestrator(ctrl_url, admin_url):
    """Case 2: Budget-aware orchestrator with pre-planning, tight reservations, and model routing."""
    sched = SchedulerClient(ctrl_url, "smart-budget-orchestrator")
    sched.start()
    high, low = make_workflows()
    admitted_high = []
    pending_high = list(high)
    pending_low = list(low)
    in_flight = {}
    wf_by_id = {w["id"]: w for w in high + low}
    t0 = time.time()
    last_progress = t0
    watermark = 25.0  # Below this remaining budget, route low-priority work to cheap model.

    try:
        while time.time() - last_progress < 4.0:
            if not pending_high and not admitted_high and not pending_low and not in_flight:
                break
            try:
                ev = sched.events.get(timeout=0.2)
            except queue.Empty:
                continue

            etype = ev.get("type")
            if etype == "UPDATE":
                st = ev["update"]["status"]
                sched.ack(st)
                tid = st["task_id"]["value"]
                if st["state"] in ("TASK_FINISHED", "TASK_FAILED", "TASK_KILLED") and tid in in_flight:
                    wf_id, step_actual = in_flight.pop(tid)
                    wf = wf_by_id[wf_id]
                    wf["spent"] += step_actual
                    wf["next_step"] += 1
                    if wf["next_step"] >= len(wf["steps"]):
                        wf["done"] = True
                    elif wf["priority"] == "high":
                        admitted_high.append(wf)
                    last_progress = time.time()

            elif etype == "OFFERS":
                for offer in ev["offers"]["offers"]:
                    tb = offer_token_budget(offer)
                    # 1. Finish already-admitted high-value workflows first.
                    chosen = None
                    res_amt = 0.0
                    act_amt = 0.0
                    for idx, wf in enumerate(admitted_high):
                        step = wf["steps"][wf["next_step"]]
                        if tb >= step["tight_res"]:
                            chosen = admitted_high.pop(idx)
                            res_amt, act_amt = step["tight_res"], step["actual"]
                            break

                    # 2. Pre-plan admission of a new high-value workflow only if full chain fits.
                    if chosen is None and pending_high:
                        # Account for remaining committed spend of active high workflows.
                        committed = sum(
                            sum(s["actual"] for s in w["steps"][w["next_step"]:])
                            for w in admitted_high
                        )
                        wf_next = pending_high[0]
                        chain_cost = sum(s["actual"] for s in wf_next["steps"])
                        step0 = wf_next["steps"][0]
                        if tb - committed >= chain_cost and tb >= step0["tight_res"]:
                            chosen = pending_high.pop(0)
                            res_amt, act_amt = step0["tight_res"], step0["actual"]

                    # 3. Run low-value background tasks once high-value queue is clear,
                    #    downgrading to cheap model tier when budget is below watermark.
                    if chosen is None and not pending_high and not admitted_high and not in_flight and pending_low:
                        wf_low = pending_low[0]
                        step = wf_low["steps"][0]
                        use_cheap = tb <= watermark
                        need_res = step["cheap_res"] if use_cheap else step["tight_res"]
                        need_act = step["cheap_actual"] if use_cheap else step["actual"]
                        if tb >= need_res:
                            chosen = pending_low.pop(0)
                            res_amt, act_amt = need_res, need_act

                    if chosen is None:
                        sched.decline(offer["id"])
                        continue

                    step = chosen["steps"][chosen["next_step"]]
                    tid = f"{chosen['id']}-{step['name']}"
                    sched.launch_task(offer, tid, res_amt, act_amt, admin_url)
                    in_flight[tid] = (chosen["id"], act_amt)
                    last_progress = time.time()
    finally:
        sched.teardown()

    bal = http_get_json(f"{admin_url}/balance")["balance"]
    high_done = sum(1 for w in high if w["done"])
    low_done = sum(1 for w in low if w["done"])
    stranded_wasted = sum(w["spent"] for w in high + low if not w["done"])
    return {
        "mode": "Smart Budget Orchestrator",
        "high_done": f"{high_done}/{len(high)}",
        "low_done": f"{low_done}/{len(low)}",
        "total_done": f"{high_done + low_done}/{len(high) + len(low)}",
        "stranded_wasted": round(stranded_wasted, 2),
        "remaining_budget": round(bal, 2),
        "elapsed_s": round(time.time() - t0, 2),
    }


def main():
    ap = argparse.ArgumentParser(description="Live multi-agent token budget benchmark on Medios")
    ap.add_argument("--medios-build", required=True, help="Path to Medios build directory")
    ap.add_argument("--provider-bin", required=True, help="Path to medios-tokens binary")
    args = ap.parse_args()

    r1 = run_cluster_case("naive", args.medios_build, args.provider_bin)
    r2 = run_cluster_case("smart", args.medios_build, args.provider_bin)

    print("\n=== Multi-Agent Token Budget Benchmark (100-unit pool, 6 workflows) ===")
    print(f"{'Metric':<28} {'Case 1: Naive Queue':<24} {'Case 2: Smart Orchestrator'}")
    print("-" * 78)
    print(f"{'High-value workflows (3-step)':<28} {r1['high_done']:<24} {r2['high_done']}")
    print(f"{'Low-value digests':<28} {r1['low_done']:<24} {r2['low_done']}")
    print(f"{'Total completed':<28} {r1['total_done']:<24} {r2['total_done']}")
    print(f"{'Wasted on stranded work':<28} {str(r1['stranded_wasted']):<24} {r2['stranded_wasted']}")
    print(f"{'Remaining token_budget':<28} {str(r1['remaining_budget']):<24} {r2['remaining_budget']}")
    print(f"{'Wall time (s)':<28} {str(r1['elapsed_s']):<24} {r2['elapsed_s']}")

    assert r1["high_done"] == "0/4", f"expected naive contention to strand high workflows, got {r1}"
    assert r2["total_done"] == "6/6", f"expected smart orchestrator to finish all 6 workflows, got {r2}"
    assert r2["stranded_wasted"] == 0.0, f"expected 0 wasted spend in smart mode, got {r2}"
    print("\nBENCHMARK VERIFIED: same broker, same medios-tokens, policy in the scheduler.")


if __name__ == "__main__":
    main()
