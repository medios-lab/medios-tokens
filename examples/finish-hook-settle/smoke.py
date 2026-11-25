#!/usr/bin/env python3
"""End-to-end smoke test for Pattern 1: Task Finish Hook settlement.

Verifies three scenarios against live medios-controller + medios-node + medios-tokens:
  1. Successful task reserves 25, spends 18 -> finish hook settles -> balance 100 -> 75 -> 82.
  2. Failing task (exit 1) reserves 25, spends 10 -> finish hook STILL settles -> balance 82 -> 57 -> 72.
  3. Task without usage.json (crash/missing file) reserves 25 -> fail-closed -> balance stays 47 (no refund).
"""

import argparse
import http.client
import json
import os
import pwd
import shutil
import subprocess
import threading
import time
from pathlib import Path

CTRL_HOST, CTRL_PORT = "127.0.0.1", 15070
NODE_HOST, NODE_PORT = "127.0.0.1", 15071
ADMIN_PORT = 19097


def post_json(host, port, path, body, headers=None):
    conn = http.client.HTTPConnection(host, port, timeout=10)
    hdrs = {"Content-Type": "application/json", "Accept": "application/json"}
    if headers:
        hdrs.update(headers)
    conn.request("POST", path, body=json.dumps(body).encode("utf-8"), headers=hdrs)
    resp = conn.getresponse()
    data = resp.read().decode("utf-8", errors="replace")
    conn.close()
    return resp.status, data


def wait_http(host, port, path, timeout=10.0):
    dl = time.time() + timeout
    while time.time() < dl:
        try:
            conn = http.client.HTTPConnection(host, port, timeout=1)
            conn.request("GET", path)
            r = conn.getresponse()
            r.read()
            conn.close()
            if r.status == 200:
                return True
        except Exception:
            pass
        time.sleep(0.2)
    return False


def carve_scalar(offer, name, amount):
    for res in offer.get("resources", []):
        if res.get("name") == name and res.get("type") == "SCALAR":
            if res.get("scalar", {}).get("value", 0) >= amount:
                c = json.loads(json.dumps(res))
                c["scalar"]["value"] = amount
                return c
    return None


def offer_scalars(offer):
    return {
        r["name"]: round(float(r["scalar"]["value"]), 4)
        for r in offer.get("resources", [])
        if r.get("type") == "SCALAR"
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--medios-build", required=True, help="Path to medios CMake build directory")
    ap.add_argument("--provider-bin", required=True, help="Path to compiled medios-tokens binary")
    ap.add_argument("--use-native-finish-hook", action="store_true", help="Attach TaskInfo.finish (requires medios v0.1.14+)")
    args = ap.parse_args()

    build = Path(args.medios_build).resolve()
    ctrl_bin = build / "src/controller/medios-controller"
    node_bin = build / "src/node/medios-node"
    launcher_dir = build / "src"
    example_dir = Path(__file__).resolve().parent
    work_sh = example_dir / "work.sh"
    settle_sh = example_dir / "settle-hook.sh"

    work = Path("/tmp/medios-finish-hook-smoke")
    if work.exists():
        shutil.rmtree(work)
    (work / "controller").mkdir(parents=True)
    (work / "node").mkdir(parents=True)
    state_file = work / "ledger.json"

    env = os.environ.copy()
    env["LIBPROCESS_IP"] = "127.0.0.1"
    ctrl_log = open(work / "controller.log", "w")
    node_log = open(work / "node.log", "w")
    prov_log = open(work / "provider.log", "w")

    ctrl = node = prov = fw_conn = None
    stop_evt = threading.Event()
    try:
        ctrl = subprocess.Popen(
            [str(ctrl_bin), f"--ip={CTRL_HOST}", f"--port={CTRL_PORT}", "--registry=in_memory", f"--work_dir={work / 'controller'}"],
            env=env, stdout=ctrl_log, stderr=subprocess.STDOUT,
        )
        assert wait_http(CTRL_HOST, CTRL_PORT, "/health"), "controller did not start"

        node = subprocess.Popen(
            [
                str(node_bin),
                f"--controller={CTRL_HOST}:{CTRL_PORT}",
                f"--ip={NODE_HOST}", f"--port={NODE_PORT}",
                f"--work_dir={work / 'node'}",
                "--containerizers=medios", "--launcher=posix", "--isolation=",
                "--switch_user=false", f"--launcher_dir={launcher_dir}",
                '--executor_environment_variables={"LIBPROCESS_IP":"127.0.0.1"}',
                "--resources=cpus:2;mem:1024;disk:1024",
            ],
            env=env, stdout=node_log, stderr=subprocess.STDOUT,
        )
        assert wait_http(NODE_HOST, NODE_PORT, "/health"), "node did not start"
        time.sleep(0.8)

        prov = subprocess.Popen(
            [
                str(Path(args.provider_bin).resolve()),
                f"--controller=http://{CTRL_HOST}:{CTRL_PORT}",
                f"--node=http://{NODE_HOST}:{NODE_PORT}",
                "--resource=token_budget",
                "--initial=100", "--cap=100",
                f"--state-file={state_file}",
                f"--admin-addr=127.0.0.1:{ADMIN_PORT}",
            ],
            stdout=prov_log, stderr=subprocess.STDOUT,
        )
        assert wait_http("127.0.0.1", ADMIN_PORT, "/balance"), "medios-tokens did not start"

        # Subscribe minimal framework
        user = pwd.getpwuid(os.getuid()).pw_name
        fw_conn = http.client.HTTPConnection(CTRL_HOST, CTRL_PORT, timeout=None)
        fw_conn.request(
            "POST", "/api/v1/scheduler",
            body=json.dumps({
                "type": "SUBSCRIBE",
                "subscribe": {
                    "framework_info": {
                        "user": user, "name": "finish-hook-smoke",
                        "principal": "team:smoke", "roles": ["*"],
                        "capabilities": [{"type": "MULTI_ROLE"}],
                    }
                },
            }).encode("utf-8"),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        resp = fw_conn.getresponse()
        assert resp.status == 200
        stream_id = resp.getheader("Medios-Stream-Id")

        lock = threading.Lock()
        fw_id = [None]
        offers = []
        statuses = {}

        def reader():
            buf = b""
            while not stop_evt.is_set():
                chunk = resp.read1(4096) if hasattr(resp, "read1") else resp.read(1)
                if not chunk:
                    break
                buf += chunk
                while b"\n" in buf:
                    sb, _, rest = buf.partition(b"\n")
                    if not sb.strip():
                        buf = rest
                        continue
                    n = int(sb.strip())
                    if len(rest) < n:
                        buf = sb + b"\n" + rest
                        break
                    ev = json.loads(rest[:n].decode("utf-8"))
                    buf = rest[n:]
                    et = ev.get("type")
                    if et == "SUBSCRIBED":
                        with lock:
                            fw_id[0] = ev["subscribed"]["framework_id"]["value"]
                    elif et == "OFFERS":
                        with lock:
                            offers.extend(ev["offers"]["offers"])
                    elif et == "UPDATE":
                        s = ev["update"]["status"]
                        with lock:
                            statuses.setdefault(s["task_id"]["value"], []).append(s["state"])
                        if s.get("uuid") and fw_id[0]:
                            post_json(
                                CTRL_HOST, CTRL_PORT, "/api/v1/scheduler",
                                {
                                    "type": "ACKNOWLEDGE",
                                    "framework_id": {"value": fw_id[0]},
                                    "acknowledge": {"node_id": s["node_id"], "task_id": s["task_id"], "uuid": s["uuid"]},
                                },
                                headers={"Medios-Stream-Id": stream_id},
                            )

        threading.Thread(target=reader, daemon=True).start()
        dl = time.time() + 10
        while time.time() < dl and fw_id[0] is None:
            time.sleep(0.05)
        assert fw_id[0]

        def call(payload):
            payload["framework_id"] = {"value": fw_id[0]}
            return post_json(CTRL_HOST, CTRL_PORT, "/api/v1/scheduler", payload, headers={"Medios-Stream-Id": stream_id})

        def wait_offer(expected_budget, timeout=10.0):
            call({"type": "REVIVE"})
            dl = time.time() + timeout
            while time.time() < dl:
                matched = None
                decline_ids = []
                with lock:
                    while offers:
                        o = offers.pop(0)
                        if matched is None and offer_scalars(o).get("token_budget") == expected_budget:
                            matched = o
                        else:
                            decline_ids.append(o["id"])
                if decline_ids:
                    call({"type": "DECLINE", "decline": {"offer_ids": decline_ids, "filters": {"refuse_seconds": 0.5}}})
                if matched:
                    return matched
                time.sleep(0.05)
            raise TimeoutError(f"timed out waiting for offer with token_budget={expected_budget}")

        def run_case(offer, tid, reserve, work_cmd, expect_state):
            task = {
                "name": tid,
                "task_id": {"value": tid},
                "node_id": offer["node_id"],
                "principal": "operator:smoke",
                "resources": [
                    carve_scalar(offer, "cpus", 0.2),
                    carve_scalar(offer, "mem", 64),
                    carve_scalar(offer, "token_budget", reserve),
                ],
            }
            admin_url = f"http://127.0.0.1:{ADMIN_PORT}"
            if args.use_native_finish_hook:
                task["command"] = {"shell": True, "value": work_cmd}
                task["finish"] = {
                    "command": {
                        "shell": True,
                        "value": f"MEDIOS_TOKENS_ADMIN_URL={admin_url} {settle_sh}",
                    },
                    "cap": {"nanoseconds": 15_000_000_000},
                }
            else:
                task["command"] = {
                    "shell": True,
                    "value": f"trap 'MEDIOS_TASK_ID={tid} MEDIOS_TOKENS_ADMIN_URL={admin_url} {settle_sh}' EXIT; {work_cmd}",
                }
            st, body = call({
                "type": "ACCEPT",
                "accept": {
                    "offer_ids": [offer["id"]],
                    "operations": [{"type": "LAUNCH", "launch": {"task_infos": [task]}}],
                },
            })
            assert st == 202, f"ACCEPT {tid} failed: {st} {body}"
            terminal = {"TASK_FINISHED", "TASK_FAILED", "TASK_KILLED", "TASK_ERROR", "TASK_LOST"}
            dl = time.time() + 15
            while time.time() < dl:
                with lock:
                    hist = statuses.get(tid, [])
                    for s in hist:
                        if s in terminal:
                            assert s == expect_state, f"{tid} ended in {s}, want {expect_state}"
                            return
                time.sleep(0.05)
            raise TimeoutError(f"{tid} did not terminate")

        # Case 1: exit 0, reserved 25, spent 18 -> settles to 82
        o1 = wait_offer(100.0)
        run_case(o1, "settle-ok-1", 25.0, f"{work_sh} --fake-spend 18", "TASK_FINISHED")
        o2 = wait_offer(82.0)
        print(f"1. PASS exit-0 settle (100 - 25 + 7 = 82): offer={offer_scalars(o2)}")

        # Case 2: exit 1 (failure), reserved 25, spent 10 -> hook STILL settles to 72
        run_case(o2, "settle-fail-2", 25.0, f"{work_sh} --fake-spend 10 --fail", "TASK_FAILED")
        o3 = wait_offer(72.0)
        print(f"2. PASS exit-1 settle (82 - 25 + 15 = 72): offer={offer_scalars(o3)}")

        # Case 3: no usage.json written -> fail-closed -> stays at 47 (no refund)
        run_case(o3, "settle-missing-3", 25.0, "echo no-usage-written", "TASK_FINISHED")
        o4 = wait_offer(47.0)
        print(f"3. PASS missing-usage fail-closed (72 - 25 = 47): offer={offer_scalars(o4)}")
        call({"type": "TEARDOWN"})
        print("\n=== ALL 3 FINISH-HOOK SETTLEMENT CASES PASSED ===")
    finally:
        stop_evt.set()
        if fw_conn:
            try:
                fw_conn.close()
            except Exception:
                pass
        for proc in (prov, node, ctrl):
            if proc:
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except Exception:
                    proc.kill()
        ctrl_log.close()
        node_log.close()
        prov_log.close()


if __name__ == "__main__":
    main()
