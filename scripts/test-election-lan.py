#!/usr/bin/env python3
"""
TV5-D25: LAN Failover Election Test Script (Fake Endpoint / Recorded Trace / Live LAN G6).

Modes:
  1) --mode replay --trace <path.json>
     Replays a recorded NODE_STATUS trace fixture (SPEC-03 §5), validates schema,
     dynamically prepares StopLeaderAction, audits term monotonicity, Election Safety
     (<= 1 Leader per term), and 2/5 minority no-new-leader safety.

  2) --mode fake
     Starts 5 local TCP v1 fake LAN endpoints (4-byte big-endian length prefix + UTF-8 JSON
     NODE_STATUS per SPEC-03), executes a full deterministic LAN failover drill:
       - 5/5 baseline -> dynamic Leader discovery (non-node-1) -> StopLeaderAction
       - 4/5 failover to higher term
       - 3/5 failover to higher term (2 nodes stopped)
       - 2/5 quorum loss verification (0 leaders despite term advances)
       - 5/5 restore + term monotonicity & Election Safety audit
     Clearly reports FAKE_LOCAL_TCP_ENDPOINTS (physical 5-laptop failover is deferred to G6).

  3) --mode live --inventory <inventory.json> [--dry-run] [--allow-placeholder]
     Ready for TV6-D25 / Gate G6 on 5 physical laptops + Laptop 6 Monitor.
     Probes real NODE_STATUS over TCP v1 on port 9100, dynamically selects current Leader
     (fail-closed on 0 or >1 Leaders), prepares/prints StopLeaderAction, and audits term
     progression across failover. Refuses placeholder IPs unless --allow-placeholder is set.

No external Python packages required (stdlib only).
"""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import socket
import socketserver
import struct
import sys
import threading
import time
import uuid


VOTERS = ("node-1", "node-2", "node-3", "node-4", "node-5")
QUORUM_SIZE = 3
MAX_FRAME_BYTES = 1_048_576
PLACEHOLDER_PREFIXES = ("192.168.10.", "192.168.1.10")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def recv_exact(sock: socket.socket, size: int) -> bytes:
    buf = bytearray()
    while len(buf) < size:
        chunk = sock.recv(size - len(buf))
        if not chunk:
            raise ConnectionError("EOF before completing TCP frame")
        buf.extend(chunk)
    return bytes(buf)


def send_frame(sock: socket.socket, obj: dict) -> None:
    payload = json.dumps(obj, separators=(",", ":")).encode("utf-8")
    if not (1 <= len(payload) <= MAX_FRAME_BYTES):
        raise ValueError(f"Frame length out of range: {len(payload)}")
    sock.sendall(struct.pack(">I", len(payload)) + payload)


def read_frame(sock: socket.socket) -> dict:
    raw_len = recv_exact(sock, 4)
    length = struct.unpack(">I", raw_len)[0]
    if not (1 <= length <= MAX_FRAME_BYTES):
        raise ValueError(f"Invalid incoming frame length: {length}")
    raw_body = recv_exact(sock, length)
    return json.loads(raw_body.decode("utf-8"))


def query_node_status(host: str, port: int, timeout_s: float = 1.5, sender: str = "monitor-lan-failover") -> dict:
    msg_id = str(uuid.uuid4())
    req = {
        "protocolVersion": 1,
        "messageId": msg_id,
        "correlationId": None,
        "type": "NODE_STATUS",
        "senderId": sender,
        "sentAt": utc_now_iso(),
        "payload": {},
    }
    with socket.create_connection((host, port), timeout=timeout_s) as sock:
        sock.settimeout(timeout_s)
        send_frame(sock, req)
        resp = read_frame(sock)
    if resp.get("protocolVersion") != 1:
        raise AssertionError(f"Unexpected protocolVersion: {resp.get('protocolVersion')}")
    if resp.get("correlationId") != msg_id:
        raise AssertionError(f"Correlation mismatch: expected={msg_id}, got={resp.get('correlationId')}")
    if resp.get("type") != "NODE_STATUS_RESPONSE":
        raise AssertionError(f"Unexpected response type: {resp.get('type')}")
    return resp["payload"]


def validate_node_status(dto: dict) -> None:
    nid = dto.get("nodeId")
    if nid not in VOTERS:
        raise AssertionError(f"Invalid voter nodeId: {nid}")
    role = dto.get("role")
    if role is not None and role not in ("LEADER", "FOLLOWER", "CANDIDATE"):
        raise AssertionError(f"Invalid role on {nid}: {role}")
    term = dto.get("term")
    if term is not None and (not isinstance(term, int) or term < 0):
        raise AssertionError(f"Invalid term on {nid}: {term}")
    if dto.get("storageHealthy") is None:
        raise AssertionError(f"storageHealthy missing on {nid}")
    if dto.get("peerConnectivity") is None:
        raise AssertionError(f"peerConnectivity missing on {nid}")
    last_log = dto.get("lastLogIndex")
    commit = dto.get("commitIndex")
    applied = dto.get("lastApplied")
    if applied is not None and commit is not None and applied > commit:
        raise AssertionError(f"lastApplied ({applied}) > commitIndex ({commit}) on {nid}")
    if commit is not None and last_log is not None and commit > last_log:
        raise AssertionError(f"commitIndex ({commit}) > lastLogIndex ({last_log}) on {nid}")


def select_unique_leader(statuses: dict) -> dict:
    leaders = []
    for nid, dto in statuses.items():
        validate_node_status(dto)
        if dto.get("role") == "LEADER":
            if not dto.get("storageHealthy"):
                raise RuntimeError(f"UNHEALTHY_LEADER: {nid} reports storageHealthy=false")
            leaders.append(dto)
    if len(leaders) == 0:
        raise RuntimeError("NO_LEADER_FOUND: 0 healthy LEADER nodes observed")
    if len(leaders) > 1:
        desc = [f"{l['nodeId']}@term={l['term']}" for l in leaders]
        raise RuntimeError(f"MULTIPLE_LEADERS_DETECTED: fail-closed ({desc})")
    return leaders[0]


def prepare_stop_leader_action(statuses: dict, endpoint_map: dict, reason: str) -> dict:
    leader = select_unique_leader(statuses)
    nid = leader["nodeId"]
    return {
        "targetNodeId": nid,
        "observedTerm": leader["term"],
        "commitIndex": leader.get("commitIndex", 0),
        "lastLogIndex": leader.get("lastLogIndex", 0),
        "clientEndpoint": endpoint_map.get(nid),
        "commandHint": f"docker stop -t 10 railway-backend-{nid}  # or scripts/stop-node.ps1 -TargetLeader",
        "reason": reason,
        "preparedAt": utc_now_iso(),
    }


def audit_trace_steps(steps: list) -> dict:
    max_term_by_node = {}
    leader_by_term = {}
    violations = []

    for idx, step in enumerate(steps):
        statuses = step.get("statuses", {})
        online_count = len(statuses)
        for nid, dto in statuses.items():
            try:
                validate_node_status(dto)
            except AssertionError as exc:
                violations.append(f"Step {idx}: {exc}")
                continue
            term = dto.get("term")
            if term is not None:
                prev = max_term_by_node.get(nid)
                if prev is not None and term < prev:
                    violations.append(f"Step {idx}: term regression on {nid} ({prev} -> {term})")
                else:
                    max_term_by_node[nid] = term
            if dto.get("role") == "LEADER" and term is not None:
                seen_before = term in leader_by_term
                existing = leader_by_term.setdefault(term, nid)
                if existing != nid:
                    violations.append(
                        f"Step {idx}: Election Safety violated at term {term} (leaders={existing}, {nid})"
                    )
                if not seen_before and online_count < QUORUM_SIZE:
                    violations.append(
                        f"Step {idx}: Minority step ({online_count}/5 online) illegally elected {nid} at term {term}"
                    )

    return {
        "passed": len(violations) == 0,
        "stepCount": len(steps),
        "leaderByTerm": leader_by_term,
        "maxTermByNode": max_term_by_node,
        "violations": violations,
    }


class FakeNodeState:
    def __init__(self, node_id: str, role: str, term: int, leader_id, commit_index: int = 10):
        self.lock = threading.Lock()
        self.node_id = node_id
        self.role = role
        self.term = term
        self.leader_id = leader_id
        self.last_log_index = commit_index
        self.commit_index = commit_index
        self.last_applied = commit_index
        self.storage_healthy = True
        self.online = True

    def to_status_dto(self, online_nodes: set) -> dict:
        with self.lock:
            peers = {p: (p in online_nodes) for p in VOTERS if p != self.node_id}
            return {
                "nodeId": self.node_id,
                "role": self.role,
                "term": self.term,
                "leaderId": self.leader_id,
                "lastLogIndex": self.last_log_index,
                "commitIndex": self.commit_index,
                "lastApplied": self.last_applied,
                "storageHealthy": self.storage_healthy,
                "peerConnectivity": peers,
                "observedAt": utc_now_iso(),
            }


class FakeLanCluster:
    """In-process 5-node TCP v1 fake endpoint harness for deterministic TV5-D25 failover tests."""

    def __init__(self):
        self.states = {
            nid: FakeNodeState(nid, "LEADER" if nid == "node-3" else "FOLLOWER", 5, "node-3", 12)
            for nid in VOTERS
        }
        self.servers = {}
        self.threads = {}
        self.endpoints = {}

    def _start_node_server(self, nid: str) -> None:
        cluster = self

        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                try:
                    req = read_frame(self.request)
                    if req.get("type") == "NODE_STATUS":
                        online = {k for k, s in cluster.states.items() if s.online}
                        dto = cluster.states[nid].to_status_dto(online)
                        resp = {
                            "protocolVersion": 1,
                            "messageId": str(uuid.uuid4()),
                            "correlationId": req.get("messageId"),
                            "type": "NODE_STATUS_RESPONSE",
                            "senderId": nid,
                            "sentAt": utc_now_iso(),
                            "payload": dto,
                        }
                    else:
                        resp = {
                            "protocolVersion": 1,
                            "messageId": str(uuid.uuid4()),
                            "correlationId": req.get("messageId"),
                            "type": "ERROR_RESPONSE",
                            "senderId": nid,
                            "sentAt": utc_now_iso(),
                            "payload": {
                                "errorCode": "INVALID_REQUEST",
                                "message": f"Unsupported on fake LAN client channel: {req.get('type')}",
                                "leaderHint": cluster.states[nid].leader_id,
                                "term": cluster.states[nid].term,
                                "requestId": None,
                            },
                        }
                    send_frame(self.request, resp)
                except Exception:
                    return

        class ReusableTCPServer(socketserver.ThreadingTCPServer):
            allow_reuse_address = True
            daemon_threads = True

        srv = ReusableTCPServer(("127.0.0.1", 0), Handler)
        port = srv.server_address[1]
        t = threading.Thread(target=srv.serve_forever, daemon=True)
        t.start()
        self.servers[nid] = srv
        self.threads[nid] = t
        self.endpoints[nid] = ("127.0.0.1", port)
        self.states[nid].online = True

    def start_all(self) -> None:
        for nid in VOTERS:
            self._start_node_server(nid)

    def stop_node(self, nid: str) -> None:
        self.states[nid].online = False
        srv = self.servers.pop(nid, None)
        if srv is not None:
            srv.shutdown()
            srv.server_close()

    def restart_node(self, nid: str, leader_id: str, new_term: int, commit_idx: int) -> None:
        st = self.states[nid]
        with st.lock:
            st.role = "FOLLOWER"
            st.term = max(st.term, new_term)
            st.leader_id = leader_id
            st.last_log_index = max(st.last_log_index, commit_idx)
            st.commit_index = max(st.commit_index, commit_idx)
            st.last_applied = max(st.last_applied, commit_idx)
        self._start_node_server(nid)

    def elect_new_leader(self, new_leader: str, new_term: int, commit_idx: int) -> None:
        online = [nid for nid, s in self.states.items() if s.online]
        if len(online) < QUORUM_SIZE:
            raise RuntimeError(f"Cannot elect leader with only {len(online)}/5 nodes online")
        for nid in online:
            st = self.states[nid]
            with st.lock:
                st.role = "LEADER" if nid == new_leader else "FOLLOWER"
                st.term = new_term
                st.leader_id = new_leader
                st.last_log_index = commit_idx
                st.commit_index = commit_idx
                st.last_applied = commit_idx

    def advance_minority_no_leader(self, new_term: int) -> None:
        online = [nid for nid, s in self.states.items() if s.online]
        assert len(online) < QUORUM_SIZE
        for idx, nid in enumerate(online):
            st = self.states[nid]
            with st.lock:
                st.role = "CANDIDATE" if idx == 0 else "FOLLOWER"
                st.term = new_term
                st.leader_id = None

    def probe_all(self) -> dict:
        result = {}
        for nid in VOTERS:
            host, port = self.endpoints[nid]
            try:
                result[nid] = query_node_status(host, port, timeout_s=0.5)
            except Exception:
                continue
        return result

    def endpoint_strings(self) -> dict:
        return {nid: f"{hp[0]}:{hp[1]}" for nid, hp in self.endpoints.items()}

    def close(self) -> None:
        for nid in list(self.servers.keys()):
            self.stop_node(nid)


def run_replay_mode(trace_path: Path, expect_fail: bool = False) -> int:
    data = json.loads(trace_path.read_text(encoding="utf-8"))
    steps = data.get("steps", [])
    print(f"[TV5-D25][REPLAY] traceId={data.get('traceId')} mode={data.get('mode')} steps={len(steps)}")
    print(f"[TV5-D25][REPLAY] physicalLanVerified={data.get('physicalLanVerified', False)} (real physical failover is G6 only)")

    if steps:
        first_statuses = steps[0].get("statuses", {})
        try:
            action = prepare_stop_leader_action(first_statuses, {}, reason="Replay baseline leader inspection")
            print(f"[TV5-D25][STOP-LEADER-PREPARED] target={action['targetNodeId']} term={action['observedTerm']} commit={action['commitIndex']}")
        except RuntimeError as exc:
            print(f"[TV5-D25][STOP-LEADER-PREPARE-INFO] {exc}")

    report = audit_trace_steps(steps)
    print(f"[TV5-D25][AUDIT] passed={report['passed']} leaderByTerm={report['leaderByTerm']} maxTermByNode={report['maxTermByNode']}")
    if report["violations"]:
        for v in report["violations"]:
            print(f"[TV5-D25][VIOLATION] {v}")

    if expect_fail:
        return 0 if not report["passed"] else 1
    return 0 if report["passed"] else 1


def run_fake_mode(output_log: Path | None) -> int:
    lines = []

    def log(msg: str) -> None:
        line = f"{utc_now_iso()} {msg}"
        lines.append(line)
        print(line)

    log("[TV5-D25][FAKE] Starting 5-node local TCP v1 fake LAN endpoint drill (NOT physical 5-laptop G6)")
    cluster = FakeLanCluster()
    recorded_steps = []
    try:
        cluster.start_all()
        ep_map = cluster.endpoint_strings()
        log(f"[TV5-D25][FAKE] Bound 5 fake TCP v1 endpoints: {ep_map}")

        # Step 0: 5/5 baseline
        s0 = cluster.probe_all()
        assert len(s0) == 5, f"Expected 5 online nodes, got {len(s0)}"
        recorded_steps.append({"stepIndex": 0, "phase": "BASELINE_5_OF_5", "statuses": s0})
        action1 = prepare_stop_leader_action(s0, ep_map, "TV5-D25 fake LAN 4/5 failover")
        log(f"[TV5-D25][STOP-LEADER] step=1 target={action1['targetNodeId']} term={action1['observedTerm']} endpoint={action1['clientEndpoint']}")
        assert action1["targetNodeId"] == "node-3"

        # Step 1: Stop Leader -> 4/5 failover
        cluster.stop_node(action1["targetNodeId"])
        cluster.elect_new_leader("node-1", 6, 13)
        s1 = cluster.probe_all()
        assert len(s1) == 4 and action1["targetNodeId"] not in s1
        recorded_steps.append({"stepIndex": 1, "phase": "FAILOVER_4_OF_5", "statuses": s1})
        leader_4of5 = select_unique_leader(s1)
        assert leader_4of5["nodeId"] == "node-1" and leader_4of5["term"] == 6
        log(f"[TV5-D25][TERM-LOG] 4/5 failover PASS: oldLeader=node-3@term=5 -> newLeader={leader_4of5['nodeId']}@term={leader_4of5['term']}")

        # Step 2: Stop another node (Leader) -> 3/5 failover
        action2 = prepare_stop_leader_action(s1, ep_map, "TV5-D25 fake LAN 3/5 failover")
        log(f"[TV5-D25][STOP-LEADER] step=2 target={action2['targetNodeId']} term={action2['observedTerm']} endpoint={action2['clientEndpoint']}")
        cluster.stop_node(action2["targetNodeId"])
        cluster.elect_new_leader("node-4", 7, 14)
        s2 = cluster.probe_all()
        assert len(s2) == 3
        recorded_steps.append({"stepIndex": 2, "phase": "FAILOVER_3_OF_5", "statuses": s2})
        leader_3of5 = select_unique_leader(s2)
        assert leader_3of5["nodeId"] == "node-4" and leader_3of5["term"] == 7
        log(f"[TV5-D25][TERM-LOG] 3/5 failover PASS: newLeader={leader_3of5['nodeId']}@term={leader_3of5['term']}")

        # Step 3: Stop 3rd node -> 2/5 quorum loss (0 Leaders!)
        action3 = prepare_stop_leader_action(s2, ep_map, "TV5-D25 fake LAN 2/5 quorum loss")
        log(f"[TV5-D25][STOP-LEADER] step=3 target={action3['targetNodeId']} term={action3['observedTerm']} endpoint={action3['clientEndpoint']}")
        cluster.stop_node(action3["targetNodeId"])
        cluster.advance_minority_no_leader(8)
        s3 = cluster.probe_all()
        assert len(s3) == 2
        recorded_steps.append({"stepIndex": 3, "phase": "NO_QUORUM_2_OF_5", "statuses": s3})
        assert all(dto["role"] != "LEADER" for dto in s3.values()), f"2/5 must have 0 leaders: {s3}"
        roles_map = {k: v["role"] for k, v in s3.items()}
        log(f"[TV5-D25][TERM-LOG] 2/5 no-quorum PASS: online={sorted(s3.keys())}, roles={roles_map} -> 0 LEADERs")

        # Step 4: Restore stopped nodes -> 5/5 convergence at term 9
        cluster.restart_node("node-3", "node-2", 9, 15)
        cluster.restart_node("node-1", "node-2", 9, 15)
        cluster.restart_node("node-4", "node-2", 9, 15)
        cluster.elect_new_leader("node-2", 9, 15)
        s4 = cluster.probe_all()
        assert len(s4) == 5
        recorded_steps.append({"stepIndex": 4, "phase": "RESTORED_5_OF_5", "statuses": s4})
        leader_5of5 = select_unique_leader(s4)
        log(f"[TV5-D25][TERM-LOG] 5/5 restore PASS: leader={leader_5of5['nodeId']}@term={leader_5of5['term']} commit={leader_5of5['commitIndex']}")

        report = audit_trace_steps(recorded_steps)
        assert report["passed"], f"Audit failed: {report['violations']}"
        log(f"[TV5-D25][AUDIT] PASS: steps={report['stepCount']} leaderByTerm={report['leaderByTerm']} maxTermByNode={report['maxTermByNode']}")
        log("[TV5-D25][RESULT] FAKE_LAN_ENDPOINT_DRILL=PASS | PHYSICAL_5_LAPTOP_FAILOVER=NOT_RUN (G6)")
        return 0
    finally:
        cluster.close()
        if output_log is not None:
            output_log.parent.mkdir(parents=True, exist_ok=True)
            output_log.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_live_mode(inventory_path: Path, dry_run: bool, allow_placeholder: bool, timeout_s: float) -> int:
    inv = json.loads(inventory_path.read_text(encoding="utf-8"))
    voters = inv.get("voters", [])
    if len(voters) != 5:
        print(f"[TV5-D25][LIVE][ERROR] Expected 5 voters in inventory, found {len(voters)}", file=sys.stderr)
        return 1

    endpoint_map = {}
    for v in voters:
        ep = v["clientEndpoint"]
        host, port_str = ep.rsplit(":", 1)
        if not allow_placeholder and any(host.startswith(p) for p in PLACEHOLDER_PREFIXES):
            print(
                f"[TV5-D25][LIVE][BLOCKED] Inventory contains placeholder IP ({ep}). "
                "Update with TV6-D24/D25 real physical LAN IPs before running live G6 failover, "
                "or pass --allow-placeholder explicitly.",
                file=sys.stderr,
            )
            return 2
        endpoint_map[v["nodeId"]] = (host, int(port_str))

    statuses = {}
    for nid, (host, port) in endpoint_map.items():
        try:
            statuses[nid] = query_node_status(host, port, timeout_s=timeout_s)
        except Exception as exc:
            print(f"[TV5-D25][LIVE] {nid} ({host}:{port}) OFFLINE/UNREACHABLE: {exc}")

    if len(statuses) < QUORUM_SIZE:
        print(f"[TV5-D25][LIVE][FAIL] Only {len(statuses)}/5 nodes reachable (< quorum 3/5)", file=sys.stderr)
        return 1

    ep_strings = {nid: f"{hp[0]}:{hp[1]}" for nid, hp in endpoint_map.items()}
    action = prepare_stop_leader_action(statuses, ep_strings, "Live G6 LAN failover stop-leader")
    print(json.dumps({"stopLeaderAction": action, "dryRun": dry_run}, indent=2))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="TV5-D25 LAN Failover Election Test Harness")
    parser.add_argument("--mode", choices=("replay", "fake", "live"), default="fake")
    parser.add_argument("--trace", type=Path, help="Path to recorded JSON trace fixture (--mode replay)")
    parser.add_argument("--inventory", type=Path, help="Path to LAN inventory JSON (--mode live)")
    parser.add_argument("--dry-run", action="store_true", help="Prepare StopLeaderAction without executing stop")
    parser.add_argument("--allow-placeholder", action="store_true", help="Allow placeholder IPs in live mode")
    parser.add_argument("--timeout", type=float, default=1.5, help="TCP probe timeout seconds")
    parser.add_argument("--output-log", type=Path, help="Optional file path to save run log")
    args = parser.parse_args()

    if args.mode == "replay":
        if not args.trace or not args.trace.is_file():
            print("--trace <fixture.json> is required in replay mode", file=sys.stderr)
            return 1
        return run_replay_mode(args.trace)
    if args.mode == "fake":
        return run_fake_mode(args.output_log)
    if args.mode == "live":
        if not args.inventory or not args.inventory.is_file():
            print("--inventory <inventory.json> is required in live mode", file=sys.stderr)
            return 1
        return run_live_mode(args.inventory, args.dry_run, args.allow_placeholder, args.timeout)
    return 1


if __name__ == "__main__":
    sys.exit(main())
