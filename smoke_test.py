#!/usr/bin/env python3
"""Read-only smoke test for codex-app-mcp.

Spawns server.py as a real MCP subprocess, performs the MCP handshake, then
exercises the read-path tools (list_threads, thread_status, read_thread,
search_threads) against your real Codex threads. It never starts a turn and
never mutates anything.

Requires the `codex` CLI on PATH with at least one existing thread.

Usage: python3 smoke_test.py [path/to/server.py]
"""

import json
import os
import subprocess
import sys
import threading

SERVER = (
    sys.argv[1]
    if len(sys.argv) > 1
    else os.path.join(os.path.dirname(os.path.abspath(__file__)), "server.py")
)

proc = subprocess.Popen(
    [sys.executable, SERVER],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.DEVNULL,
    text=True,
    bufsize=1,
)

pending = {}
lock = threading.Lock()


def reader():
    for line in proc.stdout:
        line = line.strip()
        if not line:
            continue
        msg = json.loads(line)
        if "id" in msg:
            with lock:
                holder = pending.pop(msg["id"], None)
            if holder:
                holder["msg"] = msg
                holder["event"].set()


threading.Thread(target=reader, daemon=True).start()

_id = 0


def request(method, params, timeout=120):
    global _id
    _id += 1
    holder = {"event": threading.Event(), "msg": None}
    with lock:
        pending[_id] = holder
    proc.stdin.write(
        json.dumps({"jsonrpc": "2.0", "id": _id, "method": method, "params": params}) + "\n"
    )
    proc.stdin.flush()
    if not holder["event"].wait(timeout):
        raise TimeoutError(method)
    return holder["msg"]


def notify(method, params):
    proc.stdin.write(json.dumps({"jsonrpc": "2.0", "method": method, "params": params}) + "\n")
    proc.stdin.flush()


def call_tool(name, arguments):
    r = request("tools/call", {"name": name, "arguments": arguments})
    res = r.get("result", {})
    payload = json.loads(res["content"][0]["text"])
    return payload, bool(res.get("isError"))


failures = []

r = request(
    "initialize",
    {"protocolVersion": "2025-06-18", "clientInfo": {"name": "smoke", "version": "0"}},
)
info = r.get("result", {}).get("serverInfo", {})
print("initialize:", info)
if info.get("name") != "codex-app-mcp":
    failures.append("initialize")
notify("notifications/initialized", {})

r = request("ping", {})
print("ping:", r.get("result"))

r = request("tools/list", {})
tools = r.get("result", {}).get("tools", [])
print("tools/list: %d tools: %s" % (len(tools), ", ".join(t["name"] for t in tools)))
if len(tools) != 11:
    failures.append("tools/list")

payload, is_error = call_tool("list_threads", {"limit": 3})
print("list_threads isError=%s count=%s" % (is_error, payload.get("count")))
if is_error or "threads" not in payload:
    failures.append("list_threads")
    print(json.dumps(payload, indent=2)[:2000])
else:
    for t in payload["threads"]:
        print("  -", t["id"], "|", (t.get("name") or "")[:40], "|", t.get("status"))

if payload.get("threads"):
    tid = payload["threads"][0]["id"]

    p, is_error = call_tool("thread_status", {"thread_id": tid})
    print(
        "thread_status isError=%s status=%s last_turn.status=%s"
        % (is_error, p.get("status"), (p.get("last_turn") or {}).get("status"))
    )
    if is_error:
        failures.append("thread_status")

    p, is_error = call_tool("read_thread", {"thread_id": tid, "turn_limit": 2})
    print("read_thread isError=%s turns=%s" % (is_error, len(p.get("turns", []))))
    if is_error:
        failures.append("read_thread")

    p, is_error = call_tool("search_threads", {"query": "a", "limit": 2})
    print("search_threads isError=%s count=%s" % (is_error, p.get("count")))
    if is_error:
        failures.append("search_threads")
else:
    print("note: no threads found; skipped thread_status/read_thread/search_threads")

proc.stdin.close()
proc.wait(timeout=15)
print("server exit code:", proc.returncode)

if failures:
    print("FAILURES:", failures)
    sys.exit(1)
print("SMOKE OK")
