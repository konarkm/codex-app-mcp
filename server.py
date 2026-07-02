#!/usr/bin/env python3
"""codex-app-mcp — an MCP server that lets Claude Code drive OpenAI Codex CLI threads.

Single-file, Python 3 stdlib only. Speaks newline-delimited JSON-RPC 2.0 to Claude
Code over stdin/stdout (the "front"), and lazily spawns a `codex app-server` child
process that it talks to over its stdio (the "back").

NOTHING but protocol JSON is ever written to stdout; all diagnostics go to stderr.
"""

import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid
from collections import deque, defaultdict

# ----------------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------------

SERVER_NAME = "codex-app-mcp"
SERVER_VERSION = "0.1.0"
DEFAULT_PROTOCOL_VERSION = "2025-06-18"

# Wire defaults for our "YOLO" turn policy.
YOLO_APPROVAL_POLICY = "never"            # AskForApproval string variant
YOLO_THREAD_SANDBOX = "danger-full-access"  # SandboxMode string (thread/start)
YOLO_TURN_SANDBOX_POLICY = {"type": "dangerFullAccess"}  # SandboxPolicy tagged obj

REQUEST_TIMEOUT_SEC = float(os.environ.get("CODEX_APP_MCP_TIMEOUT_SEC", "120"))

NOTIFICATION_BUFFER_SIZE = 300
ITEM_TEXT_TRUNCATE = 2000
PREVIEW_TRUNCATE = 200

# Notification methods we buffer per-thread for thread_status / wait support.
BUFFERED_NOTIFICATION_METHODS = {
    "turn/started",
    "turn/completed",
    "thread/status/changed",
    "item/completed",
    "error",
}

# ServerRequest (server->client) methods that are approval prompts we auto-accept.
# Each maps to the exact response body that means "approve" per the response schema.
APPROVAL_RESPONSES = {
    # ReviewDecision-shaped responses use "approved".
    "execCommandApproval": {"decision": "approved"},
    "applyPatchApproval": {"decision": "approved"},
    # CommandExecution / FileChange approval decisions use "accept".
    "item/commandExecution/requestApproval": {"decision": "accept"},
    "item/fileChange/requestApproval": {"decision": "accept"},
    # Permissions approval: grant an empty (default => no extra restrictions) profile.
    "item/permissions/requestApproval": {"permissions": {}},
}


def log(*args):
    """Write a diagnostic line to stderr (never stdout)."""
    try:
        print("[codex-app-mcp]", *args, file=sys.stderr, flush=True)
    except Exception:
        pass


def truncate(text, limit):
    if text is None:
        return None
    if not isinstance(text, str):
        return text
    if len(text) <= limit:
        return text
    return text[:limit] + "…[truncated]"


# ----------------------------------------------------------------------------
# Codex app-server client (the "back")
# ----------------------------------------------------------------------------


class CodexClient:
    """Manages a single `codex app-server` child process and its JSON-RPC traffic."""

    def __init__(self):
        self._proc = None
        self._lock = threading.Lock()          # guards spawn/handshake lifecycle
        self._write_lock = threading.Lock()    # short-held guard for stdin writes
        self._id_counter = 0
        self._id_lock = threading.Lock()
        self._pending = {}                       # request id -> Event/result holder
        self._pending_lock = threading.Lock()
        self._reader_thread = None
        # Per-thread ring buffers of recently seen notifications.
        self._notifications = defaultdict(lambda: deque(maxlen=NOTIFICATION_BUFFER_SIZE))
        self._notif_lock = threading.Lock()
        # Durable turn-completion tracking, independent of the size-bounded
        # notification ring buffer so completions can never be evicted by
        # item/completed churn on a chatty turn. Keyed by (threadId, turnId).
        self._completed_turns = {}               # (threadId, turnId) -> params
        self._completion_lock = threading.Lock()
        self._restarted_msg = None               # surfaced on the next tool result

    # -- lifecycle ----------------------------------------------------------

    def _alive(self):
        return self._proc is not None and self._proc.poll() is None

    def ensure_started(self):
        """Spawn + handshake the child if it is not running. Returns a restart note or None."""
        with self._lock:
            if self._alive():
                return None
            note = None
            if self._proc is not None:
                note = "codex app-server had exited; respawned a fresh child."
                log("child not alive; respawning")
            self._spawn_locked()
            self._handshake_locked()
            if note:
                self._restarted_msg = note
            return note

    def _spawn_locked(self):
        codex_path = shutil.which("codex")
        if not codex_path:
            raise RuntimeError("`codex` executable not found on PATH")
        # Reap any prior child (e.g. a wedged-but-alive one) before overwriting
        # the handle, so we don't leak its pipes/fds and its reader thread. The
        # reader loop for the old proc exits once we close its stdout.
        old = self._proc
        if old is not None:
            try:
                if old.poll() is None:
                    old.terminate()
                    try:
                        old.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        old.kill()
                        old.wait(timeout=5)
            except Exception as exc:
                log("error reaping previous child:", repr(exc))
            for stream in (old.stdout, old.stdin):
                try:
                    if stream is not None:
                        stream.close()
                except Exception:
                    pass
        log("spawning:", codex_path, "app-server")
        self._proc = subprocess.Popen(
            [codex_path, "app-server"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=sys.stderr,  # let codex logs flow to our stderr
            text=True,
            bufsize=1,  # line-buffered
        )
        # Reset routing state for the new process.
        with self._pending_lock:
            for holder in self._pending.values():
                holder["error"] = {"code": -32000, "message": "child restarted"}
                holder["event"].set()
            self._pending.clear()
        self._reader_thread = threading.Thread(
            target=self._reader_loop, args=(self._proc,), daemon=True
        )
        self._reader_thread.start()

    def _handshake_locked(self):
        # initialize request (must complete before any other method).
        init_params = {
            "clientInfo": {
                "name": SERVER_NAME,
                "title": "Codex App MCP (Claude Code)",
                "version": SERVER_VERSION,
            }
        }
        self._do_request("initialize", init_params)
        # initialized notification acknowledges the handshake.
        self._notify("initialized", {})

    # -- low-level IO -------------------------------------------------------

    def _next_id(self):
        with self._id_lock:
            self._id_counter += 1
            return self._id_counter

    def _write_message(self, msg):
        line = json.dumps(msg, separators=(",", ":")) + "\n"
        # Short-held write lock so the reader thread (answering server-requests)
        # and the request path never interleave bytes on stdin, without either
        # blocking the other for the duration of a request.
        with self._write_lock:
            proc = self._proc
            if proc is None or proc.stdin is None:
                raise RuntimeError("codex app-server stdin is not available")
            proc.stdin.write(line)
            proc.stdin.flush()

    def _notify(self, method, params):
        self._write_message({"jsonrpc": "2.0", "method": method, "params": params})

    def _do_request(self, method, params, timeout=None):
        """Send a request and block for its response.

        Does NOT hold self._lock across the blocking wait. Id allocation, the
        pending-map insert, and the stdin write are each individually
        thread-safe (_id_lock / _pending_lock / _write_lock), so the reader
        thread can answer server-requests and route responses while this call
        is parked on the event.
        """
        req_id = self._next_id()
        holder = {"event": threading.Event(), "result": None, "error": None}
        with self._pending_lock:
            self._pending[req_id] = holder
        self._write_message(
            {"jsonrpc": "2.0", "id": req_id, "method": method, "params": params}
        )
        wait_for = timeout if timeout is not None else REQUEST_TIMEOUT_SEC
        if not holder["event"].wait(wait_for):
            with self._pending_lock:
                self._pending.pop(req_id, None)
            raise TimeoutError(
                "timed out after %ss waiting for codex response to %s" % (wait_for, method)
            )
        if holder["error"] is not None:
            err = holder["error"]
            raise CodexError(err.get("message", "codex error"), err)
        return holder["result"]

    def request(self, method, params, timeout=None):
        """Public: ensure child alive, send a request, return its result dict.

        The spawn/handshake (if needed) is serialized under self._lock, but the
        request itself is issued WITHOUT holding self._lock — otherwise the lock
        would be held across the blocking event.wait() and the reader thread
        could never acquire it to answer a server-request, deadlocking the turn.
        """
        with self._lock:
            if not self._alive():
                # Caller normally pre-warms via ensure_started, but be safe.
                self._spawn_locked()
                self._handshake_locked()
        return self._do_request(method, params, timeout=timeout)

    # -- reader thread ------------------------------------------------------

    def _reader_loop(self, proc):
        try:
            for line in proc.stdout:
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    log("non-JSON line from child:", line[:200])
                    continue
                self._dispatch(msg)
        except Exception as exc:  # pragma: no cover - defensive
            log("reader loop error:", repr(exc))
        finally:
            # Child stdout closed: fail any in-flight requests.
            with self._pending_lock:
                for holder in self._pending.values():
                    if holder["error"] is None and holder["result"] is None:
                        holder["error"] = {"code": -32000, "message": "codex app-server exited"}
                        holder["event"].set()
                self._pending.clear()
            log("reader loop ended (child stdout closed)")

    def _dispatch(self, msg):
        # Response to one of our requests?
        if "id" in msg and ("result" in msg or "error" in msg):
            req_id = msg["id"]
            with self._pending_lock:
                holder = self._pending.pop(req_id, None)
            if holder is not None:
                if "error" in msg:
                    holder["error"] = msg["error"]
                else:
                    holder["result"] = msg.get("result")
                holder["event"].set()
            return
        # Server -> client request (has id + method): must be answered.
        if "id" in msg and "method" in msg:
            self._handle_server_request(msg)
            return
        # Notification (method, no id).
        if "method" in msg:
            self._handle_notification(msg)
            return
        log("unrecognized message from child:", json.dumps(msg)[:200])

    def _handle_server_request(self, msg):
        method = msg.get("method")
        req_id = msg.get("id")
        if method in APPROVAL_RESPONSES:
            result = APPROVAL_RESPONSES[method]
            log("auto-approving server request:", method)
            try:
                self._write_message({"jsonrpc": "2.0", "id": req_id, "result": result})
            except Exception as exc:
                log("failed to answer approval:", repr(exc))
            return
        # Anything else: respond with a JSON-RPC error and log it.
        log("declining unsupported server request:", method)
        try:
            self._write_message(
                {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {
                        "code": -32601,
                        "message": "codex-app-mcp does not handle server request: %s" % method,
                    },
                }
            )
        except Exception as exc:
            log("failed to decline server request:", repr(exc))

    def _handle_notification(self, msg):
        method = msg.get("method")
        if method not in BUFFERED_NOTIFICATION_METHODS:
            return
        params = msg.get("params") or {}
        thread_id = params.get("threadId")
        if not thread_id:
            return
        entry = {"method": method, "params": params, "received_at": time.time()}
        with self._notif_lock:
            self._notifications[thread_id].append(entry)
        # Durably record turn completions so wait_for_turn_completed never
        # misses one that scrolled out of the bounded ring buffer.
        if method == "turn/completed":
            turn = params.get("turn") or {}
            turn_id = turn.get("id")
            if turn_id is not None:
                with self._completion_lock:
                    self._completed_turns[(thread_id, turn_id)] = params

    # -- notification access ------------------------------------------------

    def get_notifications(self, thread_id):
        with self._notif_lock:
            return list(self._notifications.get(thread_id, ()))

    def wait_for_turn_completed(self, thread_id, turn_id, timeout):
        """Block until a turn/completed notification for (thread_id, turn_id) arrives.

        Returns the matching notification params, or None on timeout. Also returns
        immediately if the turn already completed before this call.

        Backed by the durable self._completed_turns map (not the size-bounded
        notification ring buffer), so a completion can never be missed because
        item/completed churn evicted it.
        """
        key = (thread_id, turn_id)
        deadline = time.time() + timeout
        while True:
            with self._completion_lock:
                params = self._completed_turns.get(key)
            if params is not None:
                return params
            if time.time() >= deadline:
                return None
            time.sleep(0.2)

    def take_restart_note(self):
        note = self._restarted_msg
        self._restarted_msg = None
        return note

    def shutdown(self):
        proc = self._proc
        if proc is None:
            return
        try:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
        except Exception:
            pass


class CodexError(Exception):
    """A JSON-RPC error returned by the codex app-server."""

    def __init__(self, message, error_obj):
        super().__init__(message)
        self.error_obj = error_obj


# ----------------------------------------------------------------------------
# Tool implementations
# ----------------------------------------------------------------------------


def _text_input(prompt):
    """Wrap a plain prompt string as the UserInput array turn/start expects."""
    return [{"type": "text", "text": prompt}]


def _status_type(status):
    if isinstance(status, dict):
        return status.get("type")
    return status


def _thread_row(thread):
    """Compact a Thread object into a small row for list/search results."""
    return {
        "id": thread.get("id"),
        "name": thread.get("name"),
        "preview": truncate(thread.get("preview"), PREVIEW_TRUNCATE),
        "updatedAt": thread.get("updatedAt"),
        "status": _status_type(thread.get("status")),
        "cwd": thread.get("cwd"),
    }


def _item_summary(item, include_outputs):
    """Reduce a ThreadItem to a small, text-truncated summary."""
    itype = item.get("type")
    out = {"type": itype, "id": item.get("id")}
    if itype == "userMessage":
        parts = []
        for c in item.get("content", []) or []:
            if isinstance(c, dict) and c.get("type") == "text":
                parts.append(c.get("text", ""))
        out["text"] = truncate("\n".join(parts), ITEM_TEXT_TRUNCATE)
    elif itype == "agentMessage":
        out["text"] = truncate(item.get("text"), ITEM_TEXT_TRUNCATE)
    elif itype == "plan":
        out["text"] = truncate(item.get("text"), ITEM_TEXT_TRUNCATE)
    elif itype == "reasoning":
        summary = item.get("summary") or item.get("content") or []
        out["text"] = truncate("\n".join(summary), ITEM_TEXT_TRUNCATE)
    elif itype == "commandExecution":
        out["command"] = truncate(item.get("command"), ITEM_TEXT_TRUNCATE)
        out["status"] = item.get("status")
        out["exitCode"] = item.get("exitCode")
        if include_outputs:
            out["output"] = truncate(item.get("aggregatedOutput"), ITEM_TEXT_TRUNCATE)
    elif itype == "fileChange":
        out["status"] = item.get("status")
        changes = item.get("changes") or []
        if isinstance(changes, list):
            out["files"] = len(changes)
        elif isinstance(changes, dict):
            out["files"] = list(changes.keys())
    elif itype == "mcpToolCall":
        out["server"] = item.get("server")
        out["tool"] = item.get("tool")
        out["status"] = item.get("status")
    elif itype == "webSearch":
        out["query"] = item.get("query")
    else:
        # Generic fallback: keep any obvious text field.
        for k in ("text", "status", "query"):
            if k in item:
                out[k] = truncate(item.get(k), ITEM_TEXT_TRUNCATE) if k == "text" else item.get(k)
    return out


def _final_agent_message(items):
    """Return the last agentMessage text in an items list, if any."""
    last = None
    for item in items or []:
        if item.get("type") == "agentMessage":
            last = item.get("text")
    return last


class Tools:
    """Holds tool metadata and dispatches tool calls against a CodexClient."""

    def __init__(self, codex):
        self.codex = codex

    # -- registry -----------------------------------------------------------

    def list_tools(self):
        return [
            {
                "name": "list_threads",
                "description": (
                    "List your Codex threads (most-recently-updated first) so you can survey "
                    "what work is in flight. Returns compact rows: id, name, preview, updatedAt, "
                    "status, and cwd."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "limit": {"type": "integer", "description": "Max threads to return (default 20)."},
                        "archived": {"type": "boolean", "description": "If true, return only archived threads."},
                    },
                },
            },
            {
                "name": "search_threads",
                "description": (
                    "Find Codex threads whose name or preview contains a query string. This is a "
                    "LOCAL case-insensitive substring filter over a recent page of threads (the "
                    "installed Codex has no server-side thread search)."
                ),
                "inputSchema": {
                    "type": "object",
                    "required": ["query"],
                    "properties": {
                        "query": {"type": "string", "description": "Case-insensitive substring to match."},
                        "limit": {"type": "integer", "description": "Max matches to return (default 10)."},
                    },
                },
            },
            {
                "name": "read_thread",
                "description": (
                    "Read a Codex thread's recent turns and items to catch up on what it has been "
                    "doing. Item texts are truncated; set include_outputs to see command output."
                ),
                "inputSchema": {
                    "type": "object",
                    "required": ["thread_id"],
                    "properties": {
                        "thread_id": {"type": "string"},
                        "turn_limit": {"type": "integer", "description": "Max recent turns to return (default 5)."},
                        "include_outputs": {"type": "boolean", "description": "Include command output text (default false)."},
                    },
                },
            },
            {
                "name": "thread_status",
                "description": (
                    "Quick check on whether a Codex thread is idle or running and what happened "
                    "recently, combining its latest turn with this session's buffered notifications."
                ),
                "inputSchema": {
                    "type": "object",
                    "required": ["thread_id"],
                    "properties": {"thread_id": {"type": "string"}},
                },
            },
            {
                "name": "create_thread",
                "description": (
                    "Start a brand-new Codex thread and kick off its first turn with your prompt. "
                    "Runs full-access with approvals disabled (YOLO). Returns the new thread_id and "
                    "turn id; the turn runs in the background."
                ),
                "inputSchema": {
                    "type": "object",
                    "required": ["prompt"],
                    "properties": {
                        "prompt": {"type": "string"},
                        "cwd": {"type": "string", "description": "Working directory for the thread."},
                        "model": {"type": "string"},
                        "effort": {"type": "string", "description": "Reasoning effort: none|minimal|low|medium|high|xhigh."},
                    },
                },
            },
            {
                "name": "send_to_thread",
                "description": (
                    "Send a follow-up message to an existing Codex thread, resuming it and starting "
                    "a new turn (YOLO full-access). Fire-and-forget by default; set wait=true to "
                    "block until the turn completes and return its final assistant message."
                ),
                "inputSchema": {
                    "type": "object",
                    "required": ["thread_id", "prompt"],
                    "properties": {
                        "thread_id": {"type": "string"},
                        "prompt": {"type": "string"},
                        "model": {"type": "string"},
                        "effort": {"type": "string", "description": "Reasoning effort: none|minimal|low|medium|high|xhigh."},
                        "wait": {"type": "boolean", "description": "Block until turn/completed (default false)."},
                        "timeout_sec": {"type": "integer", "description": "Seconds to wait when wait=true (default 300)."},
                    },
                },
            },
            {
                "name": "interrupt_turn",
                "description": "Interrupt the currently running turn on a Codex thread (e.g. to stop it going down the wrong path).",
                "inputSchema": {
                    "type": "object",
                    "required": ["thread_id"],
                    "properties": {"thread_id": {"type": "string"}},
                },
            },
            {
                "name": "archive_thread",
                "description": "Archive a Codex thread to get it out of the active list. The thread is preserved on disk and can be unarchived.",
                "inputSchema": {
                    "type": "object",
                    "required": ["thread_id"],
                    "properties": {"thread_id": {"type": "string"}},
                },
            },
            {
                "name": "unarchive_thread",
                "description": "Restore a previously archived Codex thread back to the active list.",
                "inputSchema": {
                    "type": "object",
                    "required": ["thread_id"],
                    "properties": {"thread_id": {"type": "string"}},
                },
            },
            {
                "name": "rename_thread",
                "description": "Set or update a Codex thread's user-facing title so it is easy to find later.",
                "inputSchema": {
                    "type": "object",
                    "required": ["thread_id", "title"],
                    "properties": {
                        "thread_id": {"type": "string"},
                        "title": {"type": "string"},
                    },
                },
            },
            {
                "name": "fork_thread",
                "description": "Fork a Codex thread into a new thread with copied history, so you can branch an exploration without disturbing the original.",
                "inputSchema": {
                    "type": "object",
                    "required": ["thread_id"],
                    "properties": {"thread_id": {"type": "string"}},
                },
            },
        ]

    # -- dispatch -----------------------------------------------------------

    def call(self, name, args):
        """Run a tool; return (result_obj, is_error)."""
        handler = getattr(self, "_tool_" + name, None)
        if handler is None:
            return ({"error": "unknown tool: %s" % name}, True)
        return handler(args or {})

    # -- read-path tools ----------------------------------------------------

    def _tool_list_threads(self, args):
        limit = args.get("limit", 20)
        params = {"limit": limit}
        if "archived" in args and args["archived"] is not None:
            params["archived"] = bool(args["archived"])
        resp = self.codex.request("thread/list", params)
        rows = [_thread_row(t) for t in resp.get("data", [])]
        out = {"threads": rows, "count": len(rows)}
        if resp.get("nextCursor"):
            out["nextCursor"] = resp["nextCursor"]
        return (out, False)

    def _tool_search_threads(self, args):
        query = args.get("query", "")
        limit = args.get("limit", 10)
        resp = self.codex.request("thread/list", {"limit": 200})
        q = query.lower()
        matches = []
        for t in resp.get("data", []):
            hay = " ".join(
                str(x or "") for x in (t.get("name"), t.get("preview"))
            ).lower()
            if q in hay:
                matches.append(_thread_row(t))
            if len(matches) >= limit:
                break
        return ({"query": query, "threads": matches, "count": len(matches)}, False)

    def _tool_read_thread(self, args):
        thread_id = args["thread_id"]
        turn_limit = args.get("turn_limit", 5)
        include_outputs = bool(args.get("include_outputs", False))
        params = {"threadId": thread_id, "includeTurns": True}
        resp = self.codex.request("thread/read", params)
        thread = resp.get("thread", {})
        turns = thread.get("turns", []) or []
        recent = turns[-turn_limit:] if turn_limit else turns
        out_turns = []
        for turn in recent:
            out_turns.append(
                {
                    "id": turn.get("id"),
                    "status": turn.get("status"),
                    "startedAt": turn.get("startedAt"),
                    "completedAt": turn.get("completedAt"),
                    "items": [_item_summary(i, include_outputs) for i in turn.get("items", [])],
                }
            )
        out = {
            "thread_id": thread.get("id"),
            "name": thread.get("name"),
            "status": _status_type(thread.get("status")),
            "cwd": thread.get("cwd"),
            "turns": out_turns,
        }
        return (out, False)

    def _tool_thread_status(self, args):
        thread_id = args["thread_id"]
        resp = self.codex.request("thread/read", {"threadId": thread_id, "includeTurns": True})
        thread = resp.get("thread", {})
        turns = thread.get("turns", []) or []
        last_turn = turns[-1] if turns else None
        notes = self.codex.get_notifications(thread_id)
        recent_events = [
            {"method": n["method"], "params": _trim_notification(n["params"])}
            for n in notes[-10:]
        ]
        out = {
            "thread_id": thread.get("id"),
            "name": thread.get("name"),
            "status": _status_type(thread.get("status")),
            "last_turn": (
                {
                    "id": last_turn.get("id"),
                    "status": last_turn.get("status"),
                    "final_message": truncate(_final_agent_message(last_turn.get("items")), ITEM_TEXT_TRUNCATE),
                }
                if last_turn
                else None
            ),
            "recent_events": recent_events,
        }
        return (out, False)

    # -- write-path tools ---------------------------------------------------

    def _tool_create_thread(self, args):
        start_params = {
            "approvalPolicy": YOLO_APPROVAL_POLICY,
            "sandbox": YOLO_THREAD_SANDBOX,
        }
        if args.get("cwd"):
            start_params["cwd"] = args["cwd"]
        if args.get("model"):
            start_params["model"] = args["model"]
        start_resp = self.codex.request("thread/start", start_params)
        thread = start_resp.get("thread", {})
        thread_id = thread.get("id")

        turn_params = {
            "threadId": thread_id,
            "input": _text_input(args["prompt"]),
            "approvalPolicy": YOLO_APPROVAL_POLICY,
            "sandboxPolicy": YOLO_TURN_SANDBOX_POLICY,
        }
        if args.get("model"):
            turn_params["model"] = args["model"]
        if args.get("effort"):
            turn_params["effort"] = args["effort"]
        turn_resp = self.codex.request("turn/start", turn_params)
        turn = turn_resp.get("turn", {})
        out = {
            "thread_id": thread_id,
            "turn_id": turn.get("id"),
            "status": turn.get("status"),
            "note": "Turn started in the background. Use thread_status or read_thread to follow up.",
        }
        return (out, False)

    def _tool_send_to_thread(self, args):
        thread_id = args["thread_id"]
        wait = bool(args.get("wait", False))
        timeout_sec = args.get("timeout_sec", 300)

        self.codex.request("thread/resume", {"threadId": thread_id})

        turn_params = {
            "threadId": thread_id,
            "input": _text_input(args["prompt"]),
            "approvalPolicy": YOLO_APPROVAL_POLICY,
            "sandboxPolicy": YOLO_TURN_SANDBOX_POLICY,
        }
        if args.get("model"):
            turn_params["model"] = args["model"]
        if args.get("effort"):
            turn_params["effort"] = args["effort"]
        turn_resp = self.codex.request("turn/start", turn_params)
        turn = turn_resp.get("turn", {})
        turn_id = turn.get("id")

        if not wait:
            out = {
                "thread_id": thread_id,
                "turn_id": turn_id,
                "status": turn.get("status"),
                "waited": False,
                "note": "Fire-and-forget; turn runs in the background.",
            }
            return (out, False)

        completed = self.codex.wait_for_turn_completed(thread_id, turn_id, float(timeout_sec))
        if completed is None:
            # Not an error: the turn is simply still running. Surface the latest
            # final assistant message if we have buffered one.
            last_msg = None
            for n in reversed(self.codex.get_notifications(thread_id)):
                if n["method"] == "turn/completed":
                    last_msg = _final_agent_message((n["params"].get("turn") or {}).get("items"))
                    if last_msg:
                        break
            out = {
                "thread_id": thread_id,
                "turn_id": turn_id,
                "waited": True,
                "completed": False,
                "note": "Turn still running after %ss; check back with thread_status." % timeout_sec,
                "last_final_message": truncate(last_msg, ITEM_TEXT_TRUNCATE),
            }
            return (out, False)

        completed_turn = completed.get("turn", {})
        final_message = _final_agent_message(completed_turn.get("items"))
        if final_message is None:
            # turn/completed notifications carry the Turn without its items
            # populated (items stream separately), so fetch the final message
            # with a thread/read.
            try:
                resp = self.codex.request(
                    "thread/read", {"threadId": thread_id, "includeTurns": True}
                )
                for t in resp.get("thread", {}).get("turns", []) or []:
                    if t.get("id") == turn_id:
                        final_message = _final_agent_message(t.get("items"))
                        break
            except (CodexError, TimeoutError) as exc:
                log("final-message fallback read failed:", repr(exc))
        out = {
            "thread_id": thread_id,
            "turn_id": turn_id,
            "waited": True,
            "completed": True,
            "status": completed_turn.get("status"),
            "final_message": truncate(final_message, ITEM_TEXT_TRUNCATE),
        }
        return (out, False)

    def _tool_interrupt_turn(self, args):
        thread_id = args["thread_id"]
        # turn/interrupt requires a turnId; find the most recent active turn.
        turn_id = self._latest_turn_id(thread_id)
        if not turn_id:
            return ({"error": "no recent turn found to interrupt for this thread"}, True)
        self.codex.request("turn/interrupt", {"threadId": thread_id, "turnId": turn_id})
        return ({"thread_id": thread_id, "turn_id": turn_id, "interrupted": True}, False)

    def _latest_turn_id(self, thread_id):
        # Prefer a turnId from buffered notifications; fall back to thread/read.
        for n in reversed(self.codex.get_notifications(thread_id)):
            params = n["params"]
            if "turn" in params and isinstance(params["turn"], dict) and params["turn"].get("id"):
                return params["turn"]["id"]
            if params.get("turnId"):
                return params["turnId"]
        try:
            resp = self.codex.request("thread/read", {"threadId": thread_id, "includeTurns": True})
            turns = resp.get("thread", {}).get("turns", []) or []
            if turns:
                return turns[-1].get("id")
        except CodexError:
            pass
        return None

    def _tool_archive_thread(self, args):
        self.codex.request("thread/archive", {"threadId": args["thread_id"]})
        return ({"thread_id": args["thread_id"], "archived": True}, False)

    def _tool_unarchive_thread(self, args):
        self.codex.request("thread/unarchive", {"threadId": args["thread_id"]})
        return ({"thread_id": args["thread_id"], "archived": False}, False)

    def _tool_rename_thread(self, args):
        self.codex.request(
            "thread/name/set", {"threadId": args["thread_id"], "name": args["title"]}
        )
        return ({"thread_id": args["thread_id"], "name": args["title"]}, False)

    def _tool_fork_thread(self, args):
        resp = self.codex.request("thread/fork", {"threadId": args["thread_id"]})
        new_thread = resp.get("thread", {})
        return (
            {"source_thread_id": args["thread_id"], "new_thread_id": new_thread.get("id")},
            False,
        )


def _trim_notification(params):
    """Shrink a buffered notification's params to the bits worth surfacing."""
    out = {}
    if "turnId" in params:
        out["turnId"] = params["turnId"]
    if "status" in params:
        out["status"] = _status_type(params["status"])
    turn = params.get("turn")
    if isinstance(turn, dict):
        out["turn"] = {"id": turn.get("id"), "status": turn.get("status")}
    if "error" in params and isinstance(params["error"], dict):
        out["error"] = truncate(params["error"].get("message"), 500)
    return out


# ----------------------------------------------------------------------------
# MCP front-end (stdin/stdout JSON-RPC)
# ----------------------------------------------------------------------------


class MCPServer:
    def __init__(self):
        self.codex = CodexClient()
        self.tools = Tools(self.codex)
        self._out_lock = threading.Lock()

    def _send(self, obj):
        with self._out_lock:
            sys.stdout.write(json.dumps(obj, separators=(",", ":")) + "\n")
            sys.stdout.flush()

    def _reply(self, req_id, result):
        self._send({"jsonrpc": "2.0", "id": req_id, "result": result})

    def _error(self, req_id, code, message):
        self._send({"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}})

    def run(self):
        log("starting; reading MCP requests on stdin")
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                log("ignoring non-JSON input")
                continue
            if not isinstance(msg, dict):
                # Valid JSON but not a JSON-RPC object (e.g. a list/str/int/
                # null/bool). Skip it rather than letting msg.get(...) raise.
                log("ignoring non-object JSON input:", repr(msg)[:120])
                continue
            try:
                self._handle(msg)
            except Exception as exc:
                # One malformed-but-object request must never tear down the
                # server. Log, and reply with an internal error if it had an id.
                log("error handling message:", repr(exc))
                req_id = msg.get("id")
                if req_id is not None:
                    try:
                        self._error(req_id, -32603, "internal error: %s" % exc)
                    except Exception:
                        pass
        log("stdin closed; shutting down")
        self.codex.shutdown()

    def _handle(self, msg):
        method = msg.get("method")
        req_id = msg.get("id")
        is_notification = req_id is None

        if method == "initialize":
            params = msg.get("params") or {}
            protocol_version = params.get("protocolVersion") or DEFAULT_PROTOCOL_VERSION
            self._reply(
                req_id,
                {
                    "protocolVersion": protocol_version,
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
                },
            )
            return

        if method == "notifications/initialized":
            return  # ignore

        if method == "ping":
            self._reply(req_id, {})
            return

        if method == "tools/list":
            self._reply(req_id, {"tools": self.tools.list_tools()})
            return

        if method == "tools/call":
            self._handle_tools_call(req_id, msg.get("params") or {})
            return

        # Unknown method.
        if is_notification:
            log("ignoring unknown notification:", method)
            return
        self._error(req_id, -32601, "Method not found: %s" % method)

    def _handle_tools_call(self, req_id, params):
        name = params.get("name")
        args = params.get("arguments") or {}
        try:
            restart_note = self.codex.ensure_started()
        except Exception as exc:
            self._tool_result(req_id, {"error": "failed to start codex app-server: %s" % exc}, True)
            return

        try:
            result, is_error = self.tools.call(name, args)
        except CodexError as exc:
            self._tool_result(req_id, {"error": str(exc), "codex_error": exc.error_obj}, True)
            return
        except TimeoutError as exc:
            self._tool_result(req_id, {"error": str(exc)}, True)
            return
        except KeyError as exc:
            self._tool_result(req_id, {"error": "missing required argument: %s" % exc}, True)
            return
        except Exception as exc:
            log("tool error:", repr(exc))
            self._tool_result(req_id, {"error": "internal error: %s" % exc}, True)
            return

        # Surface any restart that happened on this call.
        note = self.codex.take_restart_note() or restart_note
        if note and isinstance(result, dict):
            result.setdefault("_restart", note)
        self._tool_result(req_id, result, is_error)

    def _tool_result(self, req_id, payload, is_error):
        text = json.dumps(payload, separators=(",", ":"))
        self._reply(
            req_id,
            {"content": [{"type": "text", "text": text}], "isError": bool(is_error)},
        )


def main():
    server = MCPServer()

    def _terminate(signum, _frame):
        # Reap the codex child before exiting so it is never orphaned when
        # Claude Code tears the MCP server down (the normal SIGTERM case).
        log("received signal %s; shutting down" % signum)
        try:
            server.codex.shutdown()
        finally:
            os._exit(0)

    signal.signal(signal.SIGTERM, _terminate)
    # SIGHUP is not defined on Windows; guard so this stays portable.
    if hasattr(signal, "SIGHUP"):
        signal.signal(signal.SIGHUP, _terminate)

    try:
        server.run()
    except KeyboardInterrupt:
        pass
    finally:
        server.codex.shutdown()


if __name__ == "__main__":
    main()
