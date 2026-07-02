# codex-app-mcp design notes

Single-file Python 3 stdlib-only MCP server (`server.py`) that lets an MCP
client (e.g. Claude Code) list/read/create/message/archive OpenAI Codex CLI
threads by speaking JSON-RPC to a `codex app-server` child process.

The authoritative reference for the backend protocol is the Codex CLI itself:
the app-server docs in the [openai/codex](https://github.com/openai/codex)
repo (`codex-rs/app-server/README.md`) and the JSON schemas it generates
(`ClientRequest`, `ServerNotification`, `ServerRequest`). Built against
codex-cli 0.136.0; verified through 0.142.x.

## Architecture

- **Front (MCP, stdin/stdout)**: newline-delimited JSON-RPC 2.0. Implements
  `initialize` (echoes the client's `protocolVersion`, else "2025-06-18";
  capabilities `{"tools":{}}`), `notifications/initialized` (ignored),
  `ping` → `{}`, `tools/list`, `tools/call`. Unknown method → -32601. Tool
  results: `{"content":[{"type":"text","text":"<compact JSON>"}],"isError":<bool>}`.
  NOTHING but protocol JSON on stdout; all logging goes to stderr.
- **Back (codex app-server child)**: lazily spawns `codex app-server`
  (resolved from PATH at spawn time) on first tool call. Newline JSON-RPC over
  its stdio. Handshake: `initialize` request with `clientInfo`, await response,
  then send the `initialized` notification.
- A reader thread routes child responses by id; request timeout is 120s
  (override with the `CODEX_APP_MCP_TIMEOUT_SEC` env var).
- **Notifications**: the server buffers the last ~300 server notifications
  that carry a thread id (`turn/started`, `turn/completed`,
  `thread/status/changed`, `item/completed`, `error`), grouped per thread, to
  power `thread_status` and `wait=true`. Turn completions are additionally
  recorded in a durable map keyed by (threadId, turnId) so a completion can
  never be evicted from the ring buffer by chatty `item/completed` traffic.
- **ServerRequests (server → client, must be answered or the turn deadlocks)**:
  approval-type requests (`execCommandApproval`, `applyPatchApproval`,
  `item/commandExecution/requestApproval`, `item/fileChange/requestApproval`,
  `item/permissions/requestApproval`) are auto-APPROVED (see the YOLO policy
  below). Anything else gets a JSON-RPC error response and a stderr log line.
- Child death → the next tool call respawns and re-handshakes, and reports the
  restart in that call's result (`_restart` field). The child is reaped on
  stdin EOF, SIGTERM, and SIGHUP.

## YOLO turn policy

Every turn the server starts passes `approvalPolicy: "never"` and a
full-access sandbox. Note the asymmetry in the wire protocol: `thread/start`
takes `sandbox` (a SandboxMode string, `"danger-full-access"`) while
`turn/start` takes `sandboxPolicy` (a tagged object,
`{"type": "dangerFullAccess"}`). Wire field names are camelCase; MCP tool arg
names are snake_case.

## Tools (11)

| MCP tool | Backend | Notes |
|---|---|---|
| `list_threads {limit?=20, archived?}` | `thread/list` | Compact rows: id, name, preview (≤200 chars), updatedAt, status type, cwd |
| `search_threads {query, limit?=10}` | `thread/list` (limit 200) + local case-insensitive substring filter over name+preview | Local filter; Codex has no thread/search |
| `read_thread {thread_id, turn_limit?=5, include_outputs?=false}` | `thread/read` | Item texts truncated to ~2000 chars each |
| `thread_status {thread_id}` | `thread/read` + this session's buffered notifications | Cheap "is it done/running, what happened lately" |
| `create_thread {prompt, cwd?, model?, effort?}` | `thread/start` (+YOLO) then `turn/start` (+YOLO) | Returns thread_id + turn id |
| `send_to_thread {thread_id, prompt, model?, effort?, wait?=false, timeout_sec?=300}` | `thread/resume` then `turn/start` (+YOLO, model/effort passthrough) | wait=false → return turn id immediately; wait=true → block until `turn/completed`; on timeout report still-running (NOT an error) |
| `interrupt_turn {thread_id}` | `turn/interrupt` | Resolves the latest turn id from buffered notifications, falling back to `thread/read` |
| `archive_thread` / `unarchive_thread` | `thread/archive` / `thread/unarchive` | |
| `rename_thread {thread_id, title}` | `thread/name/set` | |
| `fork_thread {thread_id}` | `thread/fork` | |

## Quality bar

- `python3 -m py_compile server.py` clean; no third-party imports.
- `python3 smoke_test.py` passes: spawns the server, MCP-initializes,
  `tools/list`, then exercises the read-path tools against real threads under
  `~/.codex`. It is read-only and safe to run; it never starts a turn.
- Write-path tools (create/send/archive/rename/fork) touch real Codex state,
  so exercise them deliberately, not from CI.
