# codex-app-mcp

A single-file, stdlib-only Python MCP server that lets Claude Code (or any MCP
client) act as a chief-of-staff over your OpenAI **Codex CLI** threads: list,
read, search, create, message, interrupt, archive, rename, and fork them.

This also makes cross-model delegation trivial. Your Claude session can hand a
task to a Codex thread running whatever model and reasoning effort you pass
(`model`, `effort`), fire-and-forget or blocking, and read the result back.

It speaks MCP (JSON-RPC 2.0 over stdin/stdout) to the client, and lazily spawns
a `codex app-server` child process that it drives over JSON-RPC.

> [!WARNING]
> Every turn this server starts runs **YOLO**: approvals disabled and
> full filesystem access, with any approval prompts from the Codex agent
> auto-approved. Read the [YOLO caveat](#yolo-caveat) before installing.

## Requirements

- Python 3 (no third-party packages)
- [Codex CLI](https://github.com/openai/codex) on your `PATH`, logged in.
  Built against `codex-cli` 0.136.0 and verified through 0.142.x.

The Codex **desktop app does not need to be running** (or installed):
`codex app-server` is a headless subcommand of the CLI, spawned as a child
process on the first tool call. The app, the CLI, and this server are all just
frontends over the same on-disk thread store (`~/.codex/sessions`), so threads
created here show up there and vice versa.

## Registration

Register it with Claude Code:

```sh
claude mcp add codex-app-mcp -- python3 /path/to/codex-app-mcp/server.py
```

(Or add an equivalent `command: python3`, `args: ["/path/to/codex-app-mcp/server.py"]`
entry to your MCP client config.)

## Tools

| Tool | What it does |
|---|---|
| `list_threads {limit?=20, archived?}` | List threads (newest first): id, name, preview, updatedAt, status, cwd. |
| `search_threads {query, limit?=10}` | **Local** case-insensitive substring filter over name+preview of a recent page (Codex has no server-side thread search). |
| `read_thread {thread_id, turn_limit?=5, include_outputs?=false}` | Recent turns and item summaries (texts truncated). |
| `thread_status {thread_id}` | Cheap "is it idle/running, what happened lately" from the latest turn + buffered notifications. |
| `create_thread {prompt, cwd?, model?, effort?}` | Start a new thread and kick off its first turn. Returns thread_id + turn id. |
| `send_to_thread {thread_id, prompt, model?, effort?, wait?=false, timeout_sec?=300}` | Resume a thread and start a turn. `wait=false` returns the turn id immediately; `wait=true` blocks for `turn/completed` and returns the final assistant message. |
| `interrupt_turn {thread_id}` | Interrupt the thread's currently running turn. |
| `archive_thread {thread_id}` / `unarchive_thread {thread_id}` | Move a thread out of / back into the active list. |
| `rename_thread {thread_id, title}` | Set a thread's user-facing title. |
| `fork_thread {thread_id}` | Branch a thread into a new one with copied history. |

Backend errors are returned with `isError: true` and the Codex error message intact.

## YOLO caveat

Every turn this server starts runs with **approvals disabled and full filesystem
access** (`approvalPolicy: "never"`, `danger-full-access` sandbox). Any approval
prompts the Codex agent raises (command execution, file changes, permissions) are
**auto-approved**. This is intentional: the point is unattended delegation. Only
point this at Codex threads and working directories you trust.

## Session-lifetime caveat

The `codex app-server` child and its in-flight turns live and die with **this**
MCP session. If the session ends (or the server process exits) while a turn is
running, that in-flight turn is killed. The **threads themselves survive on
disk** under `~/.codex` and can be resumed later (e.g. via `send_to_thread`)
from a new session. If the child dies mid-session, the next tool call
transparently respawns and re-handshakes it, and reports the restart in that
call's result (`_restart` field).

## Testing

A read-only smoke test is included (it lists and reads your real Codex threads
but starts no turns and changes nothing):

```sh
python3 smoke_test.py
```

See [DESIGN.md](DESIGN.md) for architecture notes.

## License

MIT, see [LICENSE](LICENSE).
