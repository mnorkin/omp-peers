# omp-peers

Agent-to-agent messaging by name between [omp](https://github.com/badlogic/omp) sessions
in different processes.

omp already routes messages between agents by name — its IRC bus wakes an idle
agent, revives a parked one, and hands a busy one the message at its next step
boundary. The mailboxes live in a process-global map, though, so the session in
your other terminal is unreachable. This plugin adds the missing half: a shared
registry of claimed names and a durable inbox, so `peer_send` reaches a session
that another `omp` process is running.

```
you@term-1  omp  "ask release-notes what shipped in 1.4"
            └─ peer_send → ~/.omp/run/peers/peers.db → poll
you@term-2  omp  (mid-task, wakes with the question at its next step boundary)
            └─ peer_send → … → back as the first agent's peer_wait result
```

## Install

```bash
omp plugin install omp-peers
```

Straight from the repository, before it is on npm:

```bash
omp plugin install https://github.com/mnorkin/omp-peers
```

Or from a checkout, which is also how you develop it:

```bash
omp plugin link /path/to/omp-peers
```

Every session started afterwards claims a name and can be messaged. Nothing to
configure.

## Use

| Tool | What it does |
| --- | --- |
| `peer_send {to, body}` | Message a session by name. A running peer has it within a second; one that is not running gets it when it next starts |
| `peer_list` | Who is reachable, online or offline, and what is waiting for them |
| `peer_wait {from?, timeout_sec?}` | Take the next incoming message as a tool result rather than being interrupted by it later. Use it after `peer_send` when you need the answer before continuing |

| Command | What it does |
| --- | --- |
| `/peers` | The roster, and the name this session answers to |
| `/peer-name <name>` | Claim a name; it is remembered when the session is resumed |

A session's name is, in order: `OMP_PEER_NAME`, a name claimed with
`/peer-name`, the session title, then `<directory>-<session id>`. Names are
lowercased and punctuation-folded, so a session titled "Release Notes" answers
to `release-notes`.

Ask the agent in plain language — "tell `release-notes` the migration landed",
"ask `scraper` whether it finished and wait for the answer" — and it picks the
tool. Only the top-level session holds a name; subagents may send but have no
inbox of their own.

## How a message arrives

An incoming message reaches an **idle** session as a new turn, and a **busy**
one as an aside at its next step boundary, so it never interrupts a tool batch
mid-flight. If the recipient has a `peer_wait` outstanding, the message is
returned to that call instead of being injected.

A peer that is not running is not an error: the message is spooled and
delivered when that session next starts, oldest first. A name whose session
exited stays addressable; a name whose session is **live** cannot be claimed by
anyone else, so mail is never silently rerouted.

## Security

Read this before using it on a shared machine.

The store is `~/.omp/run/peers/peers.db`, mode 0600, under your own home
directory. Any process running as you can therefore queue a message that wakes
an agent holding your tools — the same trust boundary as
`~/.omp/run/daemon/token`. There is no network listener and nothing leaves the
machine.

A message body is **another agent's output, and is treated as untrusted**: it is
delivered as fenced, quoted text under a header that says it is a colleague's
words rather than an instruction from the operator, with the fence broken if the
body contains it and control characters stripped. It is never passed through as
markup the harness would read as its own. Bodies are capped at 8,000 characters.

## Configuration

| Variable | Effect |
| --- | --- |
| `OMP_PEER_NAME` | The name this session claims, outranking everything else |
| `OMP_PEERS_DIR` | Where the store lives; point it somewhere scratch to test |

Delivery latency is the poll interval, one second. Peers heartbeat every ten
seconds and are considered offline after forty-five, or as soon as their
recorded pid is gone. Undelivered messages are dropped after seven days,
delivered ones after a day.

## Test

```bash
OMP_PEERS_DIR=/tmp/peers-test bun run test/harness.ts
```

It drives two extension instances against one store with stubbed host APIs:
claiming, delivery, no double-delivery, the wait path, spool-and-drain
ordering, rename, the name conflict, and every refusal. No model is called.

## License

MIT
