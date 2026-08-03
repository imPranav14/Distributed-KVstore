# Claude Code — Distributed KV Store Project Guide

> This file is the single source of truth for how we work together.
> Read it fully before writing a single line of code.
> Re-read the relevant section at the start of every session.

---

## What We Are Building

A **fault-tolerant distributed key-value store** built entirely from scratch in Go — no third-party consensus libraries, no embedded databases. Think of it as a minimal Redis or DynamoDB that you understand completely, down to every line.

A client can call:
```
PUT("user:123", "alice")   → stores the pair, survives node crashes
GET("user:123")            → returns "alice", reads from any live majority
DELETE("user:123")         → removes the key, replicated to all nodes
```

The hard part is not the storage. It is making those three calls work correctly when:
- A node crashes mid-write
- The network between nodes drops
- Two nodes temporarily disagree on who is the leader
- A crashed node restarts and needs to catch up

Every component in this project exists to solve one of those four problems. Never lose sight of that.

---

## Technology Stack

| Layer | Technology | Why This, Not Something Else |
|---|---|---|
| Language | Go 1.22+ | Goroutines fit Raft's concurrent timers perfectly; static binary simplifies Docker |
| RPC | gRPC + protobuf (proto3) | Binary protocol, strongly typed contracts, bidirectional streaming for heartbeats |
| Persistence | OS `file I/O` (`os`, `syscall`) | No abstraction hiding what fsync does — we need to feel the disk |
| Containers | Docker + Docker Compose | Spin up a real 3-node cluster locally; kill nodes with `docker stop` |
| Testing | Go `testing` package + `-race` flag | Built-in race detector catches concurrency bugs immediately |
| Logging | `log/slog` | Structured logs (term, role, commitIndex on every line) without external deps |
| Build | `Makefile` | Reproducible commands for proto generation, testing, cluster management |

**Hard rule:** Do not introduce any library that implements Raft, consensus, or key-value storage. We build everything from scratch. That is the entire point.

---

## Project File Structure

Enforce this layout from day one. Every new file must be placed correctly before writing its content.

```
kvstore/
│
├── CLAUDE.md                        ← this file (project guide for Claude Code)
├── UNDERSTANDING.md                 ← learning journal, updated after every step
├── Makefile
├── docker-compose.yml
├── go.mod
├── go.sum
│
├── proto/
│   ├── kv/
│   │   └── kv.proto                 ← client-facing Get / Put / Delete service
│   └── raft/
│       └── raft.proto               ← RequestVote / AppendEntries / InstallSnapshot
│
├── cmd/
│   └── node/
│       └── main.go                  ← entry point only: parse config, wire deps, start
│
├── internal/
│   ├── config/
│   │   └── config.go                ← reads NODE_ID, PEER_ADDRS, GRPC_PORT, DATA_DIR
│   │
│   ├── store/
│   │   ├── store.go                 ← in-memory KV state machine: map[string]string
│   │   └── store_test.go
│   │
│   ├── wal/
│   │   ├── wal.go                   ← append-only write-ahead log
│   │   ├── entry.go                 ← WAL entry struct + serialization
│   │   └── wal_test.go
│   │
│   ├── raft/
│   │   ├── node.go                  ← RaftNode struct, state machine, public API
│   │   ├── election.go              ← RequestVote logic, election timer
│   │   ├── replication.go           ← AppendEntries, log replication loop
│   │   ├── log.go                   ← []LogEntry, log operations
│   │   ├── snapshot.go              ← InstallSnapshot, compaction trigger
│   │   ├── state.go                 ← persistent state: currentTerm, votedFor
│   │   └── raft_test.go
│   │
│   ├── server/
│   │   ├── kv_server.go             ← gRPC server wiring KV service to Raft
│   │   └── raft_server.go           ← gRPC server handling Raft RPCs
│   │
│   ├── client/
│   │   ├── client.go                ← client with request IDs, retry, timeout
│   │   └── client_test.go
│   │
│   └── ring/                        ← added only at Milestone 9 (sharding)
│       ├── ring.go
│       └── ring_test.go
│
└── tests/
    ├── integration/
    │   ├── election_test.go
    │   ├── replication_test.go
    │   └── kv_test.go
    └── chaos/
        └── chaos_test.go
```

---

## Milestone Roadmap

These are the ten milestones. Do not begin a milestone until the previous one is complete, tested, and logged in `UNDERSTANDING.md`.

```
Milestone 0  — Requirements, architecture, Makefile skeleton, docker-compose stub
Milestone 1  — Single-node in-memory KV store (Get, Put, Delete) with full unit tests
Milestone 2  — Client request IDs and server-side duplicate detection (at-most-once)
Milestone 3  — gRPC layer: proto definitions, code generation, KV server, client with retries
Milestone 4  — Write-ahead log with fsync durability and crash recovery via replay
Milestone 5  — Raft leader election: terms, randomized timeouts, RequestVote, heartbeats
Milestone 6  — Raft log replication: AppendEntries, commitIndex, log matching
Milestone 7  — Fault-tolerant KV on Raft: leader-only writes, linearizable reads, dedup
Milestone 8  — Snapshots and log compaction: InstallSnapshot, bounded disk usage
Milestone 9  — Sharding: consistent hash ring, multiple Raft groups, key routing
Milestone 10 — Hardening: race audit, chaos tests, structured logging, README
```

---

## Base Rules — Follow These at Every Step, No Exceptions

These are not suggestions. They govern every session, every function, every commit.

### Rule 1 — Concept Before Code
Never write code for a new concept without first explaining the problem it solves in plain English. If you cannot explain what breaks without this code, you do not understand it yet.

### Rule 2 — Small Steps Only
Each implementation step should touch as few files as possible. If a step feels large, split it. A diff that touches more than ~100–150 lines needs a reason. Prefer five small commits over one large one.

### Rule 3 — Readable Over Clever
Write code for the person reading it six months from now who has never seen this codebase. Use long variable names, add comments on non-obvious lines, prefer explicit over implicit. No one-liners that require a minute to decode.

### Rule 4 — Invariants Must Be Visible
Every invariant must appear as a comment directly above the code that enforces it. Example:
```go
// INVARIANT: votedFor must be persisted before we return true to a RequestVote RPC.
// If we crash after voting but before persisting, we could vote twice in the same term,
// violating the safety property that at most one leader can exist per term.
```

### Rule 5 — Every Goroutine Has an Exit
No goroutine leak, ever. Every goroutine started must have a documented exit condition, driven by a context cancellation or a done channel. Point this out when it is created.

### Rule 6 — Every Error Has Context
Never return a bare `err`. Always wrap:
```go
return fmt.Errorf("wal: failed to sync entry seq=%d: %w", entry.Seq, err)
```
The error message must tell you which file, which function, and which operation failed.

### Rule 7 — No Mutex, No Touch
Any field on a shared struct that can be accessed from more than one goroutine must be protected by a mutex. Call out the specific field, the specific goroutine that reads it, and the goroutine that writes it. Make the reasoning visible.

### Rule 8 — Tests First, Then Code
Write the test for a function before or alongside implementing it — never after. A test is not a chore; it is a precise statement of what the code must do. Explain which invariant each test is checking, not just what it does mechanically.

### Rule 9 — Confirm Before Proceeding
After every sub-step: summarize what was built, state the invariants it maintains, name the top two or three ways it could go wrong, suggest a git commit message, then stop and wait for confirmation. Do not move to the next step automatically.

### Rule 10 — Never Skip the UNDERSTANDING.md Update
After every step, before asking for confirmation, update `UNDERSTANDING.md` with the full learning entry for that step. This file is as important as the code itself.

---

## The Step-by-Step Workflow for Every Milestone

Run every milestone in this exact sequence. Do not skip steps.

```
Step 1  CONCEPT BRIEF
        Explain the distributed systems problem this milestone solves.
        What breaks in the real world without it?
        What are the two or three design approaches available?
        Wait for questions before proposing any code.

Step 2  DESIGN CHOICE
        Present the available options with their tradeoffs.
        Ask which to use. After the choice is made, explain
        why that choice is right or wrong given our constraints.

Step 3  PSEUDOCODE
        Write pseudocode in plain English or commented stubs.
        No real code yet. Walk through the logic out loud.
        Ask: "Does this make sense before we translate it?"

Step 4  IMPLEMENTATION
        Write the real code in small chunks — function by function.
        Add inline comments on every non-obvious line.
        State the invariant each block of code maintains.

Step 5  LINE-BY-LINE AUDIT
        Pick the three to five most important lines in what was
        just written. For each one, explain:
        - What does this line do?
        - What breaks if it is removed?
        - Why is it written this way and not another?

Step 6  BREAK IT ON PURPOSE
        Identify two or three ways to deliberately break the code.
        For each: predict what will fail before changing anything.
        Then make the change, run the tests, observe.
        Restore the correct code afterward and explain why the
        broken version was wrong at a deep level.

Step 7  WRITE THE TEST
        Write the test for what was just built.
        For each test, state: "This test checks the invariant that..."
        Explain why this specific scenario was chosen.
        Run go test -race ./... and confirm it passes.

Step 8  INTERVIEW DRILL
        Ask five questions about the just-completed step
        as a skeptical senior engineer would in an SDE interview.
        Start conceptual, end with code-level questions.
        Give honest feedback on every answer.
        Point out what was missing even if the answer was mostly right.
```

---

## The UNDERSTANDING.md File

This file lives at the project root. It is a learning journal. It must be updated after every step — not at the end of a milestone, but after every individual sub-step.

Claude Code is responsible for proposing the entry. The entry must be written before asking for confirmation to proceed.

### Entry Format

```markdown
## [Milestone N — Step Name]
Date: YYYY-MM-DD

### The Problem This Solves
One or two sentences. What breaks in a real distributed system without this?

### The Design Decision Made
What option was chosen and what were the alternatives?
Why was this option better for our constraints?

### How It Works — Plain English
Explain the implementation as if writing to someone who knows
Go but has never studied distributed systems.

### Key Invariants
List every invariant this code maintains, exactly as they appear in code comments.
- INVARIANT 1: ...
- INVARIANT 2: ...

### The Lines That Matter Most
For the three to five most critical lines of code, explain:
- The line itself (copy it here)
- What it does
- What breaks if it is removed
- Why it is written this way and not another

### What Breaks and Why
Document what was deliberately broken in Step 6 and what was observed.
- Change made: ...
- Prediction before breaking: ...
- What actually happened: ...
- Why the broken version is wrong at a deep level: ...

### Interview Questions and Answers
Copy the five interview questions from Step 8 and the answers given.
Mark each answer as strong / needs more depth / wrong, and explain why.

### Git Commit Message
`type: short imperative description of what this step did`

### Open Questions
Anything that is still unclear or needs revisiting in a future step.
```

---

## The "Break It" Protocol — Required at Every Milestone

This is the single most important learning exercise in the project. For every significant piece of code, deliberately introduce these specific breaks:

**Milestone 4 — WAL:**
- Remove `file.Sync()`. Tests still pass. Ask: why, and why is this dangerous in production?
- Write a test that only catches the bug when the process is killed mid-write.

**Milestone 5 — Election:**
- Set all election timeouts to the same fixed value. Observe split-vote loops.
- Remove the check that prevents voting for a candidate with a stale log.

**Milestone 6 — Replication:**
- Comment out the log consistency check in AppendEntries. Write a scenario that now corrupts data.
- Allow a follower to acknowledge AppendEntries before writing to disk.

**Milestone 7 — KV on Raft:**
- Remove the duplicate detection table. Write a test where a client retry applies a PUT twice.
- Allow followers to serve reads directly. Demonstrate a stale read.

**Milestone 8 — Snapshots:**
- Never trigger compaction. Run 100k writes and observe WAL size.
- Restore from a snapshot without replaying the remaining WAL. Show what data is lost.

Use this prompt before breaking anything:
> "Before I make this change — predict exactly which test will fail,
> which invariant is violated, and what a user of the system would observe.
> Then I'll make the change and compare reality to the prediction."

---

## Prompts to Use Every Session

Save these. Use them instead of asking Claude to just write the next thing.

**To start any concept:**
```
Before writing any code, explain the problem this step solves in the real world.
What goes wrong in a production distributed system without it?
Give me two design options with their tradeoffs. Then wait for my questions.
```

**After any function is written:**
```
Do not move on. Pick the three most important lines in that function.
For each line, ask me: what does this do, what breaks without it, why is it
written this exact way? Tell me if my answers are missing anything.
```

**When something is confusing:**
```
Explain this at two levels:
1. As if I have never studied distributed systems (plain English, analogy if useful)
2. As if I am a senior engineer who needs the precise technical definition.
I need both levels, not just one.
```

**To debug without being given the answer:**
```
Do not fix this. Give me three hints in increasing specificity.
I want to find the bug myself. Tell me if I am looking in the wrong place.
```

**At the end of every milestone:**
```
Enter interview mode. You are a senior engineer at a top-tier company
interviewing me about what we just built. Ask me the five hardest questions
someone could reasonably ask about this milestone. Mix conceptual, design,
and code-level questions. I will answer out loud. Tell me exactly what I got
wrong and what was missing from each answer, even if it was mostly correct.
```

**For the whole project at any point:**
```
Ask me to trace a single PUT("x", "1") call through the entire system
from the moment the client sends it to the moment the client receives
the response. Stop me anywhere I am vague or skip a step.
The trace should include: which goroutine handles each stage,
which mutex is held, what is written to disk and when, how many
network round trips occur, and what happens if the leader crashes
at each stage of the trace.
```

---

## Interview Prep — Questions You Must Be Able to Answer

By the end of the project, every answer below must be immediate, precise, and deep. Practice these at the end of every third or fourth milestone.

**System design questions:**
- Walk me through exactly what happens when a client calls PUT(x, 1). Every hop.
- What happens if the leader crashes after writing to its WAL but before replicating to followers?
- How does a follower that was offline for an hour catch up when it rejoins?
- Can your system ever have two leaders at the same time? Prove it.
- What is your read consistency guarantee? How does a linearizable read work in your system?
- Why consistent hashing and not modulo hashing? What specifically breaks when you add a node with modulo?

**Implementation questions:**
- Show me the AppendEntries handler. Explain every conditional check and why removing any one of them breaks safety.
- Why do you hold the mutex here but release it there? What is the exact race condition this prevents?
- Walk me through your WAL entry format. What is in each entry and why?
- Where in your codebase does a write become durable? What is the exact sequence of operations?

**Tradeoff questions:**
- What did you sacrifice to get linearizability? Could you make reads faster and what would you lose?
- Your system requires a majority quorum. In a 3-node cluster, what is the maximum number of nodes that can fail while still accepting writes? What about reads?
- What is the weakest part of your implementation right now?
- If you had two more weeks, what would you add first and why?

**Design alternative questions:**
- Why Raft and not Paxos?
- Why gRPC and not plain HTTP/JSON?
- Why Go and not Java or Rust for this project?
- Why a Write-Ahead Log and not just writing directly to disk?

---

## Makefile Targets

These must work at every milestone from the moment they are introduced.

```makefile
make proto      # regenerate Go stubs from .proto files
make build      # compile to bin/node
make test       # go test -race ./...
make up         # docker compose up --build (3-node cluster)
make down       # docker compose down -v
make logs       # docker compose logs -f
make kill1      # docker compose stop node1 (simulate failure)
make revive1    # docker compose start node1 (rejoin cluster)
```

---

## Docker Compose — Cluster Configuration

The cluster runs three nodes by default. Each node is configured entirely through environment variables. No config files — environment only.

```yaml
services:
  node1:
    build: .
    environment:
      - NODE_ID=1
      - PEER_ADDRS=node2:50051,node3:50051
      - GRPC_PORT=50051
      - DATA_DIR=/data/node1
    volumes:
      - node1-data:/data/node1

  node2:
    build: .
    environment:
      - NODE_ID=2
      - PEER_ADDRS=node1:50051,node3:50051
      - GRPC_PORT=50051
      - DATA_DIR=/data/node2
    volumes:
      - node2-data:/data/node2

  node3:
    build: .
    environment:
      - NODE_ID=3
      - PEER_ADDRS=node1:50051,node2:50051
      - GRPC_PORT=50051
      - DATA_DIR=/data/node3
    volumes:
      - node3-data:/data/node3

volumes:
  node1-data:
  node2-data:
  node3-data:
```

---

## What We Are Not Building

Be explicit about these boundaries. Do not implement them, do not plan for them, do not add hooks for them unless we reach that milestone.

- No Byzantine fault tolerance (we assume crash-stop failures only)
- No TLS or node authentication
- No multi-region or WAN topology
- No automatic shard rebalancing (manual only in Milestone 9)
- No Prometheus, Grafana, or external monitoring (`slog` only)
- No third-party Raft libraries or embedded databases

---

## The Learning Goal

The measure of success for this project is not a working binary. It is this:

> Given any file in this repository, open it to any function, and be able to explain:
> - What problem this function solves
> - Why it is implemented this way and not another
> - What invariant it maintains
> - What breaks if any specific line is removed
> - How a senior engineer at Google would critique it

If you can do that for every file by Milestone 10, the project is done correctly.

---

## How to Start

Read this file completely.

Then ask the minimum set of questions needed to confirm or adjust the requirements. There should be no more than three to five questions. After the answers, present the Milestone 0 plan.

Do not write a single line of application code until Milestone 0 is approved.
