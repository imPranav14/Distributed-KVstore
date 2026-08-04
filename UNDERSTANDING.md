# Understanding Journal — Distributed KV Store

> This file is updated after every implementation sub-step, not at the end of milestones.
> It is a record of what was built, why, what was learned, and what questions remain.
> By the end of the project, this file should be readable as a complete explanation
> of every design decision in the codebase — in plain English.

---

## How to Read This File

Each entry follows the same structure:
- **The Problem** — what breaks in a real system without this piece
- **The Decision** — what was chosen and what was rejected, with reasons
- **How It Works** — plain English explanation, no assumed knowledge
- **Key Invariants** — exact statements of what the code guarantees
- **Lines That Matter** — the most critical lines and why they exist
- **What Breaks** — what was deliberately broken and what was observed
- **Interview Q&A** — questions asked in interview drill and how they were answered
- **Open Questions** — anything still unclear

---

## Entry Template

Copy this block for every new step.

```
## [Milestone N — Step Name]
Date: YYYY-MM-DD

### The Problem This Solves


### The Design Decision Made


### How It Works — Plain English


### Key Invariants
- INVARIANT: ...

### The Lines That Matter Most
Line: `...`
Does: ...
Breaks without it: ...
Why this way: ...

### What Breaks and Why
Change: ...
Prediction: ...
Observed: ...
Why wrong at a deep level: ...

### Interview Questions and Answers
Q: ...
A: ...
Assessment: strong / needs more depth / wrong
What was missing: ...

### Git Commit Message
`...`

### Open Questions
- ...
```

---

## Milestone 0 — Requirements and Architecture
Date: 2026-08-03

### The Problem This Solves
Every later milestone needs a fixed place to put its code, a build/test loop that
already works, and a 3-node environment that can actually be started and stopped.
Without this, Milestone 1 would start by arguing about file layout instead of
building the KV store, and there would be no journal recording *why* any of the
early structural decisions were made.

### The Design Decision Made
Ran this milestone as a lighter pass (concept + design choices + implementation +
confirmation) rather than the full 8-step workflow, since there is no
distributed-systems algorithm here yet — just scaffolding. Full rigor (pseudocode,
line-by-line audit, break-it-on-purpose, interview drill) starts at Milestone 1.

Key choices made, with alternatives considered:
- **`cmd/node/main.go` is a real runnable stub, not empty.** It loads config and
  blocks on a shutdown signal, rather than being left as a `TODO` or one-line
  `fmt.Println`. This lets `make up` bring up 3 real containers today, proving
  the docker-compose env-var wiring is correct *before* any Raft/KV logic
  exists — a bug here would otherwise be invisible until Milestone 5+ and much
  harder to isolate.
- **`internal/config` was built now, not deferred.** It's pure env-var parsing
  with no distributed-systems content, and `main.go` needs it to do anything
  meaningful at all.
- **Everything else (`internal/store`, `internal/wal`, `internal/raft`,
  `internal/server`, `internal/client`, `proto/`, `tests/`) stays
  directories-only.** Git doesn't track empty directories, and creating empty
  package stubs now would just be dead weight to delete or fill in later —
  each package's first file lands with the milestone that needs it (Rule 2:
  small diffs, no premature structure).
- **`make proto` is defined but a no-op** until Milestone 3 adds actual
  `.proto` files. Alternative considered: omit the target entirely until
  Milestone 3. Rejected — having all `make` targets listed in CLAUDE.md work
  (even if some just print a message) makes `make help`-style discovery
  consistent from day one.

### How It Works — Plain English
`docker-compose.yml` describes three containers (node1, node2, node3), each
built from the same `Dockerfile` but given different environment variables —
`NODE_ID`, `PEER_ADDRS`, `GRPC_PORT`, `DATA_DIR` — so the same binary behaves
as a different cluster member depending on how it's launched. `main.go` reads
those variables via `internal/config.Load()`, logs them so you can visually
confirm each container knows who it is and who its peers are, and then just
waits. It waits by registering a channel to receive OS signals (`SIGINT`,
`SIGTERM`) and blocking on a receive from that channel — this is what keeps a
container alive after `docker compose up` instead of exiting immediately, and
it's also what lets `docker compose stop` (used by `make kill1`) shut a node
down cleanly instead of being force-killed.

### Key Invariants
- INVARIANT (implicit in `main.go`): a node must never exit successfully
  before receiving a shutdown signal — doing so would make the container
  immediately stop right after `docker compose up`, defeating the entire
  purpose of a long-running cluster node.
- INVARIANT (in `config.go`): `Config`'s fields are set once at startup and
  never mutated afterward, so no mutex is required on it — this only holds
  as long as no future code adds a way to reload config at runtime.

### The Lines That Matter Most

Line: `shutdownSignal := make(chan os.Signal, 1)` / `signal.Notify(shutdownSignal, syscall.SIGINT, syscall.SIGTERM)`
Does: Registers the process to receive `SIGINT`/`SIGTERM` on a buffered channel instead of the Go runtime's default behavior (terminate immediately).
Breaks without it: `docker compose stop` sends `SIGTERM`; without a handler, the process dies instantly with no log line, and there's no way to tell from the logs whether a shutdown was intentional (`make kill1`) or a crash.
Why this way: The channel is buffered with capacity 1 so `signal.Notify` can never block trying to deliver the signal, even if `main` hasn't reached the `<-shutdownSignal` line yet.

Line: `receivedSignal := <-shutdownSignal`
Does: Blocks the main goroutine until a shutdown signal arrives.
Breaks without it: `main()` would return immediately after the startup log, the container would exit, and `docker compose up` would show three containers starting and stopping in a loop (or exiting) instead of staying up.
Why this way: No extra goroutine is needed for this — blocking the main goroutine directly is simpler and there's nothing else for `main` to do yet at this milestone, so Rule 5 (every goroutine needs a documented exit) doesn't even come into play here.

Line: `nodeID, err := strconv.Atoi(nodeIDRaw)` (and the parallel check for `GRPC_PORT`)
Does: Fails fast with a specific error naming the bad variable if `NODE_ID` or `GRPC_PORT` isn't a valid integer.
Breaks without it: A typo'd env var (e.g. `NODE_ID=one`) would either panic somewhere far from the actual mistake, or silently zero-value and cause two nodes to think they're the same ID — a correctness bug that would be very confusing to trace back to a config typo.
Why this way: Errors are wrapped with `%w` and the exact bad value, per Rule 6 — the error tells you which variable, what value it had, and that config parsing (not something downstream) is where it failed.

### What Breaks and Why
Not applicable — Step 6 (break-it-on-purpose) is deferred to Milestone 1, since
this milestone has no algorithmic invariant worth deliberately violating yet.

### Interview Questions and Answers
Not applicable — Step 8 (interview drill) is deferred to Milestone 1 for the
same reason.

### Git Commit Message
`chore: scaffold project structure, config loader, and 3-node docker-compose cluster`

### Open Questions
- `go.sum` doesn't exist yet since there are no external dependencies —
  it will be generated automatically once Milestone 3 adds the gRPC/protobuf
  dependency.
- Whether `NodeID` should eventually be `uint64` instead of `int` once Raft
  code needs it in RPC messages — deferring that decision to Milestone 5/3
  proto design rather than guessing now.

---

## Milestone 1 — Single-Node In-Memory KV Store
Date: 2026-08-04

### The Problem This Solves
Every distributed KV store needs a "state machine" — the thing that actually
applies committed operations, one at a time, in order. Raft's whole job
(Milestones 5-7) is getting every node's log to agree on a sequence of
operations, but Raft itself doesn't know what "PUT" means; it just hands
opaque log entries to something else to apply. Building that something else
now, standalone, with its own tests, means we trust it completely before any
consensus code depends on it. It's also the smallest unit of concurrency risk
in the whole project — better to get locking discipline right on a 3-method
type than for the first time inside a 500-line Raft implementation.

### The Design Decision Made
Chose a plain `map[string]string` guarded by a single `sync.RWMutex`
(Option A), over `sync.Map` (Option B — wrong access pattern for us: ours is
arbitrary read/write on any key, not write-once-read-many; also loses type
safety) and a sharded map (Option C — solves a lock-contention problem we
don't have yet at Milestone 1; premature optimization per Rule 3).

`Get` returns `(value, found)` instead of using `""` to mean "missing," since
a missing key is a normal outcome, not an error. `Put`/`Delete` return
nothing — a map write cannot fail, so manufacturing an `error` return would
be dishonest about what the code does (Rule 6 exists for real failures, not
invented ones).

### How It Works — Plain English
`Store` wraps a Go map with an `RWMutex`. `Get` takes a read lock (`RLock`)
so many goroutines can read at once. `Put` and `Delete` take the full
exclusive lock (`Lock`) since they mutate the map and must run alone. `Delete`
on an absent key is a no-op, matching Go's built-in `delete()` — no error, no
panic, safe to call twice.

### Key Invariants
- INVARIANT: `data` must never be read or written without holding `mu`. `Get`
  is called from gRPC read-handler goroutines; `Put`/`Delete` will later be
  called from the Raft apply loop's goroutine. Without `mu`, concurrent
  writes to a Go map don't just produce a wrong answer — they crash the
  process (`fatal error: concurrent map writes`), confirmed directly in the
  Break It exercise below.

### The Lines That Matter Most

Line: `s.mu.RLock()` in `Get` vs. `s.mu.Lock()` in `Put`/`Delete`
Does: `RLock` allows unlimited concurrent readers; `Lock` is fully exclusive.
Breaks without it: if `Get` used `Lock`, reads would needlessly serialize against each other, defeating the reason to use `RWMutex` over plain `Mutex` at all.
Why this way: gives the correct safety property (writers exclusive) without paying for it on the read path.

Line: `defer s.mu.Unlock()`
Does: guarantees the lock releases on every return path, including future ones that don't exist yet.
Breaks without it: proven directly in Break 2 below — a manual `Unlock()` combined with an early return causes a permanent deadlock.
Why this way: cheap insurance against a bug class that's invisible until someone adds a conditional return months later.

Line: `value, found = s.data[key]`
Does: Go's two-value map read reports presence and value in one expression.
Breaks without it: a plain `value = s.data[key]` returns `""` (the zero value) for a missing key — indistinguishable from a key explicitly stored as `""`.
Why this way: mirrors `Get`'s own `(value, found)` signature — same idea carried through the whole function.

Line: `delete(s.data, key)` with no existence check first
Does: removes the key if present, no-ops if absent — Go's `delete()` guarantees this, never panics.
Breaks without it: nothing breaks without a check; adding one would just be redundant code re-implementing a language guarantee (Rule 3: no unnecessary cleverness).
Why this way: trust the language's own contract instead of restating it.

### What Breaks and Why

**Break 1 — `Put` uses `RLock` instead of `Lock`**
Change: swapped `Put`'s `s.mu.Lock()`/`Unlock()` for `s.mu.RLock()`/`RUnlock()` — a plausible copy-paste mistake from `Get`.
Prediction: `TestConcurrentAccess_NoRace` fails; likely a runtime crash (`fatal error: concurrent map writes`), not just a `-race` warning, since Go's map write detection is runtime-fatal.
Observed: exactly as predicted. `-race` first reported multiple `WARNING: DATA RACE` blocks showing concurrent writes inside `Put` from different goroutines, then the test binary itself hard-crashed with `fatal error: concurrent map writes`.
Why wrong at a deep level: `RWMutex`'s contract is "any number of readers XOR one writer," and `RLock()` is a promise to the mutex that the caller will not mutate shared state. Using `RLock` in a mutating function breaks that promise silently — the compiler cannot catch it, because `RWMutex` only tracks lock state, not what the caller actually does with it. This is exactly the class of bug that's invisible in single-threaded tests and only surfaces under real concurrency, which is why `-race` is a first-class part of this project's toolchain rather than optional.

**Break 2 — manual `Unlock()` plus an early return in `Delete`**
Change: replaced `defer s.mu.Unlock()` with a manual `Lock()`/`Unlock()` pair, and inserted `if _, exists := s.data[key]; !exists { return }` between them — modeling a plausible future "optimization" (skip work for absent keys) added by someone who forgets the lock is still held.
Prediction: `TestDelete_MissingKey_NoOp` (deletes a key that doesn't exist) permanently deadlocks, since the early-return path never reaches `Unlock()`.
Observed: exactly as predicted. With `-timeout 5s`, `go test` fired its own alarm and dumped every goroutine's stack, showing the test goroutine permanently blocked inside `Get`'s `RLock()` call, waiting on a lock `Delete` never released.
Why wrong at a deep level: this is a *liveness* bug, not a *safety* bug like Break 1 — no data was corrupted, no crash occurred, the process just silently stopped responding. That's arguably worse operationally: a crash produces a log line and a restart; a deadlock produces nothing but a node that looks alive and answers nothing.

### Interview Questions and Answers

Q1: Why `RWMutex` instead of a plain `Mutex` here — and under what conditions would that choice actually stop mattering (or start hurting you)?
A: RWMutex allows concurrent readers, useful for read-heavy workloads; Mutex is better when writes dominate since RWMutex adds coordination overhead.
Assessment: needs more depth.
What was missing: `RWMutex` has higher per-call overhead than `Mutex` due to extra atomic bookkeeping for the reader count — for a critical section as small as one map lookup, it can lose to a plain `Mutex` unless there's *actual* concurrent read contention, not just a read-heavy workload in the abstract. Also missing: Go's `RWMutex` blocks new `RLock`s once a writer is waiting, preventing writer starvation — a correctness-adjacent fact, not just a performance one.

Q2: Your `Get` returns `(value, found)`. Walk me through exactly what would go wrong if it just returned `value string` and used `""` to mean "not found" — give me a concrete scenario where that bites someone.
A: Distinguishes a missing key from a key whose value is `""`.
Assessment: needs more depth.
What was missing: no concrete scenario, which was explicitly requested. Example: `PUT("session:abc", "")` as an explicit tombstone marking a session invalidated. Without `found`, "invalidated" and "never existed" become indistinguishable — potentially a security-relevant bug in an auth check.

Q3: This store has no size limit and nothing ever evicts a key. Why is that acceptable right now, and at which future milestone does it stop being acceptable?
A: Acceptable now because the store is for correctness/testing; stops being acceptable once the project evolves into a persistent Raft service with WAL and snapshots (Milestone 8).
Assessment: mostly right, one important correction.
What was missing: conflated two different things. Milestone 8 (snapshots/compaction) bounds *Raft log / WAL* growth, not *the store's map* growth. Unbounded map growth as unique keys accumulate is never addressed anywhere in this project's roadmap — it's out of scope by design (see "What We Are Not Building": no eviction, no TTLs), not something a later milestone fixes.

Q4: Give me a real scenario later in this project (think Raft log replay) where `Delete`'s idempotency is load-bearing, not just a nice-to-have.
A: Raft may replay log entries during recovery; repeated deletes must leave the same final state without failing.
Assessment: strong.
What was missing: could be sharpened by naming the exact trigger — a node crash-restarting and replaying its WAL (Milestone 4), or a follower re-applying log entries as `commitIndex` advances past what it already applied (Milestone 6/7) — rather than "Raft may replay" in the abstract.

Q5: Is `defer` alone sufficient to make this store's locking bulletproof forever, or is there a category of bug it does not protect against?
A: No — defer prevents forgotten unlocks, but not deadlocks from lock ordering, self-deadlocks, incorrect lock usage, or holding locks too long.
Assessment: strong.
What was missing: nothing wrong, but worth naming concretely for this codebase — Go's `Mutex`/`RWMutex` are not reentrant. If a future refactor had `Put` call `Get` internally while still holding `Lock()`, it would deadlock instantly, every time, not intermittently. `defer` guards "forgot to unlock," not "locked twice from the same goroutine."

### Git Commit Message
`feat: add in-memory KV store with Get/Put/Delete and concurrency tests`

### Open Questions
- The map-growth-is-out-of-scope answer from Q3 should be revisited if a
  future milestone (none currently planned) ever adds eviction — flag this
  file if that scope ever changes.
- Should `Store` expose a way to iterate all keys (needed for snapshotting in
  Milestone 8)? Deferring that API decision until Milestone 8 rather than
  guessing now.

---

<!-- All future entries go below this line, in chronological order -->
