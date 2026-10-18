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

## Milestone 2 — Client Request IDs and Server-Side Duplicate Detection
Date: 2026-08-05

### The Problem This Solves
A client that times out waiting for a response cannot tell "my request never
arrived" from "it arrived, was applied, and the response was lost." Its only
safe move is to retry. But a naive retry, reapplied without recognition,
doesn't just repeat a harmless operation — it can land *after* a different
client's newer write and silently overwrite it, a lost update with no error
and no signal. This gives us at-most-once semantics: a given client request
is applied to the store at most once, no matter how many times the network
causes it to be resent. Built now, before gRPC (Milestone 3) exists, so the
mechanism is proven correct with plain Go calls before any network flakiness
is layered on top.

### The Design Decision Made
Chose a bounded table (Option B) that remembers only the *most recent*
RequestID per client, over an unbounded table remembering every request ever
made (Option A). Option B relies on clients keeping at most one request in
flight at a time (retry the same request; never send the next one before the
current one is acknowledged) — matching the sequential-retry model implied
by `internal/client`. This directly resolves the unbounded-growth concern
flagged in Milestone 1's interview (Q3), rather than reintroducing it.

New package `internal/dedup`, kept separate from both `internal/store`
(which explicitly promises to know nothing about Raft/gRPC/dedup) and any
future transport code — not in the original file tree, added as a design
decision confirmed with the user.

Within `Table.Execute`, the lock is held across the caller-supplied
`applyFn`, rather than exposing separate `Check`/`Record` methods. Separating
them would reopen a race window between checking and recording — proven
directly in Break 1 below.

### How It Works — Plain English
`Table` maps each `ClientID` to the last `RequestID` it processed. When a
request comes in, `Execute` compares the incoming `RequestID` to the last one
recorded for that client: if it's less than or equal, the request is a
duplicate (either an exact retry, or an even staler retry from before the
client's most recent request) and is silently ignored. If it's greater, it's
new work — the caller's operation runs, and the table remembers this as the
new latest RequestID for that client. All of this happens under one lock, so
two goroutines racing in with the same request can never both "win" the
check.

### Key Invariants
- INVARIANT: `applyFn` runs while `t.mu` is held. This makes "check" and
  "apply and record" one atomic step, closing the exact TOCTOU race Break 1
  reintroduces below.
- INVARIANT: `applyFn` must never call back into the same `Table` — Go's
  `sync.Mutex` is not reentrant, so that would deadlock every time.
- INVARIANT: lock ordering across the codebase must always acquire `dedup.Table`'s
  lock before `store.Store`'s lock, never the reverse, to avoid a
  lock-ordering cycle between the two packages.

### The Lines That Matter Most

Line: `if last, seenBefore := t.lastSeen[clientID]; seenBefore && requestID <= last`
Does: classifies an incoming request as exact-retry (`==`), stale-older-retry (`<`), or genuinely new (`seenBefore` false, or `requestID > last`).
Breaks without it: proven in Break 2 — using `<` instead of `<=` forgets the exact-retry case and reapplies operations on the very first retry.
Why this way: `seenBefore` is checked first so a brand-new client's zero-value map lookup (`RequestID(0)`) is never mistaken for "already processed request 0."

Line: `applyFn()` called before `t.lastSeen[clientID] = requestID`, both inside the same locked section
Does: makes "run the operation" and "record that it ran" a single atomic unit.
Breaks without it: proven in Break 1 — releasing the lock between check and record lets multiple goroutines all pass the check before any of them records it, reapplying the same request many times over.
Why this way: simplest way to get true atomicity, at the explicit cost of requiring `applyFn` to be fast.

### What Breaks and Why

**Break 1 — release the lock before calling `applyFn`, re-acquire it to record**
Change: split the single locked section into "check" (locked), "apply" (unlocked), "record" (locked) — a plausible "reduce lock contention" optimization.
Prediction: `TestExecute_ConcurrentSameRequest_AppliesExactlyOnce` fails with `applyCount` > 1.
Observed: passed 30 consecutive runs unmodified — the window between unlock and re-lock was too narrow to hit by scheduling luck with a near-instant `applyFn`. Widening it with a temporary `time.Sleep(time.Millisecond)` inside the test's `applyFn` made the bug reproduce immediately and consistently: `applyFn` ran 50 times instead of 1. Confirmed with plain `go test` — **not** `-race`; the race detector reported nothing, because every individual map access remained correctly protected by the lock (`-race` never saw an unsynchronized memory access to flag). The bug is a TOCTOU logic race spanning two critical sections, a category of bug invisible to happens-before analysis.
Why wrong at a deep level: correctness properties that span multiple lock acquisitions cannot be verified by looking at any single critical section in isolation — and cannot be caught by tooling that only reasons about memory synchronization, only by reasoning about the protocol itself.

**Break 2 — `requestID < last` instead of `requestID <= last`**
Change: one-character off-by-one, dropping the exact-retry case from the stale-duplicate check.
Prediction: `TestExecute_DuplicateRequest_SameID_DoesNotReapply` fails; a retry with the exact same RequestID gets reapplied.
Observed: exactly as predicted — `wasDuplicate` came back `false` for an exact retry, and `applyFn` would have run twice.
Why wrong at a deep level: the smallest possible code change (one comparison operator) directly reintroduces the lost-update scenario from the concept brief — proof that this invariant needs a dedicated test, not just careful code review, since a one-character typo is easy to miss by eye.

### Interview Questions and Answers

Q1: Why didn't `-race` catch Break 1, given it's a genuine concurrency bug? What category of bug is invisible to both plain testing and the race detector, and why?
A: The bug was a logic race, not a data race, so `-race` couldn't detect it.
Assessment: needs more depth.
What was missing: the mechanism. `-race` does happens-before analysis on individual memory accesses; every map access in Break 1 was still correctly wrapped in `Lock()`/`Unlock()`, so there was no unsynchronized access to flag. The bug lived in an application-level invariant spanning two separate critical sections — a named category, TOCTOU (time-of-check-to-time-of-use) — which no memory-synchronization tool can see.

Q2: `Execute` holds the lock across `applyFn`. What's the actual cost, specifically, and when does it become a real problem rather than a theoretical one?
A: Serializes all clients; fine now, a bottleneck under high concurrency.
Assessment: needs more depth.
What was missing: specifics. It serializes clients writing to *completely unrelated keys* against each other, even though `store.Store`'s own `RWMutex` could run them in parallel. The sharper timing question: does this matter more at Milestone 3 (plain gRPC, genuinely concurrent unrelated writes, no other serialization yet) or Milestone 7 (Raft's own single-leader log ordering may already dominate as the real bottleneck)?
Holding t.mu across applyFn serializes every write request, even when they come from different clients operating on completely unrelated keys. For example, a PUT to "user:1" from client A blocks a simultaneous PUT to "user:999" from client B purely because both must pass through the single dedup mutex. That contention is avoidable with sharded or per-client locking because dedup state is independent across clients. This matters most in Milestone 3, where the gRPC server can otherwise process unrelated requests concurrently. Once Raft is introduced, however, writes are already serialized by the leader's log and majority replication, so consensus latency is likely to dominate and the dedup mutex becomes a much smaller contributor to overall throughput. So after Raft, the dedup table is still required. What changes is that its global mutex is less likely to be the main performance bottleneck, because Raft's consensus process already serializes writes and introduces significantly more latency than acquiring the dedup lock.

Q3: What happens if a client restarts, loses its in-memory request counter, and resends RequestIDs starting low again to a server that already has a high last-seen value for that ClientID? Whose bug is that?
A: Breaks the protocol; belongs to the client/session layer, not the dedup table.
Assessment: strong.
What was missing: the concrete fix — ClientID must represent a session, not a persistent process identity. A client should mint a fresh ClientID (e.g. a new random UUID) on every restart so a reset counter lands in a new table entry instead of colliding with stale state.

Q4: This table never forgets a client — does Milestone 1's "unbounded growth is out of scope" verdict apply here too, or is this different?
A: Store growth is application data; dedup growth is metadata that should eventually be garbage-collected.
Assessment: good distinction, missing the hard part.
What was missing: garbage collecting dedup entries isn't free. Evicting a client's entry too early and then receiving one more genuine retry causes the table to treat it as a new client and reapply an already-applied operation — silently reintroducing the exact bug this milestone exists to prevent. Safe eviction needs an explicit session-close signal or a timeout provably longer than any client's max retry window, not a naive TTL.

Q5: The moment Milestone 3 wires this into real gRPC handlers, clients need an actual response, not just "did it apply." What has to change, and why wasn't it built now?
A: `Execute` will eventually return results/errors; the simpler API is appropriate until those requirements exist.
Assessment: strong.
What was missing: it's not just `applyFn`'s signature that changes — `Execute`'s *duplicate path* currently returns no payload at all, which only works because `Put`/`Delete` have nothing to return. Once there's a real response, the duplicate path must cache and replay it, not just suppress re-application.

### Git Commit Message
`feat: add request-ID dedup table for at-most-once write semantics`

### Open Questions
- Dedup table has no eviction — acceptable for now (bounded by active client
  count, not total requests), but any future GC must be tied to an explicit
  session-close signal, not a naive timeout, per the Q4 correction above.
- `Execute`'s API will need to grow a cached-response payload once Milestone
  3 wires it to real gRPC handlers — deferring that design until then rather
  than guessing now, per the Q5 correction.
- Which operations actually go through this table? Currently assumed to be
  PUT/DELETE only — GET is naturally side-effect-free and doesn't need
  dedup. Revisit if any future milestone adds a read-modify-write operation.

---

## Milestone 3 — gRPC Layer: Proto, KV Server, and Client with Retries
Date: 2026-08-08

### The Problem This Solves
Everything built through Milestone 2 only existed as Go objects called
directly by test functions in one process. A "distributed" KV store that
can't be reached over a network isn't distributed — it's several unrelated
single-node programs sharing a docker-compose file. This milestone puts a
real network boundary around `store.Store` + `dedup.Table`: a client process
can now call PUT/GET/DELETE on a server process and get a real answer back
over the wire. Scope note: no replication or consensus yet — each node
serves requests against its own local state only, proving the network
plumbing works before Raft (Milestones 5-7) makes multiple nodes agree.

Split into three sub-steps per Rule 2 (small diffs): 3a (proto + codegen),
3b (server), 3c (client). One consolidated entry here covers all three plus
wiring `cmd/node/main.go` to actually start the server.

### The Design Decision Made
**gRPC + protobuf** was already fixed by CLAUDE.md's tech stack table, not
re-litigated here — grounded instead in *why*: a strongly-typed generated
contract (a typo in a field name is a compile error, not a runtime JSON
surprise), smaller/faster binary encoding, and — the one that matters most
for this project specifically — native HTTP/2 bidirectional streaming,
which Raft's `AppendEntries` heartbeats (Milestone 6) will benefit from.

**3a — proto shape:** separate unary RPCs (`Get`/`Put`/`Delete`, each
strongly typed) over one generic `Execute(Operation)` RPC with an
enum/oneof — the generic version solves a flexibility problem this project
doesn't have (we know exactly three operations today), which is exactly the
premature abstraction Rule 3 warns against. `GetRequest` carries no
`client_id`/`request_id` at all: `Get` is read-only and side-effect-free, so
replaying it is always safe — it never goes through dedup, a decision
`kv.proto` enforces at the schema level, not just by convention.

**3b — server ownership model:** `KVServer` owns a concrete `*store.Store`
and `*dedup.Table` directly (Stance 1), not an interface anticipating a
future Raft-backed implementation (Stance 2). We don't yet know the right
shape for a Raft-facing abstraction, since Raft doesn't exist yet — Milestone
7's explicit job is to rewire this file's write path through consensus, and
guessing at that interface now would mean designing against requirements we
don't understand yet (Rule 2/3).

**3c — client identity and retries:** `ClientID` is self-generated (a random
UUID at construction) rather than server-assigned via a `RegisterClient` RPC
— the paper's server-assigned approach exists to make registration itself
linearizable through a replicated log, which doesn't exist until Milestone
5+. Retry strategy is exponential backoff (100ms→200ms→400ms...capped at
2s, 5 attempts) rather than fixed-interval, per explicit user choice over my
own lean toward fixed-interval for this milestone's single-node scope.
`internal/client` deliberately does not import `internal/dedup` — the client
only needs to produce raw `string`/`uint64` values matching the proto wire
fields; the two sides are connected through the `.proto` contract, not a
shared Go type.

### How It Works — Plain English
`kv.proto` defines the `KVStore` service and its request/response messages;
`make proto` (now actually functional, using `protoc-gen-go`/
`protoc-gen-go-grpc` installed via `go install`) generates the Go types and
interfaces both sides depend on. `KVServer` (`internal/server`) implements
that generated interface: `Get` reads straight from `store.Store`; `Put`/
`Delete` convert the proto's `client_id`/`request_id` fields into `dedup`'s
named types and route the actual store mutation through
`dedup.Table.Execute`, so a retried write is never reapplied. `Client`
(`internal/client`) wraps a generated gRPC stub, generates its own
`ClientID` once at construction, allocates a fresh `RequestID` once per
logical `Put`/`Delete` call (reused across every retry of that same call),
and retries with exponential backoff — each attempt individually
timeboxed via `context.WithTimeout`, and the whole retry loop responsive to
the caller's own context cancellation. `cmd/node/main.go` now actually
starts a `grpc.Server` on `GRPC_PORT`, serving in a goroutine whose
documented exit is `grpcServer.GracefulStop()` on shutdown signal — verified
against a real compiled binary and a real client over a real loopback
socket, not just unit tests.

### Key Invariants
- INVARIANT (`kv.proto`): once a field number is used in a released version,
  it must never be reused for a different field — protobuf's wire format
  identifies fields by number, not name.
- INVARIANT (`kv_server.go`): `KVServer` embeds `kvpb.UnimplementedKVStoreServer`
  by value (not pointer, per its own doc comment) — required for
  `*KVServer` to satisfy `kvpb.KVStoreServer` at all, and for forward
  compatibility when `kv.proto` grows new RPCs.
- INVARIANT (`client.go`): `Client` is not safe for concurrent use by
  multiple goroutines — not an implementation gap, a direct consequence of
  Milestone 2's dedup design, which assumes at most one request in flight
  per client.
- INVARIANT (`client.go`): a given logical `Put`/`Delete` call's `requestID`
  is allocated exactly once, before the retry loop starts, and reused by
  every retry attempt of that same call — proven directly by Break 1 below.

### The Lines That Matter Most

Line: `s.dedup.Execute(clientID, requestID, func() { s.store.Put(req.Key, req.Value) })` (`kv_server.go`)
Does: routes every write through the atomic check-or-apply-and-record path from Milestone 2, rather than touching the store directly.
Breaks without it: proven in 3b Break 1 — bypassing this and calling `store.Put` directly reapplies retried requests, reintroducing the exact lost-update bug from Milestone 2's concept brief.
Why this way: this is also the exact call site that exercises the Milestone 2 lock-ordering invariant — dedup's lock is held first, store's lock is taken second, inside the closure, never the reverse.

Line: `requestID := c.nextRequestID; c.nextRequestID++` (`client.go`, before the retry loop)
Does: allocates this logical call's `requestID` once, before any network activity.
Breaks without it: proven in 3c Break 1 — moving this inside the retried closure gives every retry attempt a new id, defeating server-side dedup entirely from the client side.
Why this way: allocation happens once per logical *operation*, not once per network *attempt* — the distinction the whole dedup design rests on, now enforced on both ends of the wire.

Line: `select { case <-time.After(backoff): ... case <-ctx.Done(): return ... }` (`client.go`)
Does: waits for backoff to elapse OR the caller's context to be cancelled, whichever comes first.
Breaks without it: proven in 3c Break 2 — a plain `time.Sleep(backoff)` ignores cancellation, leaving a caller who's already given up stuck waiting out the full backoff schedule anyway.
Why this way: this is the one place in the retry loop where the client is otherwise idle — exactly where respecting cancellation costs nothing.

Line: `clientID := dedup.ClientID(1)` — the Break 2 hardcode in `kv_server.go`'s `Delete` (restored, but worth remembering)
Does (when broken): silently collides a `Delete`'s dedup entry with an earlier `Put`'s, causing the `Delete` to be treated as a stale duplicate and never actually run.
Why it mattered: the most dangerous class of bug in this milestone's break-it exercises — no crash, no error, the operation just silently doesn't happen. `TestDelete_RemovesKey` exists specifically to catch this.

### What Breaks and Why

**3b Break 1 — bypass `dedup.Execute`, call `store.Put` directly in `Put`**
Prediction: `TestPut_DuplicateRequestID_NotReapplied` fails, retry reapplies with the second (wrong) value.
Observed: exactly as predicted — `Get` returned `"second-should-be-ignored"` instead of `"first"`.
Why wrong at a deep level: this is the server-side version of the exact bug Milestone 2 exists to prevent — proof that the protection has to be re-verified at every layer that could accidentally route around it, not just at the layer where it was originally built.

**3b Break 2 — hardcoded `requestID := dedup.RequestID(1)` in `Delete`**
Prediction: `TestDelete_RemovesKey` fails; `Delete`'s fabricated `requestID` collides with the `Put`'s real one, so dedup treats the `Delete` as already-seen and never runs it.
Observed: exactly as predicted — `Get` after `Delete` still found the key present.
Why wrong at a deep level: no crash, no error — the most operationally dangerous kind of bug in this set, easily misattributed to the client during real debugging.

**3c Break 1 — allocate `requestID` inside the retried closure instead of once per call**
Prediction: `TestPut_RetriesReuseSameRequestID` (a new test, added specifically because auditing the correct code revealed no existing test actually checked retries reuse one id) fails — each attempt gets a different, incrementing id.
Observed: exactly as predicted — attempt 0 used `RequestId=0`, attempt 1 used `RequestId=1`.
Why wrong at a deep level: this is the client-side mirror of 3b's Break 1 — the guarantee has exactly two load-bearing halves (client reuses the id, server recognizes the reuse), and breaking either one alone is enough to defeat the whole mechanism.

**3c Break 2 — `time.Sleep(backoff)` instead of a `select` on `ctx.Done()`**
Prediction: `TestPut_ContextCancelled_StopsRetryingEarly` fails, taking the full ~1.5s backoff schedule instead of stopping promptly at cancellation.
Observed: exactly as predicted — 1.5s instead of a quick exit.
Why wrong at a deep level: a caller that cancelled because *they* gave up would be left waiting on a client that's uselessly sleeping through retries for a request nobody wants an answer to anymore.

### Interview Questions and Answers

Q1: Walk through what happens when a client's Put succeeds server-side but the response is lost — how many times does store.Put run, and which code is responsible?
A: Exactly-once semantics come from client retrying with the same RequestID and server-side deduplication, so store.Put() executes exactly once.
Assessment: needs more depth.
What was missing: the actual hop-by-hop mechanism — attempt 1 applies and records `lastSeen`, but the client never learns this; attempt 2 (same requestID) hits `dedup.Execute`'s `requestID <= last` check, which is the single comparison responsible for `store.Put` not running a second time. Also: "exactly-once" is client-retry-until-success + server-at-most-once, not true exactly-once, a distinction Milestone 2 was careful about.

Q2: Why does GetRequest carry no ClientId/RequestId? What hypothetical change would make that wrong?
A: Get doesn't need deduplication because it's read-only; any state-changing behavior (like incrementing access counts) would invalidate that assumption.
Assessment: strong.

Q3: Concretely, what breaks at compile time without UnimplementedKVStoreServer embedded, and for whom?
A: Embedding UnimplementedKVStoreServer keeps old server implementations compiling when new RPCs are added by supplying default implementations.
Assessment: needs more depth.
What was missing: the concrete mechanism — without it, `*KVServer` stops satisfying `kvpb.KVStoreServer` the moment a new RPC is added, and every call site doing `kvpb.RegisterKVStoreServer(...)` (main.go, kv_server_test.go, client_test.go) fails to compile with a "missing method" error, even though none of those files have anything to do with the new RPC.

Q4: Two goroutines call Put concurrently on one Client — walk through exactly what goes wrong.
A: The immediate race is on nextRequestID, but the deeper issue is violating the protocol's assumption of one outstanding request per client.
Assessment: strong direction, missing the concrete failure.
What was missing: the specific outcome — both goroutines can read the same pre-increment value of `nextRequestID` and end up using the *same* requestID for two different logical writes, causing the server's dedup table to treat the second as a duplicate and silently drop it (a lost write, not a double-apply). Also: unlike Milestone 2's Break 1, this specific race (unsynchronized read-modify-write on a plain field) IS the kind `go test -race` would actually catch.

Q5: What does GracefulStop() buy us now, and how does "in-flight" change meaning after Milestone 6?
A: GracefulStop() lets existing RPCs finish. Today that means finishing a map update; after Raft, it could mean completing log replication and commit before shutting down.
Assessment: strong.

### Git Commit Message
`feat: add gRPC KV server and retrying client, wire node to serve real requests`

### Open Questions
- No TLS/auth on the gRPC connection — explicitly out of scope per CLAUDE.md's project boundaries, not an oversight.
- Client retry backoff has no jitter — fine with one test client; revisit if a future milestone has many clients retrying against the same node in sync.
- `internal/server`'s concrete ownership of `*store.Store`/`*dedup.Table` (Stance 1) is expected to be substantially rewired at Milestone 7 when writes must route through Raft consensus — this is anticipated scope for that milestone, not a gap in this one.
- `proto/raft/raft.proto` still doesn't exist — deferred to Milestone 5, consistent with Milestone 0's just-in-time file creation approach.

---

## Milestone 4 — Write-Ahead Log with fsync Durability and Crash Recovery
Date: 2026-08-10

### The Problem This Solves
Through Milestone 3, everything this system holds lives only in one Go
process's heap (`store.Store`'s map). Any crash — `kill -9`, OOM, container
restart, host reboot — loses everything instantly, directly contradicting
CLAUDE.md's own opening description (`PUT("user:123","alice") → stores the
pair, survives node crashes`). This also matters structurally, not just as a
feature gap: Raft (Milestones 5-7) doesn't invent durability — it assumes
each node already knows how to persist and recover its own log, and
replicates that guarantee across nodes. A WAL is a prerequisite Raft is
built on top of, not an add-on.

Split into 4a (WAL entry format, framing, fsync, replay — built and tested
standalone) and 4b (wiring into the server's write path and startup
replay), matching the pattern from Milestones 3 and this project's Rule 2.

### The Design Decision Made
**Entry format:** binary, length-prefixed (`[4-byte length][payload]`,
payload itself `[1-byte op][4-byte keyLen][key][4-byte valueLen][value]`)
over text/JSON-per-line — length-prefixing makes partial-write detection a
designed part of the format rather than an accident of a text parser
failing to parse a truncated line, and matches CLAUDE.md's stated
philosophy ("no abstraction hiding what fsync does").

**fsync frequency:** every `Append` fsyncs before returning, rather than
batching — the simplest correct baseline, and the one the break-it protocol
assumes. Batching is a throughput optimization with no measured need yet
(Rule 3).

**Dedup state does NOT persist across a crash** (WAL entries record only
the store operation, not `client_id`/`request_id`) — a conscious, named
gap, not a silent one. Confirmed as a real, if narrow, correctness risk by
the Q5 interview answer below: a stale client retry landing after a crash
can overwrite a different client's newer write, since the fresh
`dedup.Table` has no memory of the original request. Accepted for this
milestone because the harm is bounded and understood, not because it's
harmless.

**`dedup.Table.Execute`'s signature changed** (`func()` → `func() error`,
return value gained an `error`) — the first real failure mode the write
path has ever had (WAL I/O can fail; nothing before this milestone could).
Critically, `lastSeen` is recorded **only on success** — recording it on
failure would make a failed write permanently unretryable, since the
client's identical retry would then be mistaken for an already-handled
duplicate.

### How It Works — Plain English
`wal.Entry`/`Encode`/`DecodeEntry` (entry.go) define one log record and how
it's serialized, with every length bounds-checked before slicing so
corrupted input becomes an error, never a panic. `wal.WAL` (wal.go) wraps an
append-only file: `Append` writes the framed entry and fsyncs before
returning — a caller only treats a write as durable once `Append` returns
nil — and `ReadAll` replays every complete entry from the start, stopping
silently at the first *safe* truncation (too few bytes present — the
expected shape of a crash mid-write) but returning a hard error on the
first *unsafe* inconsistency (exactly enough bytes were present, but the
content doesn't decode — real corruption, not an expected crash artifact).
`kv_server.go`'s `Put`/`Delete` now append to the WAL *inside* the
`dedup.Execute` closure, before touching the store — write-ahead, log
first, apply second — and translate a WAL failure into a real gRPC error
(`codes.Internal`) for the first time. `main.go` opens the WAL, replays it
into a fresh `store.Store` *before* the gRPC server starts accepting any
requests, and closes the WAL on graceful shutdown alongside the gRPC
server.

### Key Invariants
- INVARIANT (`wal.go`): a caller must not treat a write as durable until
  `Append` returns nil — the fsync inside is what actually makes that true.
- INVARIANT (`wal.go`): `Append` is lock-protected; `ReadAll` is not,
  because it is only ever called once at startup, before any concurrent
  `Append` begins — a documented precondition, not an oversight.
- INVARIANT (`dedup.go`, new this milestone): `lastSeen` is updated only
  when `applyFn` succeeds — a failed operation must remain retryable.
- INVARIANT (`main.go`): WAL replay must complete before the gRPC server
  accepts any request.

### The Lines That Matter Most

Line: `if err := w.file.Sync(); err != nil { ... }` (`wal.go`, end of `Append`)
Does: forces the OS to flush its page cache for this file to physical storage before returning.
Breaks without it: proven in Break 1 — every existing test still passes, because none of them simulate a real crash. The actual danger (data lost on power loss/kernel panic) is provably *not* reproducible with a process-level `kill -9` either — see the crash-experiment writeup below.
Why this way: the single line that turns "the OS accepted these bytes" into "this survives a real crash" — the entire reason this milestone exists.

Line: the two different `err == io.EOF || err == io.ErrUnexpectedEOF` branches vs. the `DecodeEntry` failure branch in `ReadAll`
Does: treats "too few bytes present" as a safe, silent stop, but "exactly enough bytes present, content doesn't decode" as a hard error.
Breaks without this distinction: real corruption would be indistinguishable from an ordinary, harmless crash-truncation, potentially hiding actual data loss instead of surfacing it. `TestReadAll_TruncatedFinalEntry_IsSafelyIgnored` and `TestReadAll_CorruptEntry_ReturnsError` exist specifically because these two scenarios need different handling.
Why this way: a length-prefix framing violation (too few bytes) has an innocent, expected cause; a content-decoding violation with the framing intact does not.

Line: `if err := s.wal.Append(...); err != nil { return fmt.Errorf(...) }` placed *before* `s.store.Put(...)`, both inside the `dedup.Execute` closure (`kv_server.go`)
Does: makes the write durable before it's visible in memory, and makes both steps share one atomic dedup-guarded unit.
Breaks without this ordering: proven by the Q2 interview discussion — logging after checking dedup (instead of as part of the same guarded step) risks either double-logging duplicates that replay would then wrongly reapply, or losing an in-memory write across a crash before it's ever made durable.
Why this way: "write-ahead" isn't just a name — the log write has to causally precede the state-machine mutation, every time, with no gap a crash could land in.

Line: `t.lastSeen[clientID] = requestID` moved to run only after `applyFn()` returns nil (`dedup.go`)
Does: ties dedup's bookkeeping to actual success, not mere attempt.
Breaks without it: a WAL failure would permanently lock out retries of that exact request — the client would keep resending the same `requestID`, and dedup would keep telling it "already done" forever, even though nothing was ever saved. `TestExecute_ApplyFnFails_NotRecordedAsSeen` exists specifically to catch this regression.
Why this way: `lastSeen` must mean "this succeeded," not "this was attempted."

### What Breaks and Why

**Break 1 — remove `file.Sync()` (the break CLAUDE.md names explicitly for this milestone)**
Prediction: all existing tests still pass, because none simulate a real crash.
Observed: confirmed — all 13 tests passed unmodified.
The harder ask — "write a test that only catches the bug when the process is killed mid-write" — turned out to be close to a trick question. A real subprocess-`SIGKILL` experiment (`TestCrashSubprocess_SIGKILL_DuringAppend`, run and then deliberately deleted rather than kept as a misleading permanent test) showed the entry survived `SIGKILL` even with `fsync` removed. Why: `SIGKILL` only ends the *process*; the OS page cache is kernel-owned and untouched by a single process dying, so a still-running, still-healthy OS keeps the data available regardless of `fsync`. What `fsync` actually protects against — power loss, kernel panic, a hard VM/host reset — can't be reproduced by a process kill on a live, non-crashing OS. Real crash-consistency testing needs dedicated tools (`dm-flakey`, ALICE/CrashMonkey, hypervisor-level power-cycling), not a unit test. This is arguably the most important lesson of the whole milestone: knowing the limits of what a given testing technique can prove is as important as the fix itself.
Why wrong at a deep level: `Write()` returning success only means the OS *buffered* the bytes — treating that as "durable" conflates "the syscall didn't error" with "this data will survive a real crash," which are genuinely different guarantees.

**Break 2 — remove the mutex from `Append`**
Prediction: concurrent `Append` calls interleave their byte writes and corrupt the length-prefix framing; `TestAppend_ConcurrentCalls_NoRace` fails.
Observed: confirmed, but *intermittently* — roughly half of 10 repeated runs failed with exactly the predicted framing corruption (a garbage `keyLen` from interleaved bytes). Notably, `-race` never flagged anything on any run, even when the test failed — this corruption happens at the OS syscall level (two `write()` calls interleaving in the kernel), not as a Go-level unsynchronized memory access the race detector instruments. A different blind spot than Milestone 2's TOCTOU logic race, but the same lesson: `-race` is not a substitute for reasoning about what a lock actually protects.
Why wrong at a deep level: the mutex isn't protecting a Go variable here — it's protecting the *file's byte structure* from concurrent, unsynchronized `write()` calls, a category of corruption `-race` has no visibility into at all.

### Interview Questions and Answers

Q1: What did the SIGKILL crash-recovery smoke test actually prove, and what does it not prove?
A: Proves WAL recovery works, but doesn't prove fsync durability, because page cache can survive process death.
Assessment: strong.

Q2: Why does wal.Append happen inside the dedup.Execute closure, before store.Put — not before Execute is called, and not after it returns?
A: Correct order: dedup check → WAL + fsync → store → mark seen. Before = duplicates unnecessarily logged. After = crash can lose a successful store write.
Assessment: needs more depth.
What was missing: "before" isn't just wasted disk space — replay has no dedup logic at all, so a duplicate written to the WAL gets unconditionally reapplied on the next restart, reintroducing the exact bug dedup exists to prevent, just deferred to a future replay. "After" isn't about permanent lockout (dedup resets on crash too) — it's that a client can be told a write succeeded while the write itself never became durable, a narrower but still real gap.

Q3: Walk through what ReadAll does if a bit flip corrupts a key-length field without truncating the file.
A: ReadAll reads the complete frame → DecodeEntry validates keyLen → if invalid, return error. Without checksum, some valid-looking bit corruption may go undetected.
Assessment: strong, and surfaced a genuine gap.
The checksum observation is correct and wasn't something I'd already named: a bit flip that still produces an internally-consistent (but wrong) length would decode "successfully" with no error at all, silently returning corrupted data. Real log-structured storage adds a per-entry checksum (e.g. CRC32) specifically to catch this. Added as an open question below.

Q4: Why is recording lastSeen only on success, never on failure, correct?
A: If a WAL write fails, don't mark it seen — otherwise the retry gets treated as a duplicate even though the write never happened.
Assessment: strong.

Q5: Find a concrete case where "idempotent PUT/DELETE makes crash-era retries harmless" is false, using two clients and no Raft.
A: A's PUT(x=100) succeeds, B changes x=200, crash wipes dedup, A retries → x=100. Idempotent ≠ safe against stale retries.
Assessment: strong.
This is the concrete instance of the risk this milestone's concept brief flagged as an accepted, named gap — good to see it derived independently.

### Git Commit Message
`feat: add write-ahead log with fsync durability and crash recovery via WAL replay`

### Open Questions
- **No per-entry checksum** (surfaced by Q3): a bit flip landing inside a length field that still produces internally-consistent values would decode without error, silently returning corrupted data. Structural validation (bounds-checking) catches gross corruption; it does not catch this class of subtle corruption. Worth adding a CRC32 (or similar) per entry in a future pass.
- **Dedup state does not survive a crash** (Design Choice 3 / Q5): a stale client retry landing after a restart can overwrite a different client's newer write. Accepted and understood, not fixed, for this milestone — revisit if it ever needs to be closed (would require WAL entries to carry client_id/request_id and replay to reconstruct dedup.Table too, not just store.Store).
- **No batching/throttling of fsync**: every write pays a full fsync round-trip. No measured throughput need yet to justify the added complexity.
- Crash-consistency at the level `fsync` actually protects against (power loss, kernel panic) remains genuinely untested by this project's test suite, and can't be with process-kill-based tools alone — a documented limitation, not an oversight.

---

## Milestone 5 — Raft Leader Election
Date: 2026-08-15

### The Problem This Solves
Through Milestone 4 we had three separate, durable, single-node KV stores
that shared a docker-compose.yml and knew each other's addresses via an env
var nothing had ever read for its intended purpose. Not a distributed
system: a write to node1 landed only on node1, and killing node1 lost both
the data and the service.

The naive fix — every node forwards writes to every other — fails
immediately. Two concurrent writes to the same key applied in different
orders on different nodes leave replicas permanently disagreeing with no
authority to appeal to. The fix is to funnel all writes through a single
leader that assigns a definitive order. But that just relocates the
problem: who is the leader, and what happens when it dies? Designating one
permanently isn't fault tolerance, it's a single point of failure with two
idle backups.

Leader election is hard because of the fundamental constraint of
distributed systems: a node cannot distinguish "the leader crashed" from
"the network to the leader is slow." Both look like silence. If nodes react
to silence by promoting themselves, you get split brain.

Split into 5a (proto + persistent state), 5b (roles, terms, timers,
RequestVote — testable in-process), 5c (gRPC wiring + a real 3-container
cluster), matching the pattern from Milestones 3 and 4.

### The Design Decision Made
**Election timer: ticker loop over resettable time.Timer.** A goroutine
ticks every 10ms and checks `time.Since(lastHeartbeat) > electionTimeout`.
Less "elegant" than a `time.Timer` with `Reset`, but `Timer.Reset` has
genuinely subtle semantics (a fired timer may have a value sitting in its
channel; getting the drain-before-reset pattern wrong is a classic
hard-to-reproduce Go bug). Rule 3 favors the version a reader can verify by
eye, and the wasted CPU is irrelevant at 3 nodes.

**Persistent state in its own file, not the KV WAL.** `raft-state.dat` in
DATA_DIR, rewritten whole and fsynced on every change. Reusing the WAL
would make `wal.ReadAll` return a mix of KV operations and Raft metadata,
coupling two things that change for entirely different reasons.

**Atomic replacement (temp file + rename + directory fsync) rather than an
in-place 16-byte rewrite.** Presented as a genuinely optional call —
a 16-byte single-sector write is very likely atomic in practice, and plenty
of production code would rewrite in place. Chose the portable-correct
version because POSIX doesn't guarantee it and the failure it prevents is
precisely the double-vote bug. Break 2 below demonstrates why detection
can't substitute for prevention here.

**Carry `last_log_index`/`last_log_term` in RequestVote now**, vacuously
(always 0), even though the log doesn't exist until Milestone 6. Costs
nothing and avoids reopening the RPC signature next milestone.

**`Peer` interface mirrors the two gRPC methods exactly.** Unlike
kv_server.go, which translates wire types into meaningfully different
domain concepts, Raft's own concepts — term, candidate, vote granted — ARE
the domain model. There is nothing simpler to translate into. The payoff:
`internal/raft` never imports gRPC (verified — the real network client
lives in `internal/server`), and every election test runs in-process with
fake Peers calling directly into other Nodes' handlers.

**`PEER_ADDRS` format changed** from `node2:50051,node3:50051` to
`2=node2:50051,3=node3:50051`. `raft.Node`'s peers are keyed by node ID
because RequestVote/AppendEntries need to know WHICH node an address
belongs to. Inferring IDs from hostnames would have been a fragile hack
depending on a naming convention holding forever. This also gave
`internal/config` its first real parsing logic, and therefore its first
test file (it never got one during Milestone 0's lighter pass).

### How It Works — Plain English
Time is divided into numbered terms, each with at most one leader —
possibly zero, if an election fails. Every RPC carries a term. The rule
that makes this work: any node that sees a term higher than its own
immediately adopts it and reverts to follower. That single rule is how a
stale leader returning from a partition discovers it was replaced — there
is no "you're fired" message, it just learns from the next RPC it happens
to receive.

Every node starts as a follower. A follower that hasn't heard from a leader
within its (randomized) election timeout assumes the leader is dead:
increments its term, becomes a candidate, votes for itself, and asks
everyone for votes. A majority wins. Two rules together guarantee at most
one leader per term — each node votes at most once per term, and winning
requires a majority — because any two majorities of the same set share at
least one member, and that member cannot have voted twice. A leader sends
empty AppendEntries (heartbeats) on a fast interval to stop anyone else
from campaigning.

Randomization is what makes elections terminate. Identical timeouts mean
all nodes time out together, all campaign together, all split the vote, and
nobody ever wins — demonstrated empirically in Break 1.

### Key Invariants
- INVARIANT (`state.go`): `currentTerm` and `votedFor` are always written
  together as one logical fact, never independently — a setter that could
  bump the term without clearing the vote would let a stale vote from an
  old term apply to a new one.
- INVARIANT (`state.go`): disk is updated durably BEFORE memory. A failed
  `Save` returns an error without touching in-memory fields — a node that
  believes it voted with no durable record of it can vote twice after a
  restart.
- INVARIANT (`node.go`): entering a new term always clears any prior vote
  (`Save(term, NoVote)` in `becomeFollowerLocked`).
- INVARIANT (`node.go`): the election timeout is re-randomized on every
  reset, not chosen once at startup.
- INVARIANT (`election.go`): a vote is persisted before the RequestVote
  response is sent; if persistence fails, the vote is refused rather than
  granted.
- INVARIANT (`election.go`): a majority counted against a stale election
  must be discarded — re-validated on every single vote response, not once
  at the start.

### The Lines That Matter Most

Line: `if req.Term > n.state.CurrentTerm() { n.becomeFollowerLocked(req.Term) }` — first, unconditionally, in BOTH handlers
Does: adopts any higher term and reverts to follower before any other logic runs.
Breaks without it: this is the entire mechanism by which a stale leader or candidate learns it has been superseded. Skip it, or run it after other checks, and a stale leader keeps heartbeating (and from Milestone 7, accepting writes) indefinitely.
Why this way: term adoption is not something other logic gets to see conditionally — it runs first, in both handlers, no exceptions.

Line: `stillRelevant := n.role == Candidate && n.state.CurrentTerm() == electionTerm`
Does: re-validates, for every vote response, that the election it belongs to is still the one in progress.
Breaks without it: a candidate can step down or move to a later term while vote-counting goroutines from an earlier election are still in flight. Without this, a majority collected for term 5 could set `role = Leader` while `currentTerm` is already 7 — a leader of term 7 that never won term 7, coexisting with the legitimate one. Note the term half is load-bearing: after stepping down and starting a NEW election, the role check alone passes (`Candidate` again) and only the term comparison catches it.
Why this way: checked fresh on every response, because the world can change between when the election started and when each individual response arrives.

Line: `n.electionTimeout = electionTimeoutMin + time.Duration(rand.Int64N(span))`
Does: rolls a fresh random timeout on every reset.
Breaks without it: proven in Break 1 — ~20% of runs livelock with no leader ever elected.
Why re-rolled every time rather than once at startup: a one-time draw freezes each node's timeout forever, so two nodes whose initial draws landed close together would collide on every subsequent election involving both.

Line: `os.CreateTemp` → `Write` → `Sync` → `Close` → `os.Rename` → open dir → `Sync` dir
Does: makes the state update all-or-nothing and durable.
Breaks without it: Break 2 showed that a torn mix of new term + old vote is silently ACCEPTED by `LoadState` — the structural size check can't detect it. The protection is in preventing the bad state, not detecting it.
Why the directory fsync specifically: `rename` changes directory metadata, not file contents. Without fsyncing the directory, a crash right after the rename can lose the rename even though the temp file's bytes are safely on disk. This is the step people forget.

Line: `else if n.role == Candidate { n.role = Follower }` in `HandleAppendEntries` (same-term branch)
Does: steps a losing candidate down without touching the term or calling `becomeFollowerLocked`.
Breaks without it: a candidate that lost would keep re-campaigning on its own timeout, contesting terms indefinitely even while a legitimate leader heartbeats successfully.
Why not `becomeFollowerLocked`: that persists a `Save` clearing the vote — but this node validly voted for itself in this exact term, and that vote needn't be rewritten just because it lost. Using the full transition here would be a wasted disk write.

### What Breaks and Why

**Break 1 — fixed, identical election timeouts (named explicitly by CLAUDE.md)**
Change: replaced the randomized timeout with a fixed 200ms for every node.
Prediction: all three time out in the same tick window, all become candidates, all vote for themselves, none reaches a majority — `TestThreeNodeCluster_ElectsExactlyOneLeader` fails with zero leaders ever elected.
Observed: as predicted, but INTERMITTENTLY — 3 failures in 15 runs (~20%). Go's scheduler introduces just enough jitter between "simultaneous" ticks to occasionally break the tie by luck. Same lesson as Milestone 4's mutex break: a bug that only manifests 20% of the time is more dangerous than one that always fails, because it survives casual testing.
Why wrong at a deep level: this is a LIVENESS failure, not a safety one — nothing was corrupted, no invariant violated, the cluster simply never made progress. Real Raft deployments have had multi-second outages from exactly this. Randomization isn't an optimization; it's what makes the algorithm terminate at all.

**Break 2 — in-place rewrite instead of atomic replacement (demonstrated indirectly)**
Change: hand-constructed a 16-byte state file containing a NEW term (99) paired with an OLD `votedFor` (7) — exactly what an interrupted in-place write leaves behind.
Prediction: `LoadState` accepts it, since the size check passes.
Observed: confirmed — accepted with zero error, returning internally inconsistent state.
Why wrong at a deep level: the same blind spot found in Milestone 4's Q3 (structural validation catches gross corruption, not subtle content-level inconsistency) — but here it matters more, because this specific inconsistency IS the shape of the double-vote bug. The lesson: atomic rename doesn't help `LoadState` detect the bad state, it guarantees the bad state can never exist on disk. Prevention, not detection.

**Break 1b — memory updated before disk in `Save`**
Change: moved the in-memory field assignment above the disk operations.
Prediction: a forced disk failure leaves memory claiming a vote that was never persisted.
Observed: as predicted — memory showed term=2/votedFor=2 with the write having failed entirely. `TestSave_DiskFailure_DoesNotUpdateMemory` was written for this break and kept permanently.

**Deferred, not skipped — "remove the stale-log check in RequestVote"** (CLAUDE.md's second named break for this milestone). Verified rather than assumed: `candidateLogIsUpToDate` is currently hardcoded `true`, because `last_log_index`/`last_log_term` are always 0 on both sides until Milestone 6 gives nodes an actual log. There is no comparison to weaken. Flipping it to `false` would prove nothing useful — it would just make every election visibly fail, not demonstrate the subtle safety violation the check exists to prevent (electing a leader missing committed entries), which cannot exist in a system with no log. Deferred to Milestone 6 where it can be run honestly.

### Real Cluster Verification (5c)
Ran an actual 3-container docker-compose cluster, not a simulation:
- **Startup:** several terms cycled before settling (node3 briefly won term 2, then correctly stepped down when node2 won term 3) — messy but correct; different terms, not a safety violation. Then logs went silent, proving heartbeats were suppressing further elections. Confirmed stable: identical log-line count across a 5-second window.
- **Leader failure:** `docker compose stop node2` → node1 campaigned at term 4, won with 2 votes, node3 followed. Re-election in well under a second.
- **Persistent state inspected on disk:** `raft-state.dat` is exactly 16 bytes; the leader's file showed it had voted for itself, followers' files showed them voting for the leader, all at the same term.
- **Unplanned finding — the disruptive rejoin:** reviving node2 deposed node1, a perfectly healthy leader. Node2's persisted term made it campaign before hearing node1's heartbeat; term 5 > term 4 forced node1 to step down by the Term Rule. Correct per the rules of basic Raft, and a well-documented liveness limitation of the paper's algorithm — production systems add PreVote specifically to prevent it. Not built here; named as a real gap rather than discovered by accident.

Also fixed during this step: the Dockerfile pinned `golang:1.22` while `go.mod` had been bumped to `1.25.0` as a side effect of Milestone 3's `go get`. Flagged as low-risk at the time; this is where it actually broke the build.

### Interview Questions and Answers

Q1: Can two nodes simultaneously believe they are leader? If so, why isn't that a correctness problem, and what would make it one?
A: Yes, in different terms; Raft guarantees at most one leader per term; it's a violation if two leaders exist in the same term.
Assessment: correct, but thin.
What was missing: the mechanism (a stale leader is harmless because every action requiring a majority is rejected by nodes at the higher term, so from M6 it can never commit anything), and the actual answer to "what would make it dangerous" — a leader acting unilaterally without majority confirmation, concretely the Milestone 7 stale-read problem, which is a READ path issue rather than a write path one.

Q2: 5-node cluster, A leader at term 7, partition into {A,B} and {C,D,E}. What happens on each side, what is B's term after 5 minutes, and what happens on heal?
A: A stays leader at term 7 but can't get a majority; {C,D,E} elects a leader at term 8; B stays at term 7 because it still hears A; on heal A sees term 8 and steps down.
Assessment: strong — correctly resisted a leading question.
The question's framing ("give me a rough number" for B's term) implied it climbs. It doesn't: B is partitioned WITH A, receives A's heartbeats, and its election timer never fires. The scenario that produces unbounded term inflation is a LONE-node partition ({A,B,C,D} | {E}), where E campaigns and fails repeatedly — the canonical motivation for PreVote, and the amplified version of the disruptive rejoin observed in Docker.

Q3: Construct the interleaving where moving the fsync after the RPC response produces two leaders in the same term.
A: Stated the rule correctly (persist before responding; otherwise a crash loses the vote and the node can vote again in the same term).
Assessment: correct principle, but did not construct the interleaving as asked.
What was missing: the concrete sequence. A campaigns for term 5; its request to B is delayed; C grants and responds immediately with fsync pending; A reaches majority and becomes leader; C crashes before the fsync lands; C restarts with no record of the vote; B times out, campaigns for term 5, asks C; C grants a SECOND time; B reaches majority. Two leaders in term 5. The crux: the entire safety proof rests on "each node votes at most once per term," and the fsync is the only thing making that true across a crash.

Q4: Design PreVote — request contents, granting criteria, what it must NOT do, and is it safety or liveness?
A: Ask "would you vote for me?" before a real election; must not increase the real term or cause step-down; prevents disrupting healthy leaders; liveness fix — as was the fixed-timeout break.
Assessment: strong.
What was missing: the granting criteria specifically — a peer grants a pre-vote only if (a) it has NOT heard from a current leader within its own election timeout, and (b) the candidate's log is at least as up-to-date. Condition (a) is the whole mechanism: in the observed rejoin, node1 and node3 WERE hearing healthy heartbeats, so they'd refuse, node2 would never bump its term, and node1 would never step down. The disruption is prevented at the responder, not by restraint on the rejoining node's part.

Q5: I claim the `stillRelevant` check is dead code, because votes are conserved — nobody else could win term 5 if you collected a majority for term 5. Am I right?
A: Not dead code. While waiting for votes, another goroutine can receive a higher-term RPC, making the node a follower in term 6 while the election was for term 5; without the check the stale election could still make it leader, producing two leaders in term 6.
Assessment: strong — correctly rejected the argument.
What could be sharpened: the vote-conservation argument is TRUE but answers the wrong question — it proves nobody else won term 5, while the danger is becoming leader while currently IN term 6/7 having never won it. Also worth noting the two halves of the check aren't redundant: after stepping down and starting a new election, the node is `Candidate` again, so the role check passes and only the term comparison catches the stale result.

Overall: 3 strong, 2 needing depth. Conceptual grasp is solid throughout; the gap is giving the rule where the question asked for the mechanism or the concrete construction (Q1, Q3).

### Git Commit Message
`feat: add Raft leader election with terms, randomized timeouts, and heartbeats`

### Open Questions
- **No PreVote** — a restarting or reconnecting node can depose a healthy
  leader purely by having a higher persisted term. Observed live in Docker.
  A known limitation of basic Raft; production systems fix it with a
  pre-vote round. Out of scope for this milestone's roadmap entry.
- **No status/admin RPC** — the only way to observe a node's role is
  reading its logs. A real observability gap; a candidate for Milestone 10.
- **KV service isn't reachable from the host** — `docker-compose.yml` has
  no `ports:` mappings, so the containerized cluster can only be reached
  from inside the Docker network. Becomes genuinely necessary at Milestone
  7 when client writes must route to the leader.
- **Raft roles have no effect on KV requests yet** — a node serves
  PUT/GET/DELETE against its own local state regardless of whether it is
  leader or follower. That wiring is exactly Milestone 7.
- **Stale-log check in RequestVote is vacuous** and its break-it exercise
  is deferred to Milestone 6, when the log exists to make it real.

---

## Milestone 6 — Raft Log Replication
Date: 2026-10-07

### The Problem This Solves
Milestone 5 produced a cluster that elects a leader and then does nothing
with its leadership. This milestone builds what leadership is for: getting
every node's log to agree on the same sequence of commands in the same
order.

"Just send each new entry to everyone" fails three distinct ways, and each
one motivates a specific mechanism:
1. **Followers miss entries** (down, partitioned) and end up with holes in
   a log where position N is supposed to mean something. → `nextIndex`
   backtracking ships everything a follower is missing.
2. **Followers hold wrong entries** written by a since-deposed leader that
   never replicated them. Those must be detected and destroyed: the
   cluster's truth is whatever the current leader says. → the
   `prevLogIndex`/`prevLogTerm` consistency check plus conflict truncation.
3. **"Written somewhere" is not "safe."** An entry only the leader holds
   vanishes if the leader dies. → `commitIndex`: replicated on a majority,
   therefore guaranteed to survive any future election — with the Figure 8
   exception, below.

Split into 6a (the durable log + making the election restriction real),
6b (follower-side `AppendEntries`), 6c (leader-side `nextIndex`/
`matchIndex`, backtracking, `commitIndex`).

### The Design Decision Made
**Raft log is new code in `internal/raft`, not a second `wal.WAL`.** The
Raft log must delete a conflicting suffix; `wal.WAL` is append-only by
contract, and its torn-tail tolerance in `ReadAll` was designed around that
assumption. A functional difference, not a stylistic one. Alternatives
rejected: extending `internal/wal` with truncation (bends a package whose
whole contract is "append-only"), and rewriting the whole file on every
change like `state.go` (correct, but O(n) per append).

**1-indexed per the paper**, with index 0 as the "before the first entry"
sentinel so `prevLogIndex = 0` needs no special case. The 0-based-slice
translation is confined to `log.go`; every exported method takes and
returns real Raft indices.

**Hand-rolled fixed-layout encoding** (`[8 term][8 index][4 cmdLen][cmd]`)
rather than `proto.Marshal`, even though `raftpb.LogEntry` is right there:
protobuf serialization is not canonical — the same message can marshal to
different bytes across library versions — which is the wrong property for
a durable log that may later be checksummed or byte-compared.

**Truncation by recorded byte offset** — `offsets[i]` lets `TruncateFrom`
be one `file.Truncate` syscall instead of a rewrite.

**`commitIndex` is volatile, not persisted**, per the paper. A restarted
node starts at 0 and re-learns it. Persisting buys nothing (the entries are
already durable) and an over-eager restored value is strictly more
dangerous than an under-eager rebuilt one. Q3 below shows the consequence
this has for leaders — which is the no-op gap.

**`nextIndex` optimistic, `matchIndex` pessimistic.** On election,
`nextIndex = lastIndex+1` (assume the follower matches; back off on
rejection) and `matchIndex = 0` (assume nothing until confirmed). Optimism
on `nextIndex` costs a few wasted round trips; optimism on `matchIndex`
would let a fresh leader commit entries no follower holds.

**One mechanism for heartbeats and replication.** `sendHeartbeats` was
replaced by `replicateToAll`: an `AppendEntries` to a caught-up follower
carries no entries, and that IS the heartbeat.

**Simple decrement-by-one backtracking**, not the paper's conflict-hint
optimization (follower reports the conflicting term/first index so the
leader can skip back a whole term at once). Avoided a proto change; noted
as an open question.

**KV WAL deprecation deferred to Milestone 7.** Once the Raft log is
durable, the Milestone 4 WAL is redundant — they are the same idea at
different layers. Removing it now would break M4's working durability
before its replacement is wired to client writes.

### How It Works — Plain English
`Log` (log.go) is an ordered, durable list of entries on disk, each carrying
the term it was created in. On startup it replays the file, stops at a
partial trailing entry left by a crash, and — unlike the KV WAL — physically
truncates those partial bytes away, because this log is appended to right
after recovery and leftover garbage would land between valid entries.

A follower's `HandleAppendEntries` (replication.go) first handles terms as
in Milestone 5, resets its election timer, then runs the consistency check:
"do I have an entry at `prevLogIndex` with term `prevLogTerm`?" If not, it
refuses, and the leader backs up one position and tries again next tick. If
yes, it walks the incoming entries: an entry it already holds with the same
term is left alone; an entry at the same index with a *different* term is a
genuine conflict, so it truncates from there and appends the leader's
version. It only replies success after the append has been fsynced. Finally
it raises its `commitIndex` toward the leader's, capped at the last entry
this request actually delivered.

The leader (`Propose`, `replicateToAll`, `replicateToPeer`) appends new
commands to its own log stamped with its current term, then on every tick
ships each follower whatever that follower is missing, based on
`nextIndex`. On success it records `matchIndex` from exactly what it sent;
on failure it decrements `nextIndex`. After any success it recomputes
`commitIndex`: the highest index held by a majority AND belonging to the
leader's current term.

### Key Invariants
- INVARIANT (`log.go`): every write and every truncation is fsynced BEFORE
  the in-memory slice changes — memory is only ever a cache of what is
  durably on disk.
- INVARIANT (`log.go`): the Nth entry in the file carries index N; a gap or
  duplicate is a hard error at load, not a silent stop.
- INVARIANT (`log.go`, `IsUpToDate`): a later last term always wins; equal
  terms are broken by length. This is the election restriction, real as of
  6a.
- INVARIANT (`replication.go`): the election timer resets BEFORE the
  consistency check, so a rejected `AppendEntries` from a legitimate leader
  still suppresses an election.
- INVARIANT (`replication.go`): an existing entry is deleted ONLY when it has
  the same index and a DIFFERENT term — never because "I'm about to write
  here anyway."
- INVARIANT (`replication.go`): `Success` is returned only after the
  entries are fsynced.
- INVARIANT (`replication.go`): a follower's `commitIndex` never exceeds the
  last entry the current request delivered, and never decreases.
- INVARIANT (`replication.go`): `matchIndex` is computed from what the
  acknowledged request contained, not from the leader's current log length.
- INVARIANT (`replication.go`): an entry is committed by counting replicas
  ONLY if it is from the leader's current term (Figure 8); earlier entries
  commit transitively.
- INVARIANT (`election.go`): `initializeLeaderStateLocked` runs before `mu`
  is released on winning, so nothing observes `role == Leader` with nil
  replication maps.

### The Lines That Matter Most

Line: `prevTerm, exists := n.log.TermAt(req.PrevLogIndex); if !exists || prevTerm != req.PrevLogTerm { return ... Success: false }`
Does: refuses to append unless this log agrees with the leader's at the position just before the new entries.
Breaks without it: proven in Break 3 — damage at two levels. Logically, the leader recorded `matchIndex=3` for a follower whose log ended at 1 and committed entries existing on one machine. Physically, the follower's log got `Index=6` at slice position 2 and became permanently unloadable: `log entry at offset 21 has index 6, want 2`.
Why this way: agreement at one position inductively implies agreement at every earlier position (the Log Matching Property), so comparing a single entry verifies the whole prefix.

Line: `if present && existingTerm == newEntry.Term { continue }`
Does: leaves an entry alone if we already hold exactly it.
Breaks without it: the network duplicates and reorders RPCs — routinely in this implementation, since the leader sends every 10ms with a 100ms timeout and gRPC serves calls concurrently. A delayed duplicate carrying entries 5-7, arriving after 5-10 were replicated, would wipe out 8-10 under a naive "truncate from prevLogIndex+1" implementation. Q5 traced how that destroys committed data.
Why this way: truncation must be driven by an actual term mismatch.

Line: `newMatchIndex := prevLogIndex + uint64(len(entries))`
Does: credits the follower with exactly the entries in the request it acknowledged.
Breaks without it: Q4's interleaving — a `Propose` lands while the request is in flight, `n.log.LastIndex()` has grown, and the leader credits the follower with an entry it was never sent. In a 3-node cluster that's a majority, and since `Propose` stamps the current term, Figure 8 offers no protection. Verified against a mutant (`matchIndex = 2, want 1`).
Why this way: `matchIndex` records what has been verified, not what anyone currently holds. The follower's `min(leaderCommit, prevLogIndex+len(entries))` is the same rule from the other side.

Line: `if !ok || termAtIndex != currentTerm { continue }` in `advanceCommitIndexLocked`
Does: refuses to commit an old-term entry by counting replicas.
Breaks without it: Figure 8 — an old-term entry sits on a majority and is counted committed, then a later candidate whose last entry came from an intermediate term (a leader that never had it) legitimately wins and overwrites it.
Why this way: the election restriction compares last terms. Only a current-term entry on a majority raises every holder's last term high enough for that comparison to protect it (Q1).

Line: `if err := l.file.Truncate(l.size); err != nil` at the end of `replayFromDisk`
Does: physically discards a partial trailing entry at recovery.
Breaks without it: proven in Break 1 — recovery and the next append both look healthy; the corruption only surfaces on the following restart.
Why this way: required by the append-after-recovery access pattern, which is precisely what the KV WAL does not have.

### What Breaks and Why

**Break 1 (6a) — partial tail left in place at recovery**
Change: removed `l.file.Truncate(l.size)` from `replayFromDisk`.
Prediction: append-after-recovery produces an unreadable log.
Observed: recovery succeeded, the append succeeded, `LastIndex()` looked right — and the NEXT reopen failed with `log entry at offset 21 has index 33554432, want 2`, the five garbage bytes read as part of entry 2's header.
Why wrong at a deep level: the bug is silent until the next crash recovery, which is exactly when you can least afford a surprise. A node could run for days on a log it will never be able to reload.

**Break 2 (6a) — election restriction removed (CLAUDE.md's Milestone 5 break, finally real)**
Change: `candidateLogIsUpToDate := true`.
Prediction: a voter with real entries grants its vote to a candidate with an empty log.
Observed: confirmed at unit level during 6a (kept as `TestHandleRequestVote_RefusesCandidateWithStaleLog` plus its grant-when-current counterpart). The full-damage scenario promised in 6a was run after 6c, on a scratch copy of the repo: n1 leads term 1 and commits two writes to n2 while n3 is partitioned; n1 dies; n3 (empty log) campaigns in term 9. **Control, restriction intact:** n2 refuses; n3 holds 1 of 3 votes and cannot lead. **Restriction removed:** n2 grants, n3 wins legitimately, takes one write, and n2's log goes from `[committed-a committed-b]` to `[new-leader-write]` — two writes n1 had reported committed, gone. n2 is also left with `commitIndex=2, LastIndex=1`.
Why wrong at a deep level: the election restriction is the only thing that guarantees a new leader holds every committed entry. Without it, "committed" means nothing — any node can win and rewrite history.

**Break 3 (6b) — consistency check removed (named by CLAUDE.md)**
Change: deleted the `prevLogIndex`/`prevLogTerm` check.
Prediction: a lagging follower accepts the leader's optimistic first guess and is credited with entries it doesn't have.
Observed: `TestReplication_LaggingFollowerIsNotCountedAsCaughtUp` (written for this break, kept permanently) failed with `matchIndex=3 for a follower whose log ends at 1`; both consistency-check unit tests failed. A further scratch demonstration showed the follower's log physically corrupted — `Index=6` at slice position 2 — and unloadable on restart.
Why wrong at a deep level: two independent failures from one missing check. Logical: committed data on a single machine. Physical: a node that can never come back.

**Break 4 (6b) — follower acknowledges before fsync (named by CLAUDE.md)**
Change: `Log.Append` moved into a goroutine; `Success` returned immediately.
Prediction: `TestAppendEntries_EntriesAreDurableBeforeSuccess` fails.
Observed: **passed 20 out of 20 runs — the test never caught the realistic version.** Only after adding a 50ms sleep to widen the window did it fail (`reopened log LastIndex() = 0` after `Success` was returned).
Why wrong at a deep level, and why the test can't see it: two independent reasons. Timing — the goroutine usually wins the race against the test's next statement. And the Milestone 4 lesson again — even with perfect timing, a test on a live OS reads through the page cache and cannot tell "fsynced" from "buffered." That test catches gross ordering mistakes only; it is not a durability regression test, despite its name.

### Interview Questions and Answers

Q1: Prove that committing a current-term entry on a majority makes every earlier entry in that leader's log permanently safe.
A: Any future candidate needs a majority; it overlaps the majority holding the entry; by the election restriction that holder rejects a candidate without the entry; so every future leader has it; Log Matching preserves every earlier entry.
Assessment: needs more depth.
What was missing: the proof proves too much — substitute "old-term entry" and every sentence still reads true, yet Figure 8 shows the conclusion is false. The election restriction never checks containment; it compares last log terms. A current-term entry (term T) on a majority raises every holder's last term to at least T, so any winner's last entry comes from a leader of term at least T, all of whom hold the entry by induction. An old-term entry (term S) leaves holders' last term at S, so a candidate whose last entry came from an intermediate-term leader that never had it can still outrank them. Also, quorum intersection is size-independent, not specific to 5 nodes.

Q2: The election restriction (6a) and the current-term commit rule (6c) both protect committed entries. Is one redundant?
A: Neither. The restriction keeps committed entries in future leaders; the current-term rule prevents incorrectly committing an old-term entry in the first place.
Assessment: strong.
What could be sharpened: they're dependent, not just complementary — (b) is only safe because of (a), and exists to limit commitment to the cases where (a) is strong enough. Delete (a) and (b) protects nothing; delete (b) and (a) can't save old-term entries. Break 2's full-damage run is direct evidence for (a).

Q3: Full-cluster power loss after entry 5 is committed. Is data lost? How and when is it re-committed? What if no client ever writes again?
A: Not lost if fsynced; commitIndex is volatile and recomputed; a new leader can't commit an old-term entry directly, so production Raft appends a no-op in the new term; without it, commitIndex may never advance.
Assessment: strong — found a real gap.
What could be sharpened: "fsynced on a majority before being counted committed," not "before the leader acknowledged"; and there's no "if" about the new leader having it — committed means on a majority, so the election restriction guarantees every possible new leader has it. The hedge "if there is no leader no-op mechanism" — there isn't one in this code, confirmed on a scratch copy: commitIndex stayed 0 on all three nodes through 10 replication rounds, and jumped to 2 everywhere after a single current-term Propose. In Milestone 7 this means durable-but-invisible data after a restart.

Q4: Why `prevLogIndex + len(entries)` rather than `n.log.LastIndex()` for matchIndex? Construct the interleaving.
A: A Propose appends entry 6 while an RPC carrying 4-5 is in flight; on success LastIndex() is 6; the leader records matchIndex=6 and may commit 6 though it was never sent.
Assessment: strong.
What could be sharpened: "may commit" is "will commit" in a 3-node cluster — leader plus this follower is a majority, and Propose stamps the current term so Figure 8 doesn't apply. The general principle (record what was verified, not what anyone holds) is the same one behind the follower's `min(leaderCommit, prevLogIndex+len(entries))`. Exposed a test gap: none of the existing 50 tests caught this variant; `TestReplication_MatchIndexCountsOnlyEntriesActuallySent` was added and verified to fail on the mutant.

Q5: Can unconditional truncation destroy committed entries, or only uncommitted ones?
A: Yes — a delayed duplicate with prevLogIndex=7 carrying entry 8 makes the naive follower truncate 8-10 after 9-10 were committed.
Assessment: strong.
What could be sharpened: why deleting on ONE follower destroys committed data — the entry can drop below a majority while the leader, whose matchIndex only ever increases, keeps counting this follower; a leader crash in that window lets a new leader without 9-10 win legitimately. In this implementation the reordering is routine whenever latency exceeds one 10ms tick. And on the follower itself, commitIndex (never decreasing) now points past the end of its log — in Milestone 7 that is an out-of-range apply, or permanent silent divergence if 9-10 are later replaced and lastApplied is already past them.

Overall: 4 strong, 1 needs more depth. A clear improvement on Milestone 5's gap — constructions were given where they were asked for.

### Git Commit Message
`feat: add Raft log replication with consistency check, conflict repair, and commitIndex`

### Open Questions
- **No no-op entry on election** (Q3). A new leader holding only
  previous-term entries can never commit them until a client writes.
  Confirmed. Planned as the first step of Milestone 7, which is also where
  linearizable reads need it and where the apply loop must learn to skip
  no-op commands.
- **No conflict-hint optimization.** Backtracking is one index per round
  trip; a follower far behind on a different term costs one RTT per stale
  entry. The paper's hint (conflicting term + first index of that term)
  would need two new `AppendEntriesResponse` fields.
- **`matchIndex` never decreases, even on rejection.** Correct for honest
  followers, but it means a follower that loses entries (Q5's naive
  truncation, or disk corruption) keeps being counted for them with no
  detection.
- **The durability test is weaker than its name** (Break 4). It cannot
  distinguish fsynced from page-cached on a live OS. Real verification
  needs crash-consistency tooling; same limitation documented in
  Milestone 4.
- **Nothing in the running system calls `Propose` yet.** A live Docker
  cluster would show only empty heartbeats, so this milestone was verified
  in-process (51 tests) rather than on a real cluster. Milestone 7 wires KV
  writes to `Propose`.
- **The KV WAL is now redundant** and is scheduled for deletion in
  Milestone 7.
- **The log grows without bound** until Milestone 8's snapshots.
- **The leader replicates on every 10ms tick**, with no separate heartbeat
  interval. Fine at 3 nodes; it's also what makes RPC reordering routine.

---

## Milestone 7a — Apply Pipeline, Leader No-Op, Replicated Dedup
Date: 2026-10-10

### The Problem This Solves
Milestone 6 produced a replicated log that nothing used: committed entries
never reached any node's KV store, so the three replicas' data stayed
completely independent. Committing an entry isn't the same as applying it;
something has to turn committed log entries into store mutations, on every
node, in the same order, exactly once.

Two problems ride along. Milestone 6's Q3 found that a newly elected leader
holding only previous-term entries can never commit them (Figure 8 forbids
it), so after a full restart data sits on every disk and is invisible to
every reader. And dedup living in one leader's memory fails across leader
changes: a write that commits on L1 just before L1 dies gets retried
through L2, which has never seen it, and enters the log twice.

Milestone 7 is split 7a (this entry: apply pipeline, no-op, replicated
dedup — all testable in-process), 7b (writes through Propose, delete the KV
WAL), 7c (linearizable reads via ReadIndex, client failover, real-cluster
verification). The interview drill runs once, at the end of 7c, covering
the whole milestone.

### The Design Decision Made
**Apply channel over callback (Choice 1A).** Raft sends `ApplyMsg` on a Go
channel; the KV layer runs one goroutine that reads it. Rejected: a
callback passed into `NewNode`, which would run KV logic on Raft's
goroutines and invite lock-order cycles between `Node.mu` and the store's
locks. The channel keeps `internal/raft` free of KV knowledge and gives the
store exactly one writer.

**Dedup moved inside the replicated state machine.** Since every replica
applies the same log in the same order, every replica builds the same dedup
table and skips the same duplicates. A retried request may legitimately
appear in the log twice; the second copy is a no-op everywhere. Side
effect: replaying the log on restart rebuilds the dedup table, closing
Milestone 4's "dedup does not survive a crash" open question.

**No-ops are delivered to the state machine, not filtered out by Raft.**
ReadIndex (7c) will wait for the state machine's applied index to reach
the read index, which right after an election IS the no-op's index.
Filtering no-ops would make the first read after every leader change wait
forever. Raft delivers every index; the state machine decides what to skip.

**Commands encoded with protobuf (`kv.proto`'s `Command`).** A deliberate
reversal of Milestone 6's reasoning for hand-rolled log framing. Protobuf's
non-canonical encoding only matters when bytes get re-encoded; a command is
marshalled exactly once, by the leader, and every replica stores and
replays those identical bytes. The price is a stricter field-number rule:
these bytes live in a durable, forever-replayed log.

**`OP_UNSPECIFIED = 0` is reserved and refused at encode time.** proto3
omits default-valued fields, so a zero-valued Command marshals to zero
bytes, the exact shape of a no-op. Refusing it keeps the two impossible to
confuse.

**Fail-stop on an undecodable entry.** Log entries have no checksum, so
bad bytes may be corruption on this replica's disk alone. Skipping the
entry would make this replica silently diverge; stopping matches
CLAUDE.md's crash-stop failure model.

### How It Works — Plain English
Whenever a node's `commitIndex` rises, Raft drops a token into a size-1
"wake up" channel. A single applier goroutine wakes, copies every committed
entry it hasn't delivered yet (under the lock), releases the lock, and
sends each one down the apply channel in order, recording `lastApplied` as
it goes. On the other end, `StateMachine.Run` (the only goroutine allowed
to touch the store) checks each entry arrives at exactly the next index,
skips no-ops (still counting them as applied), decodes real commands, and
runs them through the dedup table so a request already applied is ignored.

When a node wins an election, it now writes a no-op in its new term before
doing anything else. Once that no-op reaches a majority, it's committed,
and by Figure 8's transitivity every older entry beneath it commits too.

A single-node cluster (allowed by `config.go`) now works: with zero peers
the node's own vote is a majority, checked before the vote-counting loop.

### Key Invariants
- INVARIANT (`apply.go`): entries are sent on `applyCh` WITHOUT holding
  `mu`. Safe only because entries at or below `commitIndex` can never be
  truncated; uncommitted entries are never copied out.
- INVARIANT (`apply.go`): exactly one applier goroutine per node, so
  delivery order is index order with no interleaving possible.
- INVARIANT (`election.go`): a leader that cannot write its no-op steps
  down WITHOUT `becomeFollowerLocked`, which would persist `(term, NoVote)`
  and free the node to vote twice in the current term.
- INVARIANT (`election.go`): leader state is initialized BEFORE the no-op
  is appended, so `nextIndex` lands on the no-op itself.
- INVARIANT (`replication.go`): a follower's `commitIndex` only ever
  increases. `min(leaderCommit, lastNewEntry)` can land below the current
  value on a short delayed request, and is then ignored.
- INVARIANT (`state_machine.go`): entries are applied exactly once, in
  index order. A gap or repeat is an error, never a silent skip or
  double-apply.
- INVARIANT (`state_machine.go`): `encodeCommand` never produces empty
  bytes; empty means no-op.
- INVARIANT (`kv.proto`): `Command` field numbers are permanent; existing
  logs are replayed forever.

### The Lines That Matter Most

Line: `case n.applyCh <- msg:` inside a `select` with `case <-n.stopCh: return`, after `n.mu.Unlock()`
Does: delivers a committed entry without holding the node's lock, and can still exit while blocked.
Breaks without it: sending while holding `mu` lets a slow KV consumer block every RequestVote and AppendEntries handler, so the node stops heartbeating and triggers cluster-wide elections because its state machine was busy. Without the `stopCh` arm, Stop leaks the applier forever when nobody is reading (`TestApplier_ExitsOnStopWhileBlockedAndClosesChannel`).
Why this way: committed entries are immutable, so a copy taken under the lock stays valid after it's released.

Line: `n.role = Follower; n.resetElectionTimerLocked()` (instead of `n.becomeFollowerLocked(...)`) when the no-op write fails
Does: steps down while leaving the persisted self-vote intact.
Breaks without it: mutant verified, persisted state became `(1, 0)` instead of `(1, 7)` — the node could grant a second vote in a term it already voted in.
Why this way: `becomeFollowerLocked` is for ENTERING a new term; here the term isn't changing.

Line: `if newCommitIndex > n.commitIndex { n.commitIndex = newCommitIndex; ... }`
Does: refuses to move `commitIndex` backward.
Breaks without it: a latent Milestone 6 bug. A delayed request carrying only entry 3, arriving with `LeaderCommit=6` after the follower had committed through 5, set `commitIndex` to 3 (mutant verified). The M6 test `NeverGoesBackward` only covered a LOWER LeaderCommit, so it never hit this path. In 7a this would have stalled application (`lastApplied` above `commitIndex`).
Why this way: committed is permanent; the min() is about not exceeding our own log, not about lowering anything.

Line: `if len(peers) == 0 { won := n.becomeLeaderLocked(electionTerm) ... }` in `startElection`
Does: lets a single node elect itself.
Breaks without it: a latent Milestone 5 bug — with zero peers the vote-counting loop runs zero times and the majority check is never reached, so the node campaigned forever (mutant verified).
Why this way: one vote out of one IS a majority; the check just has to happen somewhere the loop can't skip.

Line: `if msg.Index != expected { return fmt.Errorf(...) }` in `StateMachine.apply`
Does: turns an out-of-order or duplicate delivery into a loud error.
Breaks without it: a double-apply or skipped entry would make this replica's state differ from the others with no visible symptom.
Why this way: Raft guarantees order, but this is the one place a violation of that guarantee would become silent data divergence, so it's checked where it would cause harm.

### What Breaks and Why
No CLAUDE.md-named breaks belong to 7a; Milestone 7's two named breaks
(remove dedup → retry applied twice; followers serve reads → stale read)
run in 7c against the real cluster. Instead, each new guard was
mutation-tested on a scratch copy of the repo, never in the working tree:

- **Mutant 1: commitIndex may move backward** (restores Milestone 6's code).
  Prediction: the short-delayed-request test fails.
  Observed: `CommitIndex() = 3, want 5`.
- **Mutant 2: no zero-peer majority check** (restores Milestone 5's code).
  Prediction: single-node election never completes.
  Observed: `role = 1 (Candidate), want Leader`; the no-op test timed out waiting for an ApplyMsg.
- **Mutant 3: step down via becomeFollowerLocked.**
  Prediction: the self-vote is erased.
  Observed: `persisted (term, votedFor) = (1, 0), want (1, 7)`.

Why these matter at a deep level: two of the three were bugs already in
committed code from earlier milestones, invisible because nothing
exercised those paths until the apply pipeline depended on them. Building
the next layer is what exposed them.

### Real-Binary Verification
Ran the actual node binary as a single-node cluster (empty `PEER_ADDRS`):
it elected itself at term 1, wrote exactly one 20-byte entry to
`raft-log.dat` (a header with an empty command — the no-op), and shut down
with exit code 0. The clean exit exercises the shutdown chain: `Stop` →
applier closes `applyCh` → `StateMachine.Run` returns.

### Interview Questions and Answers
Deferred to the end of Milestone 7c, where one drill covers 7a, 7b and 7c
together — the same pattern as Milestones 3, 4 and 6.

### Git Commit Message
`feat: add apply pipeline, leader no-op, and replicated dedup state machine`

### Open Questions
- **Client requests still use the old path.** `main.go` runs the state
  machine, but its store only receives no-ops until 7b routes writes
  through `Propose` and deletes the KV WAL. Marked TRANSITIONAL in code.
- **A decode failure stops the node permanently.** Correct for crash-stop,
  but there's no automatic recovery; Milestone 8's snapshot transfer could
  rebuild such a replica.
- **Restart re-applies the whole log from index 1.** Unbounded until
  Milestone 8's snapshots.
- **The applier copies all pending entries in one batch.** Fine at this
  scale; a very far-behind node would allocate the full backlog at once.

---

## Milestone 7b — Writes Through Raft, KV WAL Deleted
Date: 2026-10-10

### The Problem This Solves
After 7a, committed entries reached every node's state machine, but client
writes still bypassed Raft entirely and landed on a separate WAL-backed
store. This step moves every write onto the replicated path and removes the
second log.

The hard part isn't calling `Propose`. It's that `Propose` returning an
index proves nothing: the handler must wait for that entry to be applied,
then prove the entry applied at that index is actually its own. Leadership
can be lost between proposing and committing, and a later leader's entry
can take the slot.

### The Design Decision Made
**Write handlers wait for apply, then check the term.** After
`WaitForApplied(index)`, the handler compares the term of the entry now at
that index with the term it proposed in. Equal means it's ours: one leader
per term, each index assigned once per term, so `(index, term)` identifies
one entry (Log Matching). Unequal means a later leader's entry won the slot.
Rejected alternative: comparing the applied command bytes. That works, but
the term comparison is O(1) and leans on a property Raft already
guarantees.

**An error means "outcome unknown," not "failed."** A timeout can fire an
instant before the entry commits. That's why the client always retries
with the same request ID, and why 7a's replicated dedup had to exist first:
the two halves only work together.

**Waiting uses a broadcast channel, not `sync.Cond`.** `appliedCh` is
closed and replaced on every apply. A `Cond` can't be selected against
`ctx.Done()`, and handlers need to give up on their own deadline.

**A `stopped` channel releases waiters at shutdown.** Once Raft stops,
nothing is ever applied again. Without it, each in-flight write would hold
`GracefulStop` for up to `applyWaitTimeout` (5s).

**The KV WAL is deleted** (`internal/wal`: 4 files, 21 tests). The Raft log
is the write-ahead log now; a restarted node rebuilds its store by
re-applying its committed log. Milestone 4's lessons stay recorded in this
journal; the code that taught them is gone.

**`Get` is leader-only and explicitly transitional.** Not yet linearizable.
Both gaps are left for 7c's ReadIndex rather than half-built here.

### How It Works — Plain English
A `Put` arrives. The server encodes it as a `Command` and hands it to Raft.
A follower refuses immediately ("not leader"). The leader appends it to its
log and gets back an index and a term. The handler then sleeps until its
node's state machine reports that index as applied — woken by a broadcast
each time anything is applied, or released early by its deadline or by the
node shutting down. Once woken, it checks the term recorded at that index.
Same term: the write is done, reply OK. Different term: this node lost
leadership and someone else's entry is committed there, so reply with an
error and let the client retry with the same request ID.

On restart, nothing special happens: the node loads its Raft log, elects
or follows, its leader's no-op commits, and every committed entry is
re-applied from index 1. That replay is the whole recovery path.

### Key Invariants
- INVARIANT (`kv_server.go`): the entry applied at an index is ours if and
  only if it has the term we proposed it in.
- INVARIANT (`kv_server.go`): an error from `replicate` means the outcome is
  unknown, not that the write failed; the client must retry with the same
  request ID.
- INVARIANT (`state_machine.go`): `WaitForApplied` checks `lastApplied` and
  reads `appliedCh` under ONE lock acquisition, so no wakeup can fall into
  the gap between them.
- INVARIANT (`state_machine.go`): `Run` closes `stopped` on every exit path,
  releasing all waiters.
- INVARIANT (`main.go`): on shutdown, `raftNode.Stop()` runs before
  `grpcServer.GracefulStop()`, so waiting handlers are released before
  `GracefulStop` waits on them.

### The Lines That Matter Most

Line: `if !ok || appliedTerm != term { return status.Error(codes.Unavailable, "leadership lost ...") }`
Does: refuses to report success for an index another leader's entry filled.
Breaks without it: mutant verified, the handler returned OK for a write a new leader had overwritten (`error = <nil> (code OK), want code Unavailable`). The client would believe a write succeeded that exists nowhere.
Why this way: the term comparison is enough because of Log Matching; it needs no byte comparison and no extra bookkeeping.

Line: `wakeup := sm.appliedCh` read under the same lock as the `lastApplied >= index` check
Does: guarantees a waiter either sees its index applied or holds the channel that will be closed when it is.
Breaks without it: mutant verified, splitting the two lock acquisitions made the 200-waiter concurrency test fail 3 out of 3 runs (`a waiter failed: context deadline exceeded — a wakeup was lost`). Notably the single-waiter test PASSED against the same mutant every time: the bug only shows under concurrency.
Why this way: this is the textbook lost-wakeup pattern; the fix is to make check-and-subscribe atomic.

Line: `defer close(sm.stopped)` in `Run`
Does: releases every waiter the moment application ends, for any reason.
Breaks without it: mutant verified, a handler still waiting 3s after the node stopped; graceful shutdown would stall on every in-flight write.
Why this way: `defer` covers both clean shutdown and fail-stop.

Line: `raftNode.Stop()` before `grpcServer.GracefulStop()` in `main.go`
Does: lets the applier close `applyCh` → `Run` returns → waiters released → `GracefulStop` has nothing left to wait on.
Breaks without it: reversed, `GracefulStop` waits on handlers that are waiting on applies that will never happen.
Why this way: shutdown order has to follow the dependency chain.

### What Breaks and Why
Milestone 7's two CLAUDE.md-named breaks (remove dedup; followers serve
reads) run in 7c on the real cluster. Here, each new guard was
mutation-tested on a scratch copy:

- **Mutant 1: no term check after apply.** Prediction: the leadership-lost
  test gets OK instead of an error. Observed: confirmed — but only on the
  second attempt. The first mutation left `term` unused, so the mutant did
  not compile, the test never ran, and the output was empty. Empty output
  looked like a pass and wasn't. Lesson: a mutation test must show the
  mutant compiling before its result means anything.
- **Mutant 2: check and channel read in two lock acquisitions.**
  Prediction: lost wakeups. Observed: the concurrency test failed 3/3; the
  single-waiter test passed 3/3. A test with one waiter cannot catch a race
  that needs two.
- **Mutant 3: `Run` never closes `stopped`.** Prediction: waiters hang past
  shutdown. Observed: both shutdown tests failed, one after a full 3s.

### Real-Binary Verification
Single-node cluster, real binary, real client over gRPC:
1. Wrote `a=1`, `b=2`, `c=3`, deleted `b`.
2. `SIGKILL` — exit status 137.
3. Data directory afterward: `raft-log.dat` and `raft-state.dat`, no
   `wal.log`.
4. Restarted the same binary on the same data: it won term 2, and reads
   returned `a="1"`, `b` not found, `c="3"`.
5. `SIGTERM` — exit status 0.

The data came back purely from Raft log replay. One honest limit: the read
ran a full second after restart, long after replay finished, so this run
does NOT exercise the transitional `Get`'s pre-replay window.

A first attempt at this run reported exit status 127 for both kills. That
was a scripting error (the node was started inside a `$(...)` subshell, so
`wait` couldn't see it), not the node's status; it was re-run correctly
before recording any numbers.

### Interview Questions and Answers
Deferred to the end of Milestone 7c (one drill for 7a–7c).

### Git Commit Message
`feat: route writes through Raft with wait-for-apply and delete the KV WAL`

### Open Questions
- **`Get` is not linearizable yet.** A cut-off leader serves old data, and
  a read right after a leader change can miss keys not yet replayed. Both
  are 7c's ReadIndex.
- **`Propose` returns `isLeader=false` for two different reasons**: not
  the leader, or the leader's own log append failed. The handler reports
  both as "not leader." Correct for the client (try another node), but the
  disk failure is invisible in the response.
- **The client only knows one node.** A "not leader" error is retried
  against the same node. Round-robin failover is 7c.
- **Every write pays a full round of fsync on a majority before replying.**
  No batching. Correct baseline; throughput is untested.
- **`TermAt` is only stable because applied entries are never truncated.**
  Milestone 8's log compaction removes old entries from the log; the term
  check must still work after compaction (snapshot metadata must carry the
  last included term).

---

## Milestone 7c — Linearizable Reads (ReadIndex) and Client Failover
Date: 2026-10-11

### The Problem This Solves
After 7b, writes were correct but reads were not linearizable. Two gaps:
a leader cut off by a partition still believes it leads and answers from
data a newer leader has changed; and a new leader's `commitIndex` can sit
below entries the previous leader already committed, so a read right after
an election could miss a write that had already returned OK.
Separately, the client knew one address: a "not leader" reply or a dead
node meant failure.

### The Design Decision Made
**ReadIndex (Choice 2B)** over reads through the log (2A: every GET pays a
majority fsync) and lease reads (2C: no round trip, but correctness depends
on bounded clock drift; a GC pause or clock jump breaks it). Cost: one
round trip to a majority per read, no disk write.

**Acknowledgement = `resp.Term == term`, not `resp.Success`.** The
confirmation round needs proof that a majority still accepts this leader's
term, not proof that their logs match. `replicateToPeer` now returns that
fact; `replicateToAll` ignores it and `ReadIndex` counts it, so one code
path serves both replication and confirmation.

**Reads before the first current-term commit fail fast** with
`ErrNotReadyForReads` rather than waiting inside Raft. The client retries.
Makes the brief post-election read pause visible rather than hidden.

**Round-robin failover (Choice 3A)** over leader hints (3B: needs new proto
fields, and a hint can itself be stale). `NewClient` is variadic, so every
existing caller compiles unchanged.

**Stale-read demonstration in-process**, on a fake network a test can
partition, because Docker can't produce a partition reliably. Two permanent
tests came out of it.

### How It Works — Plain English
A `Get` asks Raft for a read index. Raft refuses if this node isn't leader,
or if it hasn't yet committed anything in its own term (its no-op is still
in flight). Otherwise it notes its current commit index, then sends one
round of AppendEntries and counts replies that carry its own term. Once a
majority (itself included) has answered that way, no newer leader can have
been elected before the read began, because that leader would have needed
a majority and one of our responders would have told us about the higher
term. The handler then waits until the local state machine has applied
through the read index and reads the local store.

The client holds a connection to every node. After any failed attempt it
moves to the next node; the request ID stays fixed across all of them.

### Key Invariants
- INVARIANT (`read.go`): read only after this leader has committed an entry
  from its own term.
- INVARIANT (`read.go`): the confirmation round runs after `readIndex` is
  recorded, and needs acknowledgements of the term from a majority.
- INVARIANT (`replication.go`): a reply counts as a leadership
  acknowledgement iff `resp.Term == term`, regardless of `Success`.
- INVARIANT (`kv_server.go`): `Get` waits until the state machine has
  applied through `readIndex` before reading.
- INVARIANT (`client.go`): every attempt of one logical call, on any node,
  carries the same request ID.

### The Lines That Matter Most

Line: `acknowledgedTerm = resp.Term == term`
Does: counts a reply as confirming leadership based on term alone.
Breaks without it: mutant verified, counting only `Success` made ReadIndex fail with "1 of 3 nodes acknowledged term 1, need 2" when both followers were behind.
Why this way: leadership confirmation and log agreement are different questions (Q3).

Line: `if commitTerm != currentTerm { return 0, ErrNotReadyForReads }`
Does: blocks reads until this leader's own-term entry has committed.
Breaks without it: mutant verified, ReadIndex returned an index with no current-term commit, where commitIndex can lag the previous leader's commits.
Why this way: Figure 8 forbids committing old-term entries directly, so a new leader's commitIndex is unreliable until its no-op commits.

Line: `if err := n.confirmLeadership(ctx, currentTerm, peers); err != nil`
Does: the majority round that defeats a cut-off leader.
Breaks without it: Break 2b, below.
Why this way: a leader's own belief that it leads is not evidence.

Line: `c.current = (c.current + 1) % len(c.stubs)`
Does: moves to the next node after any failed attempt.
Breaks without it: a client stuck on a follower or a dead node can never succeed.
Why this way: "not leader," "dead," and "timed out" all mean the same thing to the client.

### What Breaks and Why

**Break 1 (named by CLAUDE.md): remove the duplicate detection table.**
Prediction: a retry logged twice is applied twice.
Observed: three tests failed. Over gRPC: `value = "second-should-be-ignored", want "first"`. Across a leader change: `x = "a", want "b"` — the retry erased client 2's newer write. Replica determinism: `z = "overwritten"`.
Why wrong at a deep level: without replicated dedup, "the client retries with the same ID" protects nothing; the log is allowed to hold a request twice, so something at apply time has to recognise it.

**Break 2 (named by CLAUDE.md): followers serve reads from local state.**
Prediction: a stale read.
Observed: both partition tests failed with the stale value. Cut-off follower: `served x="1" — a stale read after x=2 was acknowledged`. Cut-off old leader: the same.

**Break 2b: 7b's read rule (leader-only, no confirmation round).**
Observed: the follower test PASSED; the old-leader test FAILED with `x="1"` after the new leader had committed `x=2`. The strongest result of the milestone: leader-only reads close the follower gap and leave the stale-leader gap fully open. The confirmation round is the part doing the work.

**Mutants (both caught):** counting only `Success` (above); removing the
current-term precondition (above).

**The compile check earned its keep.** After 7b's silent non-compiling
mutant, every mutant was vetted before its result was trusted. The first
attempt at Breaks 2 and 2b did not compile (an unused `err`); without the
check they would have printed nothing and looked like passes, exactly as in
7b. Fixed and re-run.

### Real-Cluster Verification — Deferred
The planned 3-node Docker run (failover across a stopped leader, revival,
and a full-cluster stop/start reproducing Milestone 6's Q3 power-loss
scenario for real) was skipped at the user's request, to be done later.
What IS verified: every behaviour above in-process with real Raft nodes,
including leader change and partition; the full-restart no-op path via
`TestNoOp_NewLeaderCommitsPreviousTermEntriesWithoutClientWrites`; and
single-node crash recovery on the real binary (7b).

### Interview Questions and Answers (all of Milestone 7)

Q1: PUT(x,1) from c1 (req 5) commits at index 7 and is applied; the leader crashes before replying. c2's PUT(x,2) commits at 8; c1's retry is proposed at 9. Final x, what the retry receives, and every mechanism required.
A: x=2 everywhere; the retry gets OK; mechanisms: WAL and durable Raft state, election and log safety, replication and commitment, ordered application, replicated dedup, stable client/request IDs, write-handler index/term verification.
Assessment: parts 1–2 strong; part 3 needs more depth.
What was missing: mechanisms tied to what each prevents here, not categories. Election restriction (or index 7 is lost and replicas diverge); single in-order applier; dedup inside the state machine (handler dedup died with the old leader); requestID <= lastSeen; lastSeen only on success; same request ID across failover; term check at 9; WaitForApplied. Also "WAL" now reads as the Milestone 4 WAL deleted in 7b. Key insight missed: index 9 IS applied, as a skip; the handler can't tell executed from deduplicated and doesn't need to.

Q2: Prove the term check can't produce a false OK; name the properties. Could a node re-lead in the same term after the no-op failure step-down, and would the proof survive?
A: Log Matching makes same index+term the same entry; relies on Election Safety, Leader Append-Only, Log Matching, State Machine Safety. Correct election rules prevent a conflicting leader in the same term; two conflicting leaders in one term would break the proof.
Assessment: needs more depth.
What was missing: how the properties combine — only the term-T leader creates term-T entries (Election Safety); it assigns index i at most once in T (Leader Append-Only); copies are unchanged (Log Matching); applied implies committed implies stable. And the trap answered a different hypothetical: the question was the SAME node re-leading. It can't (startElection increments the term first); and even if it could, it would still be the only creator of term-T entries and nothing in term T could truncate its index-i entry, so the proof survives.

Q3: ReadIndex counts rejected AppendEntries as acknowledgements; advanceCommitIndex counts only accepted ones. Inconsistent?
A: Different questions: whether a majority acknowledges the leader's term, vs whether the entry has been replicated.
Assessment: strong.
What could be sharpened: what breaks if swapped. Strict rule in ReadIndex is safe but blocks reads on laggards (the mutant). Loose rule in commit is unsafe: commits entries that live on one machine (Milestone 6's lagging-follower break).

Q4: Snapshots save only the KV map. What breaks, on which nodes, why worse than a double apply?
A: Dedup metadata lost; a restored replica re-executes a retry while others skip it; replicas diverge; fix: snapshot KV, dedup, last-included index and term.
Assessment: strong.
What could be sharpened: with only PUT/DELETE the divergence needs an intervening write (Q1's shape), not an append operation the system doesn't have. The sneakiest node is a lagging follower receiving InstallSnapshot, which never crashed. Worse than double-apply because it's silent, permanent, the client got OK, and reads depend on which node leads.

Q5: Construct the lost wakeup; why the single-waiter test passed against it; which of the 200 waiters get stuck.
A: Exact interleaving (check 11, apply 12 closes A and arms B, waiter grabs B and sleeps). Single-waiter test may never hit the window. Waiters that grab the new channel stay stuck until another apply, cancellation or shutdown.
Assessment: part 1 strong; parts 2–3 need more depth.
What was missing: part 2 is structural, not luck — applies are 20–50ms apart against a 1ms gap, so the waiter is always parked before an apply happens. Part 3: the next apply rescues a waiter that lost its wakeup, so failures cluster at the HIGHEST indices, whose missed apply has no successor.

Overall: 2 strong, 3 need more depth. Every conclusion was correct; precision dropped exactly where the questions demanded it (named mechanisms, precise reasons, which waiters) — the Milestone 5 pattern returning after a stronger Milestone 6.

### Git Commit Message
`feat: add linearizable reads via ReadIndex and round-robin client failover`

### Open Questions
- **Real 3-node Docker verification is deferred**, including the
  full-cluster stop/start (Milestone 6 Q3) on real containers.
- **Every read costs a majority round trip.** No batching: N concurrent
  reads run N confirmation rounds where one would do.
- **Reads fail for a moment after every election** (`ErrNotReadyForReads`)
  until the no-op commits; the client absorbs it with retries.
- **Round-robin rotates even when the leader is merely slow.** A
  DeadlineExceeded from the actual leader sends the next attempt to a
  follower, costing extra hops. Leader hints would fix it.
- **Snapshots must carry the dedup table and last-included index/term**
  (Q4, and 7b's `TermAt` question) or replicas diverge.

---

## Milestone 8a — A Compactable Log and the Snapshot File
Date: 2026-10-11

### The Problem This Solves
The Raft log never shrinks and a restart replays it from index 1, so disk
use and recovery time both grow with the total number of writes ever made.
Snapshots fix that, but only if the log can lose its prefix and keep
working: every piece of code that assumed "the log starts at index 1" now
has to cope with a log that starts somewhere else. 8a builds that log, plus
the snapshot file it depends on, before anything actually takes a snapshot.

Milestone 8 is split 8a (this entry), 8b (taking snapshots: state machine
serialization including dedup, size trigger, restart from a snapshot), 8c
(InstallSnapshot and both CLAUDE.md breaks). Interview drill at the end of 8c.

### The Design Decision Made
Defaults accepted for the whole milestone: the state machine decides when
to snapshot (1A), triggered by log size in bytes (2A), log compacted by
rewrite-and-rename (3A), installed snapshots delivered through the applier
(4A), one-message transfer (5).

**The snapshot boundary is a movable sentinel.** `TermAt(snapshotIndex)`
always reports `snapshotTerm`. An uncompacted log is just the special case
`(0, 0)` — Milestone 6's "index 0 sentinel." Compaction moves it forward;
the consistency check needed no special case before and needs none now.

**The log file gets no header; the snapshot file is the authority on where
the log begins.** `OpenLog(path, snapshotIndex, snapshotTerm)`. Rejected:
recording the base inside the log file. Two reasons: no format migration
(every existing log is "a log whose snapshot is at 0"), and it turns the
crash window between saving a snapshot and compacting the log into an
ordinary code path rather than special recovery logic. The cost: two files
must agree, and a gap between them is a hard error.

**`EntriesFrom` returns `(entries, ok)`.** "Those entries are compacted"
(send the snapshot) and "nothing after the end" (send nothing) need
different responses; a nil return would let a caller confuse them.

**Snapshot file format** mirrors the log's: `[8 index][8 term][4 dataLen][data]`,
written with `state.go`'s temp-file / fsync / rename / directory-fsync
pattern. Data is opaque to Raft.

### How It Works — Plain English
The log now remembers the index and term of the last entry it threw away.
Asking for an entry at or below that point gets "gone," except for that
exact boundary entry, whose term is remembered. Asking for the last index
or last term counts the thrown-away entries, so a log that has compacted
everything still describes itself honestly.

`Compact(index, term)` is called only after a snapshot through `index` is
safely on disk. If the log holds the entry at `index` with the same term,
everything after it is kept; otherwise the log disagreed with the snapshot
(or the snapshot is ahead of it) and everything goes. The survivors are
written to a temp file, which then atomically replaces the log.

On startup, the caller loads the snapshot first and hands its index and
term to `OpenLog`. Replay drops any entry the snapshot covers — or
everything, if the log's entry at the boundary disagrees — and if it
dropped anything, rewrites the file so disk and memory agree. That is what
makes a crash between "snapshot saved" and "log compacted" harmless.

A follower whose snapshot is ahead of the leader's guess no longer rejects
AppendEntries forever: the request is trimmed to start at the follower's
snapshot boundary, since everything below it is committed and identical on
every node.

### Key Invariants
- INVARIANT (`log.go`): a fully compacted log reports the snapshot's term as
  its last term, never 0 — otherwise the election restriction switches off.
- INVARIANT (`log.go`): `TruncateFrom` never cuts at or below the snapshot
  boundary; those entries were applied, hence committed.
- INVARIANT (`log.go`): `Compact` requires a durable snapshot first; the
  boundary moves in memory only after the rewritten file is in place.
- INVARIANT (`log.go`): replay keeps entries after the boundary only if the
  log's entry AT the boundary has the snapshot's term.
- INVARIANT (`log.go`): if replay dropped anything, the file is rewritten,
  so the next append can't land after stale entries.
- INVARIANT (`log.go`): the log must begin exactly at `snapshotIndex + 1`;
  a gap is a hard error.
- INVARIANT (`replication.go`): an AppendEntries whose `prevLogIndex` lies
  inside our snapshot is trimmed to the boundary, never rejected.
- INVARIANT (`snapshot.go`): the snapshot file is replaced whole and
  atomically; any size mismatch is corruption.

### The Lines That Matter Most

Line: `if len(l.entries) == 0 { return l.snapshotTerm }` in `lastTermLocked`
Does: makes a fully compacted log report its true last term.
Breaks without it: mutant verified — `last = (5, 0), want (5, 3)`, and at the RequestVote level a fully compacted node with last term 4 voted for a candidate whose newest entry was from term 1.
Why this way: `IsUpToDate` compares last terms first; a node that misreports its term as 0 loses to ANY candidate with a non-zero term, regardless of length.

Line: `req = n.trimToSnapshotLocked(req)` in `HandleAppendEntries`
Does: re-bases a request whose `prevLogIndex` falls inside our snapshot.
Breaks without it: mutant verified — both trim tests rejected. In 8b, where followers compact on their own, a healthy follower would be rejected forever and never receive another entry. A liveness bug that nothing in the concept brief predicted; it surfaced while designing 8a.
Why this way: entries at or below our snapshot are committed, and by Leader Completeness every leader holds the same ones.

Line: `if len(survivors) != len(fileEntries) { return l.rewriteLocked(survivors) }` in replay
Does: finishes an interrupted compaction on disk, not just in memory.
Breaks without it: mutant verified — the file still started at index 1 after recovery. The next append would land after stale entries and break contiguity on the following restart.
Why this way: memory may never be ahead of, or different from, the file.

Line: `if entry.Index == l.snapshotIndex && entry.Term != l.snapshotTerm { return nil, nil }` in `entriesAfterSnapshot`
Does: drops the whole log when it disagrees with the snapshot at the boundary.
Breaks without it: mutant verified — `last = (5, 3), want (3, 7)`; a suffix from diverged history survived recovery.
Why this way: the same rule `Compact` applies, so recovering from an interrupted compaction gives exactly the result the compaction would have.

Line: building a new `AppendEntriesRequest` field by field instead of `trimmed := *req`
Does: avoids copying a protobuf message by value.
Breaks without it: `go vet` rejected the first version — generated messages embed state containing a `sync.Mutex`. Caught by vet before any test ran.
Why this way: the cost is that a new proto field must be added here too, or this path silently drops it. Noted in the code.

### What Breaks and Why
Milestone 8's two CLAUDE.md-named breaks need snapshots to actually be
taken, so they run in 8c. Here, each 8a guard was mutation-tested on a
scratch copy, each mutant vetted for compilation first:

- **M1: fully compacted log reports term 0.** Caught by the unit test.
  But the Node-level RequestVote test PASSED against it, because it used an
  empty-log candidate: with our term misreported as 0, the empty candidate
  tied on term and lost on length — refused for the wrong reason. Rewritten
  to use the candidate that actually exploits the bug (long log, stale term
  1), it now fails against the mutant. Same lesson as 7b's single-waiter
  test: a test can pass against a bug when its input happens to take a
  different safe path.
- **M2: no trim.** Both trim tests failed.
- **M3: replay never rewrites.** The crash-window test failed.
- **M4: replay keeps a diverged suffix.** The diverged crash test failed.
- **M5: TruncateFrom may cut into the snapshot.** Its test failed.

### Interview Questions and Answers
Deferred to the end of Milestone 8c.

### Git Commit Message
`feat: add compactable Raft log with snapshot boundary and snapshot file`

### Open Questions
- **Nothing takes a snapshot yet.** `main.go` passes `(0, 0)` to `OpenLog`,
  marked TRANSITIONAL. 8b must load the snapshot AND restore the state
  machine from it in the same change; a non-zero base without the restore
  would silently lose everything the snapshot covers.
- **The applier can't deliver a snapshot yet.** If the next entry to apply
  were compacted, it logs an error and delivers nothing. Unreachable in 8a;
  8b replaces it.
- **A follower behind the leader's snapshot can't be caught up yet.**
  `replicateToPeer` returns without sending. 8c's InstallSnapshot.
- **Compaction rewrites the whole surviving log.** Proportional to what
  remains, paid only at compaction. Segment files (etcd's approach) would
  avoid it.
- **7b's term check can now see a compacted index.** If a snapshot
  compacts past an index between its apply and the handler's `TermAt`, the
  handler reports "leadership lost" for a write that succeeded. Safe (an
  error means outcome unknown; the retry is deduplicated) but spurious.
  Matters once 8b triggers snapshots.

---

## Milestone 8b — Taking Snapshots and Restarting From Them
Date: 2026-10-11

### The Problem This Solves
8a built a log that can be compacted, but nothing compacted it. 8b makes
nodes take snapshots when their log grows past a threshold, and restart
from a snapshot plus the log after it instead of the whole history. This
is where "bounded disk usage" and "bounded restart time" actually happen.

### The Design Decision Made
**A revision to 8a: the log owns its snapshot.** 8a left "snapshot durable
before compacting" as a precondition every caller had to remember. 8b and
8c add two callers (local snapshots, installing a leader's snapshot), so
that would have been two places to get the order wrong. Now
`OpenLog(logPath, snapshotPath)` loads the snapshot itself, and
`Log.InstallSnapshot` (save, then compact) is the only way to compact;
`Compact` became unexported. The wrong order is no longer writable. Cost:
churn on 8a's call sites and tests one commit after shipping them. The
8a crash-window tests got more realistic as a result: they now save a real
snapshot file instead of passing numbers.

**The state machine takes the snapshot, on its own Run goroutine (1A).**
It checks `LogSize()` after each apply; over the threshold, it encodes its
KV map and dedup table and calls `Node.Snapshot(lastApplied, data)`. Being
the only writer, the Run goroutine gets a consistent cut for free.

**The dedup table is inside the snapshot** (`KVSnapshot` proto). This is
Milestone 7's Q4, turned into code and a test.

**Snapshots reach the state machine through the applier (4A)**, as an
`ApplyMsg` with a non-nil `Snapshot`. One sender, so a snapshot can never be
overtaken by an older entry.

**Deterministic protobuf marshalling** for snapshot data. Maps encode in
random order by default; equal state now gives equal bytes.

**Threshold is configurable** (`SNAPSHOT_THRESHOLD_BYTES`, default 1 MiB),
so 8c's "never compact" break can set it high.

**A failed snapshot is logged, not fatal.** It costs disk space, not
correctness: the log still holds everything.

### How It Works — Plain English
After applying each entry, the state machine asks Raft how big its log
file is. Past the threshold, it serializes everything it holds — every key
and value, plus the latest request ID seen from every client — and hands
that to Raft along with the index it has applied through. Raft looks up
that entry's term, saves the snapshot file durably, and only then rewrites
the log without the entries the snapshot now covers.

On restart, opening the log loads the snapshot file too. The applier sees
that the state machine (empty, `lastApplied` 0) is behind the snapshot,
sends the snapshot first, then sends the entries after it as they commit.
Restart work is now the snapshot plus a short suffix, not every write ever
made.

### Key Invariants
- INVARIANT (`log.go`): `InstallSnapshot` saves the snapshot durably before
  discarding a single entry, and is the only path that compacts.
- INVARIANT (`state_machine.go`): snapshots are taken only on the Run
  goroutine, between applies, so they capture exactly the state after
  `lastApplied`.
- INVARIANT (`kv.proto`): the snapshot carries the dedup table.
- INVARIANT (`apply.go`): a snapshot travels through the same goroutine and
  channel as entries, so it can never be overtaken by an older entry.
- INVARIANT (`state_machine.go`): a snapshot only moves state forward; one
  at or below `lastApplied` is an error.
- INVARIANT (`apply.go`, `Node.Snapshot`): only state at or below
  `commitIndex` may be snapshotted.

### The Lines That Matter Most

Line: `if err := SaveSnapshot(l.snapshotPath, snapshot); err != nil { return err }` before `compactLocked(...)` in `InstallSnapshot`
Does: makes the snapshot durable before anything it covers is deleted.
Breaks without it (order swapped): mutant verified with a new test that forces the save to fail (snapshot directory read-only, log directory writable). Correct order: save fails, log untouched. Swapped: `SnapshotIndex() = 2 after a failed save` — entries discarded with no snapshot holding them. A real crash can't be simulated in a test; a failed save is the closest honest proxy.
Why this way: same rule as every milestone since 4 — durable first, then discard.

Line: `LastRequestByClient: lastRequestByClient` in `encodeSnapshot`
Does: carries the dedup table inside the snapshot.
Breaks without it: mutant verified — `x = "b" on the original replica, "a" on the restored one`. Exactly the divergence predicted in Milestone 7's Q4.
Why this way: dedup state is replicated state; anything replicated must survive compaction.

Line: `if snapshot := n.log.Snapshot(); n.lastApplied < snapshot.LastIncludedIndex {` in `runApplier`
Does: delivers the snapshot before any entry after a restart.
Breaks without it: mutant verified — the applier never delivered anything, and the end-to-end restart test timed out reading `key-00` (`DeadlineExceeded`).
Why this way: the snapshot is the base everything after it applies on top of.

Line: `if n.commitIndex > n.lastApplied || n.lastApplied < n.log.SnapshotIndex() { n.signalApplierLocked() }` at the end of each `runApplier` pass
Does: re-arms the applier whenever work remains after a pass.
Breaks without it: mutant verified with a test written during the Step 5 audit — the existing restart test PASSED without the re-arm, because it always issued a fresh commit after the snapshot. The failing case: on restart, a leader's commit lands before the applier's first pass; that leaves one wakeup token, the first pass spends it on the snapshot alone, and with no further commit the entries after the snapshot are never applied (`timed out waiting for an ApplyMsg`). In a quiet cluster, indefinitely — and a ReadIndex read waiting on them times out.
Why this way: the snapshot must go out alone (in that pass `lastApplied` is still below it, so `EntriesFrom` would report compacted), so a second pass is needed; re-checking remaining work after every pass is more robust than reasoning about which signals are still pending.

Line: `if index > n.commitIndex { return error }` in `Node.Snapshot`
Does: refuses to snapshot an uncommitted entry.
Breaks without it: the entry becomes permanent on this node (TruncateFrom refuses to cut into a snapshot), so a new leader's legitimate replacement could never repair it.
Why this way: defensive — the state machine only passes lastApplied, which is always committed, so this cannot trip today and is NOT mutation-tested (a mutant would survive every current test for that reason). It guards future callers.

Line: `if current := sm.LastApplied(); msg.Index <= current { return error }` in `restoreSnapshot`
Does: refuses a snapshot that would roll state backward.
Breaks without it: mutant verified — a stale snapshot was applied over newer state.
Why this way: rolling back would undo writes clients were told succeeded.

### What Breaks and Why
CLAUDE.md's two named Milestone 8 breaks run in 8c. Here, each 8b guard was
mutation-tested on a scratch copy, every mutant vetted for compilation:

- **M1 dedup missing from snapshot:** caught (divergence, above). The first
  attempt did not compile — an over-clever mutation. The compile check has
  now caught four meaningless results across Milestones 7 and 8.
- **M2 applier never delivers the snapshot:** caught by both the restart
  unit test and the end-to-end test.
- **M3 `commitIndex` starts at 0, not the snapshot:** caught only by its
  direct unit test; the restart-delivery test PASSED against it. That
  exposed an error in my own design rationale: I had claimed, in the
  pseudocode and a code comment, that starting at 0 "would leave the
  snapshot undelivered." False — the applier's snapshot branch checks only
  `lastApplied`, never `commitIndex`. The line keeps `commitIndex >=
  lastApplied` true; nothing currently breaks without it (ReadIndex just
  reports "not ready" until the next commit). The comment was corrected to
  say exactly that.
- **M4 compact before save:** caught by the new failed-save test.
- **M5 snapshot may move state backward:** caught.

Two test bugs fixed along the way, neither in production code:
- `startApplierOnly` (a test helper mirroring `Start()` minus the election
  timer) didn't include `Start()`'s new initial wakeup, so a restart test
  waited on an applier nothing woke.
- The threshold test changed the fake log size from the test goroutine
  while the state machine was still checking after the previous apply — a
  race. Replaced with a fake that models the log: size grows with each
  applied entry and drops back after a snapshot, so the trigger point is
  deterministic.

### End-to-End Result
A single-node Raft cluster with a 4 KiB threshold took 2,000 writes over 20
keys. **The log peaked at 4,134 bytes**; without snapshots those writes
would be roughly 140 KB. Restarting the node from its data directory
returned the latest value for every key, rebuilt from snapshot plus log
suffix. (Verified in-process; the real-container run is still deferred.)

### Interview Questions and Answers
Deferred to the end of Milestone 8c.

### Git Commit Message
`feat: take snapshots by log size and restart from snapshot plus log suffix`

### Open Questions
- **Snapshots are written while holding `n.mu`.** During the write the node
  answers no RPCs and a leader sends no heartbeats; a snapshot slower than
  an election timeout would trigger an election. Fine at this scale;
  production systems write snapshots in the background.
- **Snapshots encode the entire state every time.** Cost is proportional to
  total data, not to what changed.
- **A follower behind the leader's snapshot still can't be caught up** —
  `InstallSnapshot` is 8c.
- **The spurious "leadership lost" error from 8a's open questions is now
  reachable**, since snapshots really happen.
- **Snapshot transfer will be one gRPC message**, capped at 4 MB by default.

---

## Milestone 8c — InstallSnapshot: Catching Up a Follower Behind the Snapshot
Date: 2026-10-12

### The Problem This Solves
Once a leader compacts its log, it can no longer send a lagging follower
the entries it needs: they are gone. Without a way to ship the snapshot
itself, a follower that was down or partitioned while the leader compacted
can never catch up. It falls permanently behind, and the cluster runs one
replica short for good.

### The Design Decision Made
**One `InstallSnapshot` RPC, no chunking.** The Raft paper streams large
snapshots in chunks (`offset`, `done`). Here the whole snapshot is one gRPC
message. Simpler, but capped by gRPC's 4 MB default message size.

**The leader decides when to send one, at the `EntriesFrom` miss.**
`replicateToPeer` already asked the log for entries from `nextIndex`; 8a
made that return `ok=false` when they were compacted. That miss is exactly
the trigger: send the snapshot instead.

**The follower's handler reuses everything 8a and 8b built.** It runs the
AppendEntries term rules, then `Log.InstallSnapshot` (save, then compact,
keep a matching suffix), raises `commitIndex`, and wakes the applier. The
applier delivers the snapshot to the state machine exactly as on restart.
No second delivery path.

**A failed install is an error, not a reply.** The response carries only a
term; the paper assumes installs can't fail, so the leader reads any reply
in its term as "the follower has everything through the snapshot." A
follower that couldn't write the snapshot to disk must not say that.

### How It Works — Plain English
The leader notices a follower needs entry 3, but its log now starts after
the snapshot at 5. So instead of entries it sends the snapshot: the KV data
and dedup table as of entry 5, plus that entry's index and term.

The follower checks the term like any leader message, and resets its
election timer. If it has already committed past entry 5 (the snapshot is a
late duplicate), it ignores it. Otherwise it saves the snapshot to disk,
throws away log entries the snapshot covers (keeping later ones only if
its entry 5 matches the leader's), marks everything through 5 as committed,
and wakes its applier, which replaces the state machine's contents with the
snapshot.

The leader, on a successful reply, records the follower as having
everything through 5 and goes back to sending ordinary entries from 6.

### Key Invariants
- INVARIANT (`snapshot.go`, `HandleInstallSnapshot`): a failure to store the
  snapshot is returned as an ERROR, never as a normal response — a normal
  response would credit the follower with entries it does not have.
- INVARIANT (`snapshot.go`, `HandleInstallSnapshot`): install only a
  snapshot that goes BEYOND our commitIndex, so commitIndex never moves
  backward.
- (`snapshot.go`, `sendSnapshot`): matchIndex only moves forward — a delayed
  reply never lowers it.
- Inherited, and what actually keeps delivery ordered (`apply.go`): the
  snapshot travels through the same single applier as entries, sent only at
  the start of a pass and only if `lastApplied` is below it.

### The Lines That Matter Most

Line: `if req.LastIncludedIndex <= n.commitIndex { return ..., nil }`
Does: ignores a snapshot that doesn't go past what this follower has
already committed.
Breaks without it: mutant M1 verified, `commitIndex = 3 after a stale
snapshot, want 5`. A targeted experiment (stale snapshot at 2 arriving while
the applier was mid-batch through 3) delivered 1, 2, 3 in order, but left
`commitIndex 2, lastApplied 3`: the node claims less is committed than it
has already applied.
Why this way: **correction to my own pseudocode and first comment.** I
claimed this check was what kept delivery ordered when a snapshot lands
mid-batch. M1 disproved that: the mid-batch ordering test passed without it.
Ordering comes from the applier (single sender; snapshot only at the start
of a pass, only if `lastApplied` is below it). The check's real job is
commit-index monotonicity, plus not rewriting files for nothing. Comment
corrected.

Line: `return nil, fmt.Errorf("raft: node %d failed to install snapshot ...")`
on `Log.InstallSnapshot` failure
Does: turns a disk failure into an RPC error.
Breaks without it (a normal reply instead): mutant M2 verified, `leader
credited a failed install: matchIndex 5, nextIndex 6; want 0, 1`. With
matchIndex 5 that follower counts toward a majority for entries 1..5 it does
not hold. That is Milestone 6's "commit on one machine" by a different route.
Why this way: the response has no success field. Adding one to the proto
would also work; an error reuses the leader's existing failure path ("no
credit, retry next tick") with no new code on the leader.

Line: `if snapshot.LastIncludedIndex > n.matchIndex[peerID] { n.matchIndex[peerID] = snapshot.LastIncludedIndex }`
Does: only ever raises matchIndex.
Breaks without it: mutant M3 verified. A late duplicate snapshot reply
after the follower was confirmed through 7 gave `matchIndex 5, nextIndex
6; want 7, 8`.
Why this way: replies arrive in any order; same rule as AppendEntries.

Line: `n.commitIndex = req.LastIncludedIndex` then `n.signalApplierLocked()`
Does: marks the snapshot committed and wakes the applier to deliver it.
Breaks without either: mutants M5 and M4 verified, both caught by
`TestInstallSnapshot_FollowerStoresCommitsAndWakesApplier`. The wakeup is
checked as a pending signal *before* any applier runs, because a running
applier's own startup signal would deliver the snapshot anyway and hide the
missing one.
Why this way: without the wakeup the follower holds the snapshot on disk
while its KV store stays stale until some unrelated commit arrives.

Line: `n.resetElectionTimerLocked()` in the handler
Does: counts the snapshot as hearing from the leader.
Breaks without it: mutant M6. **It first SURVIVED my test.** That test sent
a higher-term request, and the higher-term branch (`becomeFollowerLocked`)
resets the timer itself, so the line under test was never needed. Fixed by
sending a same-term request, the normal case; then caught: `last heard from
a leader 1h0m0s ago`.
Why this way: a follower being caught up may get only snapshot RPCs for a
while. Without the reset it starts an election against a healthy leader.

### What Breaks and Why

**Mutation round (scratch copies, each vetted for compilation):**

| Mutant | Result |
|---|---|
| M1 no stale check | caught (commitIndex backward); ordering test survived, which corrected the design claim |
| M2 normal reply on failed install | caught (leader credits matchIndex 5) |
| M3 matchIndex set unconditionally | caught (moved 7 → 5) |
| M4 no applier wakeup | caught |
| M5 no commitIndex raise | caught |
| M6 no election timer reset | survived first; test fixed; caught |
| M7 leader skips peer instead of sending snapshot | caught, including the 3-node end-to-end test |
| M8 dedup table left out of the snapshot | caught: the caught-up follower re-applied a retried request (`node 2 has x = "a"`) |

**CLAUDE.md break 1 — never compact, 100k writes:** written, not yet run.
`TestBreak_NeverCompact` runs the same 100k writes over 1,000 keys with
compaction off and on, and reports log size, snapshot size and restart time
for each. It is opt-in because it takes minutes:
`KVSTORE_LONG=1 go test -run TestBreak_NeverCompact -v -timeout 60m ./internal/server/`.
Deferred at the user's request; the numbers go here once it runs.
- Prediction: with compaction off the log grows linearly, at about 70 bytes
  per write, so roughly 7 MB for 100k writes, even though the live state is
  only 1,000 small keys. Restart replays all 100k entries. With compaction on
  the log stays near the 1 MiB threshold, plus a snapshot of tens of KB, and
  restart replays only the suffix.

**CLAUDE.md break 2 — restore from the snapshot without replaying the rest
of the log.** The mutant dropped every entry after the snapshot when the log
was reopened.
- Prediction: writes after the last snapshot are lost: the newest keys,
  between 0 and about 58 of them (4 KiB threshold, about 70 bytes per
  entry). Worse, the mutated replay also *rewrites* the file, so the suffix
  is erased from disk, not just skipped.
- What happened: `8 of 300 acknowledged writes lost after restart (snapshot
  at raft index 293): first key-293, last key-300`. Raft index 1 is the
  leader's no-op, so key-292 sits at 293, and everything after it was gone.
- The 2,000-write test showed something worse than loss: `key-00 = "1940"
  after restart, want "1980"`. The store went **back in time** to an older
  value. The read succeeds, so a client has no way to tell.
- Collateral: a node that never snapshotted (snapshot index 0) lost its
  *entire* log on restart, so seven plain log tests failed too.
- Why it's wrong at a deep level: a snapshot is a summary of a *prefix* of
  the log, never the whole. The suffix holds acknowledged writes. Recovery is
  always snapshot **plus** the suffix, never one or the other.
  `TestSnapshots_EveryAcknowledgedWriteSurvivesRestart` (each write on its
  own key) now guards this permanently.

**Two test bugs, and a pre-existing flake, found along the way:**
- The 3-node end-to-end test first failed with `not leader`. While isolated,
  the follower kept starting elections (term reached 11). On heal its higher
  term deposed the healthy leader, since there's no PreVote. Real Raft
  behavior; the test was writing to a deposed leader. It now retries against
  whichever node is leader, as the real client does (10/10 passes).
- M6 survived its first test (above).
- `TestThreeNodeCluster_ElectsExactlyOneLeader` (Milestone 5) failed in two
  mutant runs for reasons unrelated to snapshots. On a clean checkout of the
  last commit it failed **10 of 90** runs. It checked for two followers the
  instant a leader appeared, but a node that timed out in the same election
  stays a Candidate until the first heartbeat reaches it. That's a legal
  transient. It now waits for one leader *and* two followers (0 of 90).

### End-to-End Results
- **Catch-up through a snapshot (in-process, 3 real nodes):** one follower
  isolated, 200+ writes with a 2 KiB threshold so the leader compacted past
  everything the follower lacked, then healed. The follower caught up with
  no entry 2 in its log (so through the snapshot, not entries), held
  identical data, and skipped a retried request applied before the snapshot.
- **Break 1:** deferred (see above).

### Interview Questions and Answers (Milestone 8 drill)

**Q1. Restart with `snapshot.dat` at 293 and a log file holding 280–310.
What does `OpenLog` keep? What if the file's entry 293 has term 4 but the
snapshot says 5? How can entries before the snapshot end up in the file?**
Answer: keeps 294–310 and drops 280–293. On a term mismatch, trust the
snapshot (`TermAt(293)` returns 5). Old entries remain when the node saved
the snapshot and crashed before compacting.
**Needs more depth.** The first and third parts are right; the crash window
between `SaveSnapshot` and `compactLocked` is exactly the legitimate case.
Two things were missing:
- Dropping is physical, not just logical: `rewriteLocked` rewrites the file,
  finishing the interrupted compaction. Otherwise the next append would land
  after stale entries.
- On a term mismatch, **294–310 are discarded too**. `entriesAfterSnapshot`
  returns nothing. Those entries were appended after a term-4 entry 293; by
  Log Matching, a log that differs at 293 cannot be trusted after it. It is
  safe because none of them can be committed: committed entries form one
  prefix that includes term 5 at 293, and these sit on a branch that
  contradicts it.

**Q2. Make the strongest case for Raft, not the state machine, deciding when
to compact. Then: what breaks if Raft snapshots from its own timer goroutine
mid-apply?**
Answer: Raft owns the log and can watch its size independently. A timer
could snapshot halfway through applying, so the data wouldn't match its
index; the snapshot must be exactly the state after `LastIncludedIndex`.
**Needs more depth.** The second half is right; name both failure
directions:
- The data includes entry 101 but claims 100: a restore re-applies 101.
- The data claims 101 but lacks it: that write is lost.

The case for Raft deciding was weak. The strongest argument is that **only
Raft knows replication progress (`matchIndex`)**. It can hold off compacting
entries a nearly caught-up follower still needs, and avoid sending it a whole
snapshot when a few entries would do. Our state machine compacts blind.

**Q3. If the stale check doesn't order delivery, what does? Trace commitIndex
3, log to 10 (uncommitted, some stale), snapshot at (7, term 6).**
Answer: the single applier delivers sequentially and advances
`lastApplied`. If entry 7 has term 6, discard 4–7 and keep 8–10; otherwise
discard 4–10. Safe because they are uncommitted.
**Needs more depth; the safety reasoning is wrong as stated.** The trace is
right, and entries 1–3 go as well since the snapshot covers them. Two gaps:
- The exact conditions: the applier sends a snapshot only at the **start of
  a pass** and only if `lastApplied < snapshotIndex`. A batch already copied
  (all at or below `commitIndex`) finishes first, so a snapshot can never be
  followed by an entry it covers.
- "Uncommitted, so discardable" is false in general. An uncommitted entry
  this follower acknowledged may be one of the copies the leader is counting
  toward a majority; discarding it is Milestone 6's ack-before-durable bug.
  The real argument:
  - If entry 7 *mismatches* the leader's committed entry 7, then by Log
    Matching our 8–10 don't match the leader's either. This follower could
    never have acknowledged them to the current leader, so no majority
    depends on them.
  - If 7 *matches*, 8–10 are kept but not committed (`commitIndex` becomes
    7). Normal AppendEntries consistency checks will repair them later if
    they conflict.

**Q4. Why an error instead of a `success` field? What does the leader do in
each case? The install succeeds but the reply is lost: what happens next?**
Answer: an error reuses the leader's retry path without changing the proto.
On success the leader updates its progress; on error it gives no credit and
retries. On a lost reply the leader retries, and the follower sees it already
committed through that index, ignores it, and replies normally.
**Strong.** Correct trace: `commitIndex` is now 7, so the retry hits the
stale check and the normal reply is *true*. Two additions:
- The leader re-sends the **whole snapshot** to learn that. That costs
  bandwidth, not correctness.
- If the follower restarted in between, its `commitIndex` starts at
  `SnapshotIndex()` (8b's init line), so the retry is still ignored.

**Q5. 800 MB snapshot, follower offline for an hour: every failure, its
production fix, and which ones take down the whole cluster?**
Answer: a table covering the 4 MiB limit (chunking), memory (bounded
buffers), heartbeats delayed by the transfer (separate them), the leader
mutex held during network I/O (release it), and the follower's election
timeout (reset on leader contact). Lock contention, heartbeat delay and
memory exhaustion can take down the cluster; the follower's timeout usually
affects only that follower.
**Needs more depth; two claims are wrong about our code.**
- *Wrong:* the leader does **not** hold `mu` during the transfer;
  `replicateToPeer` unlocks before `sendSnapshot`. Where a lock IS held
  through 800 MB of disk I/O:
  - `Node.Snapshot` writes and fsyncs under `n.mu`. The leader stops
    heartbeating, which triggers an election. That is cluster-wide.
  - The follower's `HandleInstallSnapshot` does the same, but that only
    affects the follower.
- *Wrong:* the follower's timeout is **not** local. A unary handler runs
  only after the whole message arrives, so the timer isn't reset during a
  long transfer. The follower times out and campaigns with a higher term;
  with no PreVote that **deposes the leader**, repeatedly, every transfer
  attempt. Its stale log means it never wins, so the cluster churns through
  elections. That is the cluster-wide failure. Fix: PreVote and CheckQuorum.
- *Missed entirely:*
  - **`rpcTimeout` is 100 ms.** 800 MB takes about 6 seconds even at
    1 Gbps, so every attempt is cancelled and the follower can never catch
    up, even if the size limit were raised. Fix: a separate, much longer
    timeout or a streaming transfer.
  - **Duplicate transfers.** Every 10 ms tick starts another send, and each
    marshals another 800 MB copy, which is how memory actually blows up.
    Fix: one snapshot in flight per follower (etcd pauses a follower's
    progress tracker while a snapshot is in flight).
  - The log keeps `snapshot.Data` resident in memory on every node,
    permanently. Fix: stream the snapshot from disk.

**Overall:** solid on the mechanics: what the code does, the traces, the
retry path. Weaker on *why it is safe* (Q1's mismatch case, Q3's discard
argument) and on judging cluster-wide impact (Q5). In interviews, the
safety justification is the part that separates answers.

### Git Commit Message
`feat: catch up lagging followers with InstallSnapshot`

### Open Questions
- **No PreVote.** A healed partitioned node disrupts a healthy leader with
  its inflated term (seen in the end-to-end test). Raft §9.6's PreVote fixes
  this.
- **Snapshots over 4 MB can't be sent.** Chunking (`offset`/`done`) or a
  bigger gRPC message limit would be needed.
- **The whole snapshot is held in memory** on both sides during transfer.
- **The leader sends the snapshot while a previous one may still be in
  flight.** Each heartbeat tick can start another transfer to the same slow
  follower. That's harmless for correctness (the stale check makes
  duplicates no-ops), but wasteful for big snapshots.
- **The Docker real-cluster run for 7 and 8 is still deferred**, and so is
  the 100k-write Break 1 measurement.

---

## Milestone 9a — The Consistent Hash Ring
Date: 2026-10-17

### The Problem This Solves
One Raft group caps the system at one leader's write throughput, one
machine's memory, and snapshots the size of the whole dataset. Sharding
splits keys across several Raft groups, and every node and client then
needs the same answer to "which group owns this key?" with no coordination
at lookup time. 9a builds that answer: a deterministic consistent hash ring.

### The Design Decision Made
The Milestone 9 choices, made together:

| Choice | Picked | Main alternative | Why, for our constraints |
|---|---|---|---|
| Key → shard | consistent hash ring, 100 points per group, SHA-256 truncated to 64 bits | fixed hash slots (Redis Cluster); modulo | CLAUDE.md names it; adding a group moves ~1/(N+1) of keys, only to the new group |
| Placement | 3 groups on our 3 nodes (multi-Raft) | a separate 3-node set per group | keeps the 3-node cluster; the production architecture |
| Routing | smart client sends a group ID, server checks it (`WRONG_GROUP`) | server forwarding; leader-hint redirects | one hop; a client/server ring mismatch becomes an error, not a wrong answer |
| Map location | static `SHARD_GROUPS` | a config service Raft group | inside CLAUDE.md's "no automatic rebalancing" boundary |
| Rebalancing | group count fixed at creation | an offline resharding tool | no half-finished migration can corrupt data |

Honest costs, recorded on purpose:
- With a fixed group count, the ring's headline benefit (little movement on
  resize) never runs in the live system; modulo would route just as well.
  The benefit is measured in tests and matters for a future resize.
- Every node still holds every shard, so storage does not scale, and two
  dead nodes still take every group down.
- Because the map is not in the Raft log, the ownership check must happen
  **before proposing, never while applying** (replicas with different
  configs would apply differently, and diverge). That shapes 9c.

**Hash function, chosen on measurement.** The point labels (`group-1#0`,
`group-1#1`, ...) differ only in their last bytes.
- FNV-1a: barely moves the high bits that decide ring position. Measured
  6.5% / 28.5% / 65.0% with 100 points per group.
- SHA-256 truncated to 64 bits: 33.6% / 31.9% / 34.5%, at about 300ns per
  lookup.
- `hash/maphash`: ruled out; its seed is random per process, so every node
  would build a different ring.

### How It Works — Plain English
Picture a clock face with 2^64 positions instead of 12. Each group drops 100
markers on it, at positions computed by hashing `group-<id>#<n>`. To find a
key's owner, hash the key to a position and walk clockwise to the first
marker; that marker's group owns the key. Walking past the top of the clock
wraps back to the first marker.

Many markers per group keep the arcs, and so the load, even. A new group's
markers each take over a small arc from whichever group used to own it.
Only those keys move, and they all move to the new group.

The ring is built once and never changes, so any number of goroutines can
use it without a lock.

### Key Invariants
- INVARIANT: a Ring is immutable once New returns; any number of goroutines
  may call Owner concurrently with no mutex.
- INVARIANT: the ring is a pure function of the SET of groups. The same set
  gives an identical ring on every process, whatever order the groups were
  listed in. The group-ID tie-break makes the sort a total order, even on a
  64-bit position collision.
- Compatibility contract (comments on `PointsPerGroup` and `hash`, pinned by
  a test): the hash, the label format, the byte order and the point count
  decide where every key lives. Changing any of them in a running cluster
  makes existing data appear to vanish.

### The Lines That Matter Most

Line: `if i == len(r.points) { i = 0 }`
Does: wraps past the last point to the first; makes the ring a circle.
Breaks without it: B1, verified: `panic: index out of range [300] with
length 300`. Not only in the wrap test: about 1 in 300 real keys hashes past
the last point, so ordinary 10k-key tests panicked.
Why this way: `sort.Search` returns `len` when nothing is `>=`, which is
exactly the "past the last point" case.

Line: `return points[a].group < points[b].group` (the tie-break in `sortPoints`)
Does: orders two points with the same position the same way on every
process.
Breaks without it: B6. It **survived** the first test suite, because real
SHA-256 positions never collide among 300 points. The comparator was moved
into `sortPoints` so a test could build a collision by hand; then caught:
`sorted to [{5 1} {5 2}] from one input order and [{5 2} {5 1}] from the
other`.
Why this way: `sort.Slice` is not stable, and even a stable sort would keep
input order. A tie must be broken by something every process agrees on.

Line: `return r.points[i].position >= position`
Does: a position belongs to the first point AT or after it.
Breaks if changed to `>`: B4. It **survived** at first, and my prediction
(that the exact-position probe would catch it) was wrong. The probe used
`points[1]`, whose next point happened to belong to the same group, so
"this point" and "the next point" gave the same answer. The probe now picks
a point whose successor is in a different group; caught.
Why this way: either convention works if everyone uses the same one. `>=`
matches `sort.Search`, and the pinned test fixes it as part of the contract.

Line: `sum := sha256.Sum256([]byte(s)); return binary.BigEndian.Uint64(sum[:8])`
Does: places strings on the ring deterministically.
Breaks if swapped for FNV-1a: B2, verified: `group 1 owns 6.5% of keys`,
exactly the Python model's prediction, and the pinned hash changed.
Why this way: measured evenness; deterministic across processes; cost
negligible next to fsync.

Line: `const PointsPerGroup = 100`
Does: sets the number of virtual nodes, and with it the balance.
Breaks if changed to 200: B5. As predicted, the **balance test still
passed**; only the pinned layout and the model cross-check caught it. That
is exactly their job: a change can be "valid" and still destroy a deployed
cluster's data placement.

### What Breaks and Why
All six breaks were caught on the final code. The two that first survived,
B4 and B6, exposed test gaps, not code bugs, and both gaps were fixed. B3
(forgetting to sort) failed six tests, including `"user:0" moved from group 2
to group 1 — between two OLD groups`: a binary search over unsorted points
returns garbage that merely looks plausible.

### Verification Against an Independent Model
The Go ring reproduced the Python model **exactly**: 33,617 / 31,887 /
34,496 of 100k keys, and 23.0% moved when adding group 4, all of them to
group 4. Modulo, measured in the same tests, moved 75.1% of keys, 50.1%
between groups that already existed. The pinned owners and the pinned
`hash("group-1#0")` come from the Python model, not from this code's own
output.

### Interview Questions and Answers
Deferred to the end of Milestone 9.

### Git Commit Message
`feat: add consistent hash ring for mapping keys to shard groups`

### Open Questions
- **Hot keys.** No partitioning scheme spreads a single hot key.
- **Leader placement.** Nothing makes the 3 groups' leaders land on
  different nodes (9b). Without leader transfer, all three can end up on one
  node.
- **Resize is documented, not built (5A).** A real resize needs shard
  handoff: freeze the shard, copy its data *and* dedup state, and check the
  config version when each entry is applied.

---

## Milestone 9b — Raft Transport for Many Groups
Date: 2026-10-17

### The Problem This Solves
In 9c each process will host several Raft groups, one per shard, all
behind one port. A Raft message must reach exactly the group it belongs to.
Delivered to the wrong group, an AppendEntries would splice one log's
entries into another, and no Raft check would notice: terms and indexes from
two independent logs can line up by coincidence.

### The Design Decision Made
- **Sub-steps re-split by layer.** The original plan had 9b start three
  groups in `main.go`, but routing keys to groups waits for 9c, so in between
  every key would silently land in group 1. Now 9b is transport only, and
  `main.go` runs a single group (group 1) *through* the new transport. Every
  commit leaves a working system.
- **`group_id` as a field in each request**, not gRPC metadata (untyped,
  invisible, easy to forget) and not a port per group (9 ports, compose
  churn). Responses carry none: a reply returns on its own call.
- **Group 0 is reserved.** Proto3 reads an unset field as 0, so a sender
  that forgot to stamp the ID, or a pre-sharding binary, matches no group
  and fails loudly.
- **`raft.Node` stays unaware of groups.** One wrapper (`groupPeer`) stamps
  the ID on the way out; one dispatcher (`RaftServer`) routes on it on the
  way in. Cost: the wrapper writes into the request object Raft handed it,
  so the `Peer` interface now documents that each request is built for one
  call and never shared. Copying field by field was rejected: a field added
  to the proto later would be silently dropped.
- **One connection per peer node, shared by all groups**
  (`RaftPeerConn.ForGroup`). Connections grow with nodes, not nodes ×
  groups. `DialRaftPeer` returns a type with `Close`, so tests and shutdown
  don't leak gRPC goroutines (Rule 5); the pseudocode's bare stub had no way
  to close.

### How It Works — Plain English
Think of one building (a process) with several offices (groups) sharing one
mailroom (the port). Every outgoing letter is stamped with the sending
office's number, and the receiving mailroom delivers each letter only to the
office with that number. A letter with no number (group 0), or a number for
an office that isn't in the building, is returned to the sender; it is never
handed to whoever happens to be nearest. Between two buildings there is one
courier route (connection) shared by every office.

### Key Invariants
- INVARIANT (`raft_server.go`, `nodeFor`): a Raft message is handled only by
  the group it names. A group this process doesn't host, including group 0,
  is refused rather than handed to any group.
- INVARIANT (`raft_peer_client.go`): every outgoing Raft request names this
  peer's group; `raft.Node` never sets it.
- `RaftServer.nodes` is copied at construction and read-only afterwards, so
  handlers on many gRPC goroutines read it without a mutex (Rule 7).
- `Peer` contract (`node.go`): a request is built for a single call and
  never reused, which is what makes stamping it in place safe.

### The Lines That Matter Most

Line: `node, hosted := s.nodes[ring.GroupID(groupID)]` in `nodeFor`
Does: routes by the requested group.
Breaks if hard-coded to group 1 (B1, verified):
- Group 2's entry went to group 1 (`group 2 did not receive its own entry`).
- Groups 0 and 7 were *handled* (`error = <nil>, want NotFound`).
- Over real gRPC, group 2 never elected a leader: all its messages,
  including its own vote requests, landed in group 1's nodes.

The misrouting surfaced as an outage before it could surface as corruption.
In a subtler bug, say routing only some RPC types wrongly, corruption would
come first.

Line: `req.GroupId = uint64(p.group)` (in all three `groupPeer` methods)
Does: names the sending group on every request.
Breaks without it (B2, verified): `group IDs sent = [0 0 0]`, and over real
gRPC no group ever elected a leader. Every message arrived as group 0 and
was refused, which is exactly the loud failure that reserving 0 buys.
Why this way: Raft builds a fresh request per call (`election.go:108`,
`replication.go:281`, `snapshot.go:109`), so stamping in place costs nothing
and can't drop a field.

Line: `if _, hasZero := nodes[0]; hasZero { return error }` in `NewRaftServer`
Does: refuses to host a group that unset fields would match.
Breaks without it (B3, verified): only its construction test fails. In the
running system, a group 0 would quietly receive every unstamped or
pre-sharding message.

Line: `copied := make(...); for group, node := range nodes { copied[group] = node }`
Does: makes the routing table immune to the caller changing its map later.
Breaks without it (B4): **survived the first suite**, as predicted, because
no test changed the map after construction. A test now deletes and adds
groups in the caller's map, and routing must not change; then caught.

### What Breaks and Why
All four breaks were caught on the final code. B4 needed a new test; B1
failed differently than predicted (an outage rather than mixed values), for
the reason above.

### Verification
- The real-gRPC test: 3 processes on localhost, each hosting groups 1 and 2
  behind one port with shared connections. Each group elected its own leader
  and replicated only its own write (`x=one` in group 1, `x=two` in group 2).
  20/20 runs passed.
- A real-binary smoke run: three `node` processes on localhost elected a
  leader in term 1 through the stamped, dispatched transport, with no group
  errors. That verifies `main.go`'s wiring, which no unit test covers.

### Interview Questions and Answers
Deferred to the end of Milestone 9.

### Git Commit Message
`feat: carry group IDs on Raft RPCs and dispatch them to per-group nodes`

### Open Questions
- **A mixed-version cluster can't run.** Pre-9b nodes send group 0 and get
  refused. Fail-safe, but an upgrade means stopping every node at once.
- **Heartbeat traffic will multiply by the number of groups** in 9c, since
  each group heartbeats on its own. Production multi-Raft systems
  (CockroachDB) coalesce heartbeats per node pair.
- **Log lines don't say which group they come from yet.** Harmless with one
  group; 9c adds a group label before running several.

---

## Milestone 9c — Sharded KV Routing
Date: 2026-10-17

### The Problem This Solves
Once a process hosts several groups, each request must be served by the
group that owns its key. A write in the wrong group commits durably where no
correct read will look. A read in the wrong group answers a clean "not
found", which is a stale read that looks correct. And if the client and
server disagree about the ring, that has to surface as an error, not as data
in the wrong place.

### The Design Decision Made
- **Re-split again (Rule 2).** The planned 9c (config, layout guard, log
  labels, N groups in `main.go` *and* routing) was about 400 lines. Now:
  - 9c is routing only; `main.go` still runs one group, and a one-group ring
    sends every key to group 1.
  - 9d runs N groups per node.
  - 9e adds per-group leader guesses in the client, plus end to end.
- **The client sends `group_id`, and the server verifies it** against its own
  ring, refusing a mismatch with `FailedPrecondition` (not `Unavailable`,
  which means "try another node"). Group 0 is refused, as in 9b.
- **The KV server builds its ring from the groups it hosts**, so within a
  process the ring can't name a group without a local replica.
- **Checked before proposing, never at apply.** `Command` and the state
  machine are unchanged. Applying stays deterministic and independent of any
  replica's config.
- **Reads are checked too.** A wrong-group `Get` is the stale-read case.
- **Pseudocode change:** the client-level skew test would have taken minutes,
  because the client retries every error with backoff. It was split into a
  server-level skew test plus a client test that every attempt names
  `ring.Owner(key)`.

### Consistent Hashing Makes Ring Skew Fail Safe
A ring with one extra group agrees with the smaller ring on every key except
those it gives the extra group. So a client and server that disagree about
the group count disagree *only* about keys involving the group one of them
lacks. Every other key works normally. Measured over 60 keys against
3-group servers:
- **Client with an extra group 4:** 46 written, 14 refused. Every refusal
  was a key the client gave to group 4.
- **Client missing group 3:** 40 written, 20 refused. Every refusal was a key
  the servers own in group 3.

No key on which both sides have the group was ever refused or misrouted.
With modulo hashing, any count mismatch would disagree on most keys.

The server check is what turns both directions into errors. Without it, the
**missing-group** direction silently commits writes in groups 1 or 2, since
those groups exist everywhere.

### How It Works — Plain English
The client looks up the key on its copy of the ring and writes the group
number on the request, the same number on every retry. The server looks the
key up on *its* ring:
- **Numbers match:** it hands the request to its replica of that group, and
  everything from Milestones 7 and 8 happens inside that group as before.
- **They don't:** it refuses, before anything is written anywhere.

### Key Invariants
- INVARIANT (`kv_server.go`, `replicaFor`): an operation runs only in the
  group that owns its key, and the check happens BEFORE anything is
  proposed — never at apply time.
- The request's group must equal the server ring's owner; any ring
  disagreement becomes an error.
- `KVServer`'s ring and replica map are read-only after construction, and
  the ring is built from exactly the hosted groups.
- The client computes the group once per logical call and reuses it on every
  retry, exactly like the request ID.

### The Lines That Matter Most

Line: `if ring.GroupID(requestedGroup) != owner { return FailedPrecondition }`
Does: refuses any request whose group disagrees with this server's ring.
Breaks if the server trusts the client instead (M1, verified): a
missing-group-3 client wrote `skew-02` into group 2, though the server owns
it in group 3. A wrong-group read answered `found=false`. The extra-group-4
skew case still **passed**, masked because group 4 isn't hosted here. That is
why the skew test runs both directions.
Breaks if the server ignores the client instead (M2, verified): every
mismatch was accepted. The data landed correctly, because routing used the
server's ring, but the config mismatch went undetected. That is exactly what
lets a server-side mismatch silently misroute.

Line: `replica, err := s.replicaFor(req.GroupId, req.Key)` at the top of `Get`
Does: applies the same check to reads.
Breaks if `Get` alone trusts the client (M3, verified): only the read test
failed, with `answered found=false`, a silent stale read. Correction to my
pseudocode: a `Get` that *ignores* the client returns the right data; the
stale read comes from *trusting* it.

Line: `group := uint64(c.keyRing.Owner(key))`, computed before the retry
loop
Does: names the same group on every attempt.
Breaks if not sent (M4, verified): every client `Put` was refused as group 0
(`the request named group 0`), so five client tests failed. Each one burned
all 5 retry attempts, about 1.8s, on an error that can never succeed.

Line: `keyRing, err := ring.New(groups)` from the replica map's keys, in
`NewKVServer`
Does: makes the server's ring and its hosted groups the same set by
construction.
Why this way: a separately configured ring could name a group with no local
replica. Building it from the replicas removes that state rather than
checking for it.

### What Breaks and Why
All four breaks were caught. Two predictions were sharpened after writing
the tests, and both held:
- M1 passes one skew direction. Hosting acts as an accidental check, but
  only when the misnamed group doesn't exist.
- A `Get` that ignores the client is correct; one that trusts it is stale.

### Verification
- New tests:
  - keys land only in their owner (30 keys across 3 groups)
  - a wrong-group write leaves every group's log size unchanged, proving
    nothing was proposed
  - a wrong-group read is refused, not answered "not found"
  - construction refuses group 0
  - skew fails safe in both directions
  - the client names `ring.Owner(key)` on every attempt, including a forced
    retry
- All existing suites pass through single-group servers. Tests that call the
  server directly now name `GroupId: 1`.
- **Not separately verified:** `main.go`'s KV wiring change (one group, a
  few lines). It builds, but no binary-level run exercised it; 9e's end to
  end will.

### Interview Questions and Answers
Deferred to the end of Milestone 9.

### Git Commit Message
`feat: route KV requests to the key's shard group and refuse mismatches`

### Open Questions
- **The client retries `FailedPrecondition` like any error.** That is
  useful when *servers* disagree (a retry may reach one that agrees), but it
  wastes about 1.8s when the *client* is the misconfigured one. Failing fast
  on it is a candidate change for 9e.
- **A real shard move would need this check in the log.** If ownership could
  change, a request checked before proposing could commit after the move.
  MIT 6.5840 puts the config epoch *through* the log and checks it at apply
  time. Static config (4A) avoids this today.

---

## Milestone 9d — Each Node Runs SHARD_GROUPS Groups
Date: 2026-10-18

### The Problem This Solves
9b and 9c made a process *able* to host several groups; 9d makes every node
actually run `SHARD_GROUPS` of them, each with its own durable state. It also
enforces 9's choice 5A, "the group count is fixed at creation", instead of
merely documenting it. Changing the count is not a config change but a data
migration. Going from 3 to 4 groups sends about 23% of keys (measured in 9a)
to a new, empty group 4: reads for them return "not found" while their data
sits on disk in the old groups, and new writes create second copies.

### The Design Decision Made
- **`SHARD_GROUPS` is required, with no default.** A default silently applied
  on one node that forgot to set it is exactly the ring mismatch the system
  defends against.
- **The count is pinned in `DATA_DIR/shard-groups`** on first start, written
  atomically (temp, fsync, rename, fsync dir). Every later start must match,
  or the node refuses to boot, naming both counts.
  - Rejected: inferring the count from existing `group-N` directories. A
    crash partway through creating them is ambiguous.
  - Rejected: no guard.
- **A data directory from before sharding is refused.** Its single group's
  files sit at the root; treating it as fresh would make every old key look
  lost.
- **Layout:** `DATA_DIR/group-<g>/{raft-state.dat, raft-log.dat,
  snapshot.dat}`.
- **Log labels via `raft.WithLogger`**, an optional constructor argument. It
  is set before any goroutine exists, so there is no race, and the 9
  existing `NewNode` callers are unchanged. Only Raft's 6 lines are
  labelled; the state machine's 2 rare error lines wait for Milestone 10.
- **The whole process fails fast** if any group's state machine fails: a
  possibly diverged replica in *any* group must not keep serving.
- **Placement:** `PinShardGroups` lives in `config` as the one config value
  that is also persisted. It duplicates `raft`'s ~25-line atomic-write
  pattern rather than exporting it from the consensus package.

### How It Works — Plain English
On startup the node checks a small file in its data directory. If the file
doesn't exist, the directory is brand new: the node records the group count
and continues. If it exists and says the same count, it continues. If it
says something else, the node stops with a message saying what the
directory was created with.

Then, for each group 1..N, the node opens that group's own subdirectory,
builds a Raft node and state machine for it, and gives it its own view of
the shared connections to the other nodes. One gRPC port serves all groups:
9b's dispatcher routes Raft messages, and 9c's KV server routes client
requests. Every Raft log line carries `group_id`.

### Key Invariants
- INVARIANT (`main.go`): the group count is pinned BEFORE any group
  directory is created, so a crash between the two leaves no group data the
  pinned count doesn't describe. **Argued, not tested:** it is an ordering
  inside `main`, which no unit test reaches.
- `PinShardGroups`: a data directory only ever runs with the count it was
  created with; a pre-sharding directory or a corrupt marker is refused.
- `Node.logger` is set once in `NewNode` and never written again, so it is
  read without `mu`.
- Every group has its own directory (see break B3 for why).

### The Lines That Matter Most

Line: `if recorded != shardGroups { return error }` in `PinShardGroups`
Does: refuses to start with a different group count than the directory was
created with.
Breaks without it (B1, verified twice):
- The test fails (`restart with SHARD_GROUPS=4 on a 3-group data dir
  succeeded`).
- The **real binary** restarted happily with 4 groups and created an empty
  `group-4/` beside the others: the silent stranding of about 23% of keys,
  live.

Line: the `preShardingFiles` check before writing a fresh marker
Does: refuses a directory whose single group sits at the root.
Breaks without it (B2, verified): all three file cases were accepted as
fresh.

Line: `groupDir := filepath.Join(cfg.DataDir, fmt.Sprintf("group-%d", group))`
Does: gives each group its own log, state and snapshot files.
Breaks if every group uses `DATA_DIR` (B3, real binaries, verified):
- All three groups still elected leaders; nothing looked wrong while
  running.
- On restart: `log entry at offset 20 has index 1, want 2 (log is corrupt or
  non-contiguous)`. Offset 20 is right after the first 20-byte entry with an
  empty no-op command. The next entry was another group's no-op, also at
  index 1.
- Worse than the visible error: while running, three groups were replacing
  each other's `raft-state.dat`, so one group's term and vote could be read
  back as another's. That is a safety violation, and it is silent until
  restart.

Line: `raft.WithLogger(logger)` with `logger := slog.With("group_id", group)`
Does: labels each group's Raft log lines.
Verified: a test checks the label appears; the real-binary run showed
`group_id=1`, `2` and `3` on election lines.

### What Breaks and Why
All three breaks behaved as predicted. B1 and B3 were run as real mutant
binaries through a restart, which is the only place their damage shows.

### Verification — Real Binaries
Three `node` processes with `SHARD_GROUPS=3` on localhost:
1. Every group elected a leader in term 1.
   - group 1 → node 3
   - group 2 → node 2
   - group 3 → node 2

   **Leaders did not spread:** node 1 led nothing. That is the
   leader-placement gap, observed for real.
2. Node 1's data directory held `group-1/ group-2/ group-3/ shard-groups`,
   and the marker read `3`.
3. Restarted with `SHARD_GROUPS=4`: exited with code 1 and the pin error.
4. Restarted with `SHARD_GROUPS=3`: rejoined all three groups.
5. A directory with a root-level `raft-log.dat` exited with code 1: `holds
   raft-log.dat from before sharding`.

### Interview Questions and Answers
Deferred to the end of Milestone 9.

### Git Commit Message
`feat: run SHARD_GROUPS Raft groups per node with pinned per-group data dirs`

### Open Questions
- **Leader placement.** In the smoke run, one node led two groups and
  another led none, so throughput doesn't spread by itself. Fixes: leader
  transfer (Raft §3.10, TimeoutNow) plus a balancer.
- **Heartbeats multiply by the group count**, all over the same connection.
  Production multi-Raft systems batch them per node pair.
- **The state machine's 2 error lines are still unlabelled** (Milestone 10).
- **Upgrading from pre-sharding data means wiping it.** That is fine for
  this project; a real system needs a migration tool.

---

## Milestone 9e — A Client Built for Many Groups: Per-Group Leaders and a Classified Retry Policy
Date: 2026-10-18

### The Problem This Solves
Before 9e the client had **one** leader guess shared by all groups. With
leaders g1 → node 3 and g2, g3 → node 2 (the 9d smoke run), every switch
between groups started at the wrong node: 2 wasted calls and 300ms of
backoff per switch. The client also retried a ring mismatch
(`FailedPrecondition`) like any error, wasting about 1.8s on something
waiting can't fix. And it had no clear answer for a group that can never
elect a leader.

### The Design Decision Made
- **One leader guess per group** (`map[group]node`), updated only when a node
  actually serves the group. Rejected: leader hints in NotLeader replies
  (Raft doesn't track the leader's identity), and keeping one guess.
- **Every failure falls into exactly one class, and all classes share one
  attempt budget:**

| Class | Codes | Action |
|---|---|---|
| Caller is done | the caller's context ended (checked before classifying) | stop; wrap the context error |
| Routing refusal | `FailedPrecondition` | mark the node refused (never asked again in this call); no backoff; if every node refused → `ErrRingMismatch` |
| Leader discovery | `Unavailable`; `DeadlineExceeded` when only the attempt timed out | next eligible node, with backoff; the node stays eligible |
| Permanent | anything else | return at once |

- **Refusals are tracked as a per-call set of distinct nodes, not a count.**
  This came from review: my first pseudocode counted refusals and kept
  round-robining, which with nodes A and C refusing and B mid-election goes
  A, B, C, then **A again**. That hits 3 refusals and wrongly concludes the
  client's ring is wrong while B agrees.
- **NotLeader is not a refusal** because the server checks ownership
  *before* leadership. A NotLeader reply proves that node's ring agrees.
  That order is now an INVARIANT on the server, with a test.
- **The caller's deadline wins on every path**, including the refusal path,
  which never sleeps. If the caller's deadline expires *during* an attempt,
  that is reported as the caller's, never as "no leader reachable".
- **Errors you can check:** `ErrNoLeaderReachable` and `ErrRingMismatch`,
  each also wrapping the last gRPC error, so `status.Code` still works.
- **A `retryPolicy` struct** replaces the four constants. The defaults are
  unchanged; tests pass a fast policy. The worst case with no caller
  deadline is 5 × 2s + 1.5s ≈ 11.5s, then `ErrNoLeaderReachable`.
- **Rule 2 split:** the end-to-end integration test moved to 9f.

### How It Works — Plain English
For each key the client works out its group and starts at the node that
last served that group. If a node says "wrong group", the client crosses it
off for this call and tries the next immediately, because waiting won't
change a config disagreement. If *every* node crosses off, the client's own
config is the problem. If a node says "not leader", or doesn't answer, the
client tries the next node after a short, growing pause, giving an election
time to finish. Every one of these attempts comes out of the same budget of
5, and the caller's own deadline cuts the whole thing short at any point.

### Key Invariants
- INVARIANT (`client.go`, `retry`): ONE attempt budget covers every failure
  class, so a call always ends, even against a group that can never elect
  a leader.
- INVARIANT (`client.go`, unchanged and extended): every attempt of one call
  carries the same request ID **and** the same group; any error for a write
  means "outcome unknown".
- INVARIANT (`kv_server.go`, `replicaFor`): ownership is checked before
  leadership, so NotLeader implies the node's ring agrees.
- A refused node is never asked again within one call; the guess only ever
  points to a node that served the group.

### The Lines That Matter Most

Line: `refused[node] = true` plus `if len(refused) == len(c.stubs)`
Does: counts distinct refusing nodes and ends the call once all have refused.
Breaks as a plain counter (M1, verified): the NotLeader test ended in
`ErrRingMismatch` while B agreed, and A was asked again (`[A B C A B]`).

Line: `case codes.Unavailable, codes.DeadlineExceeded:` (NotLeader is
discovery, not refusal)
Does: keeps a NotLeader node eligible and backs off.
Breaks if NotLeader counts as a refusal (M2, verified): six tests failed.
The two that hit real servers show the real cost:
- One transient `Unavailable` made the client declare a ring mismatch.
- With nothing listening, the client gave up after **1.3ms** instead of
  backing off for 1.5s.

Line: `if !refused[candidate]` in `nextEligible`
Does: skips nodes that refused in this call.
Breaks without it (M3, verified): `calls = [A B C A B], want [A B C B]`.

Line: `if ctx.Err() != nil { return callerStopped(...) }` after a failed
attempt
Does: attributes a failure caused by the caller's own deadline to the
caller.
Breaks without it (M7, verified as predicted): only the last-attempt test
failed (`no leader reachable ... after 1 attempts`). On every other path the
top-of-loop check or the backoff sleep notices the expired context anyway.
The check exists so the final attempt doesn't blame the cluster for the
caller's deadline.

Line: `c.leaderGuess[group] = node`
Does: remembers each group's leader separately.
Breaks with one shared guess (M8, verified): **30 wasted attempts in 30
alternating calls**, exactly one wrong first guess per group switch.

### What Breaks and Why
All nine breaks were caught. The ones not covered above:

| Break | Result |
|---|---|
| M4: leadership checked before ownership | the follower test got `Unavailable`, not `FailedPrecondition` |
| M5: permanent errors retried | the `InvalidArgument` call "succeeded" on the next node |
| M6: no caller-context check at the top of the loop | an already-cancelled `Put` made a call **and succeeded**, because the fake ignores its context. Against a real server, gRPC refuses a cancelled context itself, which partly masks this in production; only the explicit check is verifiable here |
| M9: backoff on refusals | the fail-fast tests took 602ms and 201ms |

Also fixed: error messages stuttered (`client: group 1: client: every node
refused`). They now read `client: every node refused the key's group; ...
(group 1): rpc error: code = FailedPrecondition ...`.

### Verification
- All 20 client tests pass: the 9 existing ones untouched by the policy
  change, and 11 new. Timing-sensitive tests were repeated 20× under
  `-race`, with no failures.
- The server test pins the check order on a real follower: wrong group →
  `FailedPrecondition`; right group → `Unavailable`.

### Interview Questions and Answers
Deferred to the end of Milestone 9.

### Git Commit Message
`feat: per-group leader guesses and a bounded, classified client retry policy`

### Open Questions
- **The guess goes stale after a leader change** and costs one discovery
  round per group. Leader hints would fix that, but need Raft to track the
  leader's identity.
- **Backoff has no jitter.** Many clients failing over together retry in
  lockstep. Production clients randomize backoff.
- **The policy is fixed at construction.** Real clients often expose it as
  an option.

---

## Milestone 9f — Extracting StartNode So Tests Run the Wiring That Ships
Date: 2026-10-18

### The Problem This Solves
Entries 9c and 9d both noted that `main.go`'s wiring was not covered by any
test: the pin call, per-group directories, the shutdown order. A
`package main` can't be imported, so an end-to-end test could only test a
*copy* of that wiring, which can drift from what ships. 9f moves the wiring
into `server.StartNode`, which `main` and the tests both call.

### The Design Decision Made
Options for the end-to-end test:
- **A. The test copies the wiring:** no production change, but it tests a
  copy.
- **B. Extract `StartNode`:** tests what ships. *Chosen.*
- **C. Exec the real binary:** the most faithful, but slow, and it can't stop
  a single group, because groups live and die with their process.

Rule 2 splits B into 9f (the extraction) and 9g (the integration tests).

What the extraction required:
- **No `os.Exit` in a library.** A failed state machine is reported on
  `Fatal()`; `main` still exits at once (fail-stop, unchanged).
- **Cleanup on a failed start.** `main` relied on the process exiting;
  `StartNode` must close everything it opened, including the listener it
  owns from the moment it is called.
- **State machines start only after every group has opened.** In `main` each
  one started inside the per-group setup. A failure partway would leave the
  earlier groups' state machines blocked forever on channels nothing would
  close.
- **`raft.Node.Stop` became safe to call twice** (`sync.Once`). A test stops
  one group's replica on its own, and then the node shuts down. Its comment
  previously warned that a second call panics.
- **`StartNode` takes a listener**, so tests can open `:0` ports first and
  learn their addresses before building each node's peer list.

### How It Works — Plain English
`main` is now four steps: read the config, listen on the port, call
`StartNode`, then wait for a shutdown signal or a fatal error. `StartNode`
does what `main` used to do, in the same order: pin the group count, dial
the peers, open every group's files, build every group's Raft node and state
machine, serve gRPC, and start everything. If anything fails partway, it
undoes what it did and returns the error, instead of exiting.

### Key Invariants
- INVARIANT (`node.go`, moved from `main`): the group count is pinned
  BEFORE any group directory is created. **Now tested:** a refused start
  must create no `group-4/`.
- `StartNode` owns the listener: it is served on success and closed on
  failure.
- A failed start leaves nothing behind: no open files, no goroutines, no
  bound port.
- Every group's state machine starts only after every group has opened.
- `raft.Node.Stop` and `NodeRuntime.Stop` are both safe to call twice.

### The Lines That Matter Most

Line: `func StartNode(...) (_ *NodeRuntime, err error)` with
`nodeRuntime := &NodeRuntime{...}`
Does: lets the deferred cleanup see `err` while building the node in a
local variable.
Broke for real on the first run: the first draft named the result
`runtime`, and every `return nil, err` assigned `nil` to it **before** the
deferred cleanup ran. The cleanup then called `shutdown()` on a nil pointer
(`nil pointer dereference ... (*NodeRuntime).shutdown(0x0)`). It was caught
by the first test that exercised a failed start. This is Go's named-result
trap: deferred functions see the results *after* the return statement has
assigned them.

Line: the `defer` that calls `shutdown()` and `listener.Close()` on failure
Does: releases everything a failed start opened.
Breaks without it (M2, verified): `open files: 8 before (with the listener),
9 after; want 7`. Group 1's log and the listener were both leaked.

Line: starting each state machine in the loop *after* every group has
opened
Does: ensures a failed start leaves no goroutine blocked forever.
Breaks with `main`'s old order (M3, verified): `goroutines: 2 before, 3
after`. Exactly one leak: group 1's apply loop. (The first M3 mutant didn't
compile; the compile check caught it before its result could mislead.)

Line: `config.PinShardGroups(...)` before the group loop
Does: pins the count before any group directory exists.
Breaks if moved after (M4, verified): `a refused start created group-4/`. In
9d this ordering lived in `main` and was "argued, not tested". Extraction
made it testable.

Line: `n.stopOnce.Do(func() { close(n.stopCh) })` in `raft.Node.Stop`
Does: makes a second `Stop` harmless.
Breaks without it (M6, verified): `panic: close of closed channel` in both
double-`Stop` tests.

### What Breaks and Why
- M1 (no pin call): caught by both wiring tests.
- M7 (`NodeRuntime.Stop` without its own `stopOnce`) **survived**, as
  predicted. With `raft.Node.Stop` idempotent, a second shutdown only logs
  "already closed" errors, so `stopOnce` is defensive, not load-bearing.
- The failed-start test first **skipped** on macOS, because `os.ReadDir`
  stats each `/dev/fd` entry and that fails. A skip is not a pass, so it now
  counts descriptors by name only and runs for real (5/5).

### Verification — Real Binaries
The 9d smoke script, unchanged, against the refactored `main`:
- every group elected a leader
- the layout was `group-1/ group-2/ group-3/ shard-groups`
- `SHARD_GROUPS=4` exited with code 1
- a pre-sharding directory exited with code 1

Its rejoin check showed only groups 2 and 3. That turned out to be the
script stopping at the first log line: a follower whose term doesn't change
never logs. Silence isn't proof, so a functional check followed:
1. Node 2 led **all three** groups (leader placement again).
2. Node 1 was restarted from its data directory, then node 2 was killed.
3. All three groups re-elected **node 1** as leader in term 2, with 2 votes
   each, which is impossible unless node 1 rejoined every group.

### Interview Questions and Answers
Deferred to the end of Milestone 9.

### Git Commit Message
`refactor: extract node wiring into server.StartNode with clean failure and idempotent stop`

### Open Questions
- **Leader placement**, now observed three times: one node led every group
  in this run. Leader transfer would be needed to balance it.
- **`Fatal()` delivers one error per failed group**, but `main` acts on the
  first. That is enough for fail-stop; a supervisor might want all of them.

---

## Milestone 9g — End-to-End Tests, and the Client Bug They Found
Date: 2026-10-18

### The Problem This Solves
Every earlier test exercised one layer with the others faked. 9g runs the
whole system:
- three nodes built by `server.StartNode`, the same wiring `main` runs;
- three shard groups each;
- real gRPC on localhost, driven by the real client.

It asks the questions only the whole system can answer. Do keys stay in
their group across real replicas? Does a node that dies and returns catch up
in every group? Does InstallSnapshot work over the real network, per group?
Is one group's failure contained?

### The Design Decision Made
- **The tests use `StartNode`** (9f), so they test what ships.
- **Failing a single group needs a network cut, not just `Stop`.**
  - A stopped `raft.Node` still answers RPCs; only its timers and applier
    stop. So "stopped" replicas kept acknowledging AppendEntries, and the
    group stayed available.
  - `StartNode` now takes optional gRPC server options. The tests pass an
    interceptor that refuses every request naming a cut group, using the
    `GetGroupId()` every request type has.
  - A dead replica is Stop **plus** cut.
- **An honest framing.** With every node hosting every group, a real node
  failure hits all groups at once. A single-group failure (say, one group's
  disk) is the rarer case, so the test proves the groups are independent,
  not a common failure mode.

### The Bug the End-to-End Tests Found, and Its Fix
The node-failure test failed in **2 of 5** full-package runs:
`no leader reachable ... (group 3, 5 attempts): not leader`.

The election log showed group 3 *did* have a leader. A diagnostic copy with
millisecond timestamps showed why the client missed it:

| Time | Event |
|---|---|
| .889 | attempt 1 → node 1: Unavailable (just stopped) |
| .990 | attempt 2 → node 2: not leader |
| 03.160 / .174 | nodes 2 and 3 both start elections for term 2: split vote |
| 03.192 | attempt 3 → node 3: not leader (candidate) |
| **03.438** | **node 3 elected leader, term 3** |
| 03.592 | attempt 4 → node 1: Unavailable (still dead) |
| 04.394 | attempt 5 → node 2: not leader → **ErrNoLeaderReachable** |

**Root cause.** 9e's budget counted *attempts*. Spent round-robin with
growing gaps, that visits each node once or twice, early. After a split vote
the new leader's turn came before it won, and never came again. The fake
nodes in the unit tests never have elections in progress, so only the
integration test could find this.

**The fix: option C, a time budget.** Chosen with the user over three
alternatives:
- A: make the tests wait for leaders. Rejected: it hides a real bug.
- B: more attempts. Still counts attempts.
- D: leader hints. The right long-term answer, but spans three layers.

The new budget is a total duration: 5s by default, or the caller's deadline
if sooner. Backoff is short and capped (25ms doubling to 250ms), so with 3
nodes every node is asked at least every ~750ms for the whole budget.
Everything else from 9e is unchanged.

Making it robust:
- `budgetCtx` is derived from the caller's context. "Caller's context done"
  means the caller stopped, reported as theirs. "Only `budgetCtx` done"
  means our budget ran out: `ErrNoLeaderReachable`.
- Backoff is doubled step by step instead of `base << attempt`, which would
  overflow now that a call can make dozens of attempts.
- **Red before green.** The new regression test (a leader elected 150ms
  into the call) failed on the old policy with `calls [A B C A B]` and
  passes on the new one, finding the leader at about 210ms.

**Proof:** 10 full-package runs passed every test, 40 of 40, versus 2
failing runs in 5 before.

### How It Works — Plain English
The client now thinks in time, not tries: "keep asking around for up to 5
seconds, pausing at most a quarter second between tries." An election that
needs a few rounds finishes well inside that, and because the pauses stay
short, the client keeps coming back to every node, including the one that
just won.

### The Four End-to-End Tests and What They Showed
1. **Keys spread and stay in their group:** 60 keys through the client, each
   on all 3 replicas of its owning group and on no other group's replicas.
2. **A whole node dies and returns:**
   - Every key stays readable and writable with node 1 down.
   - Restarted from its data directory on the same address, node 1 caught
     up in every group in about 1s.
3. **Catch-up through snapshots, over real gRPC, per group:**
   - Setup proved both running replicas had compacted every group past
     node 1's last entry, so ordinary log entries *could not* catch it up.
   - Node 1 caught up in every group (0.8–2.4s), and its own snapshot moved
     past its old last index.
   - This is the first time InstallSnapshot has run over the real network
     with group IDs.
4. **One group loses its majority:**
   - Groups 1 and 3 kept serving.
   - Asked directly, group 2's isolated leader **rejected a read in 2ms**,
     because ReadIndex can't confirm a majority.
   - It **held a write for the full 3s** until the caller gave up: with no
     CheckQuorum, it never learns it has lost its followers.
   - Through the client, both ended with `ErrNoLeaderReachable` within the
     budget.

### Key Invariants
- INVARIANT (`client.go`, revised): ONE **time** budget covers every
  failure class. The loop can't spin: each node refuses at most once per
  call, every other failure sleeps at least `baseBackoff`, and every attempt
  and sleep is bounded by the budget.
- The caller's deadline is reported as the caller's; our budget is reported
  as `ErrNoLeaderReachable`. Never the other way round.
- (Test harness) A dead replica is Stop **and** cut; a stopped node alone
  still answers RPCs.

### The Lines That Matter Most

Line: `budgetCtx, cancelBudget := context.WithTimeout(ctx, c.policy.totalBudget)`
Does: makes the limit a duration, still cut short by the caller's deadline.
Why this way: the attempt count was the bug; deriving from `ctx` is what
lets the two endings be told apart.

Line: `maxBackoff: 250 * time.Millisecond`
Does: caps the gap between tries, so every node is revisited throughout the
budget.
Why this way: the cap, not the budget size, is what fixes the bug. The old
2s cap and doubling from 100ms left a 1.5s-wide hole after the first
rotation.

Line: `if budgetCtx.Err() != nil { break }` after an attempt, checked
*after* `ctx.Err()`
Does: reports our own budget expiring as `ErrNoLeaderReachable`.
Verified by `TestClient_OurBudgetMidAttempt_IsReportedAsNoLeaderReachable`
(and its mirror for the caller's deadline).

### What Breaks and Why — 9g Mutants
| Break | Result |
|---|---|
| B1: all groups share one directory | both restart tests failed: `log entry at offset 20 has index 1`. 9d's break B3, only checkable by hand until now, is an automated failure |
| B2: `groupPeer.InstallSnapshot` doesn't stamp its group | only the snapshot test failed (`node 1, group 3` never caught up); snapshots arrived as group 0 and were refused |
| B3: the leader never sends a snapshot | only the snapshot test failed, the same way |

### Interview Questions and Answers
Below, in the Milestone 9 interview.

### Git Commit Message
`test: end-to-end sharded cluster tests; client retries on a time budget`

### Open Questions
- **CheckQuorum.** An isolated leader holds writes until the caller gives
  up, observed directly. The fix is for a leader that hasn't heard from a
  majority within an election timeout to step down.
- **Leader hints (option D).** They would turn "search the nodes" into "go
  straight to the leader".
- **Backoff jitter.** Many clients failing over together still retry in
  lockstep.
- **Docker.** The real-container run for Milestones 7–9 is still deferred.

---

<!-- All future entries go below this line, in chronological order -->
