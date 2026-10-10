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

<!-- All future entries go below this line, in chronological order -->
