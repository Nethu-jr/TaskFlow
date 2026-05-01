# ITSS — Intelligent Task Scheduler System

Production-grade distributed task scheduler combining ideas from Celery (distributed
execution) and Airflow (DAG-style scheduling) with ML-powered priority prediction.

---

## Quick start

```bash
# 1. Train the ML priority model (one-time)
cd ml/training && python train.py

# 2. Boot the whole stack
docker compose up --build

# 3. Open http://localhost:8080 (frontend) — backend at :8000
```

Scale workers any time: `docker compose up --scale worker=10`

---

## Folder layout

```
itss/
├── backend/app/
│   ├── api/           REST + WebSocket
│   ├── core/          config, logging, redis client
│   ├── scheduler/     priority queue + dispatch loop
│   ├── workers/       worker process + handler registry
│   ├── ml/            priority predictor
│   ├── models/        Pydantic schemas
│   └── db/            task store
├── frontend/src/      React dashboard
├── ml/                training pipeline + model artifacts
└── docker/            Dockerfiles + nginx config
```

---

## Architecture

```
[React]  ──REST/WS──▶  [FastAPI]  ──schedule──▶  [Heap PQ]
                          │                          │
                          ├─▶ [ML predictor]         ▼
                          │                       [Redis list]  (worker queue)
                          │                          │
                          ▼                          ▼
                       [Redis hash] ◀────results── [Workers ×N]
                          │
                          └─▶ [Pub/sub] ─▶ frontend (live updates)
```

Data flow:
1. `POST /tasks` → backend
2. ML model assigns priority if not provided
3. Task persisted (hash map), pushed to scheduler heap with key `(run_at, priority, seq)`
4. Scheduler tick (every 500ms) pops everything ready, LPUSHes to Redis worker queue
5. Worker `BLPOP`s, executes handler, updates state, publishes update
6. WebSocket broadcasts update to connected dashboards
7. On failure: worker pushes to retry queue → scheduler re-pushes with exponential backoff

---

## Data structures (the algorithmic heart)

| Need | Choice | Why |
|------|--------|-----|
| Pick highest-priority task | **Min-heap** | O(log n) insert/pop, O(1) peek |
| Look up task by ID | **Hash map (Redis)** | O(1) average |
| Worker queue (FIFO dispatch) | **Redis list (LPUSH/BLPOP)** | Atomic, blocking pop |
| Deduplication of running tasks | **Redis SET** | O(1) membership check |
| Retry queue | **Redis list** | Decouples worker from scheduler |
| Live updates | **Redis Pub/Sub** | Fan-out without polling |
| Cron-like recurring tasks | (optional) **Sorted set** with next-fire timestamp |

### Why a min-heap, not a FIFO queue?

A queue gives O(1) but no priority. A sorted list gives O(1) pop but O(n) insert.
A heap gives O(log n) for both.

For 1M tasks: log₂(1,000,000) ≈ 20 comparisons — negligible.

### Composite key trick

Heap key: `(run_at_epoch, priority, sequence)`
- Time first → delayed tasks stay buried
- Priority second → urgent tasks first among ready set
- Sequence (monotonic int) → FIFO tiebreak + prevents Python from comparing payload objects

### Lazy cancellation

Heaps don't support O(log n) arbitrary removal. We mark cancelled
sequences in a `set` and skip them on pop. Amortized cost stays O(log n).

---

## Fault tolerance

| Failure | Mitigation |
|---------|------------|
| Worker crashes mid-task | Heartbeat keys with TTL=30s; supervisor reaps stale tasks from running set |
| Task throws exception | Push to retry queue; scheduler re-pushes with exponential backoff (`2 * 2^(retries-1)` capped at 600s) |
| Backend restart | Hash map is in Redis, not in-memory — heap rebuilt by replaying `pending` tasks at startup |
| Duplicate dispatch | `SADD` to running set during dispatch; workers verify before executing |
| Redis down | Backend returns 503; tasks queue up in client retry logic. Use Redis Sentinel/Cluster for HA |

### Exponential backoff progression
```
retry 1 → 2s   retry 2 → 4s   retry 3 → 8s   retry 4 → 16s   retry 5 → 32s
... capped at MAX_BACKOFF_SECONDS (600s).
```

---

## Scaling to 1M+ tasks/day

1M tasks/day = ~12 tasks/sec average (with bursts to ~100/sec).

**Bottlenecks (in expected order):**

1. **Scheduler heap** — single-process. At 100 ops/sec it does ~700 heap operations per
   second (push + pop). At log n=20, that's negligible CPU. Scales to ~10k tasks/sec
   on one core. Beyond that: shard by tenant or hash partition the heap.
2. **Redis queue** — handles >100k ops/sec on commodity hardware. Use Redis Cluster
   for sharding when needed.
3. **Workers** — embarrassingly parallel. Add containers until throughput satisfies
   demand. Auto-scale on queue depth: `if redis_queue_depth > 1000 for 1min: scale_up`.
4. **Database (task store)** — at 12 writes/sec it's not a problem, but for audit
   log replay you'll want partitioning by date.

**Distributed scheduler challenges (when one isn't enough):**
- *Coordination*: multiple schedulers shouldn't dispatch the same task. Solve with
  Redis distributed lock (`SET NX EX`) per task ID, or with leader election (Raft, ZooKeeper).
- *Heap state*: each scheduler keeps a partition (e.g., by hash(task_id) % N).
  Tasks routed to the right scheduler at submission time.
- *Failover*: if a scheduler dies, another takes over its partition. Use Kubernetes
  StatefulSets + persistent leases.

---

## ML priority predictor

- Model: `GradientBoostingRegressor` (sklearn) — non-linear, no feature scaling needed
- Features: task type one-hot, payload size (KB), hour of day, day of week, urgency keywords
- Output: continuous priority 1..10, clipped to int
- Training: synthetic data initially; switch to actual completed-task logs after 30 days
- Inference: ~1ms per prediction; called only when client omits priority
- Fallback: rule-based heuristic if model file missing

Retrain weekly: schedule the training job as an ITSS task itself (`task_type: "ml_inference"`).

---

## API

```
POST   /tasks              Create + schedule
GET    /tasks              List
GET    /tasks/{id}         Get one
POST   /tasks/{id}/run     Force-dispatch now
POST   /tasks/{id}/retry   Manual retry
GET    /scheduler/stats    Heap depth, queue depth, worker count
WS     /ws/tasks           Live updates

POST   /crons              Create recurring task template
GET    /crons              List
GET    /crons/{id}         Get one
PATCH  /crons/{id}         Enable/disable or change priority
DELETE /crons/{id}         Remove

GET    /history/completed?since_hours=24       Recent completed/failed
GET    /history/tasks/{id}/events              Full state-transition timeline
GET    /history/metrics?since_hours=24         Aggregated success rate, avg duration
```

Example task:
```json
POST /tasks
{
  "name": "send_welcome_email",
  "task_type": "email",
  "payload": {"to": "user@example.com"},
  "max_retries": 3
}
```

Example cron — every weekday at 9am:
```json
POST /crons
{
  "name": "daily_report",
  "cron_expr": "0 9 * * 1-5",
  "task_type": "report",
  "payload": {"format": "pdf"}
}
```

---

## Cron-style recurring tasks

Cron schedules are *templates* — when they fire, they spawn a fresh `Task`
instance that flows through the normal scheduler heap.

**Storage:**
  - `itss:cron:{id}` (HASH) — the template
  - `itss:cron_schedule` (ZSET) — score=`next_fire_epoch`, member=`cron_id`
  - `itss:cron_fire:{id}:{slot}` (string with NX TTL) — fencing lock

**Distributed correctness.** When N scheduler replicas all see the same fire
slot, they race for `SET NX EX itss:cron_fire:{id}:{epoch}`. Only one wins
and spawns the instance. The lock TTL is 2× the tick interval — long enough
to outlive a GC pause, short enough to self-clear.

**Catch-up behavior.** After a long pause (scheduler offline, scaled to 0),
the ticker rebases the next fire on the *scheduled* fire epoch, not `now()`.
This means missed slots fire in rapid succession. If you'd rather drop
missed slots (high-frequency crons), change `after_epoch=scheduled_fire` to
`after_epoch=time.time()` in `_fire()`.

**Time complexity:**
  - Find due crons: O(log n + k) via `ZRANGEBYSCORE`
  - Re-arm: O(log n) via `ZADD`
  - Scales to ~100k schedules with no tuning.

---

## Audit history (PostgreSQL)

Redis is the source of truth for **live state** (sub-ms reads). Postgres is
the source of truth for **history** (immutable audit log).

**Two append-only tables:**

  - `task_history` — one row per task, written when it reaches a terminal
    state. Used by ML retraining and the dashboard's "History" tab.
  - `task_events` — one row per state transition (scheduled, queued,
    started, completed, failed, retrying). Used for debugging and replay.

**Outbox pattern for durability.** Audit writes do NOT block the worker hot
path. If Postgres is unreachable, events buffer in Redis (`itss:audit_buffer`)
and a background drainer flushes them once Postgres recovers. The system
keeps running through database outages — only audit history is delayed.

**Why dual-write instead of CDC?** CDC (e.g., Debezium streaming Redis to
Postgres) has higher operational complexity. The outbox pattern is simpler,
already gives at-least-once delivery, and the only failure mode (lost event
during simultaneous Redis + Postgres outage) is acceptable for an audit log.

---

## ML retraining loop (closing the feedback loop)

`ml/training/train_from_db.py` is the production retraining script:

  1. Pulls completed tasks from `task_history` (last 90 days).
  2. Featurizes them with the **same** `featurize()` function used at
     inference time. *Sharing this function is the single most important
     practice in production ML* — feature drift is the #1 cause of broken
     pipelines.
  3. Below 1000 real samples, augments with synthetic data so cold-start
     models still learn something useful.
  4. Trains, evaluates (MAE + R²), saves with timestamped filename.
  5. Atomically swaps the symlink — old model stays loaded if anything crashes
     mid-write.

Schedule it as an ITSS cron (eat your own dog food):

```json
POST /crons
{
  "name": "weekly_ml_retrain",
  "cron_expr": "0 3 * * 0",
  "task_type": "ml_inference",
  "payload": {"action": "retrain"}
}
```

Then add a handler that shells out to the script.

---

## Comparison: Priority Queue vs FIFO

| Aspect | FIFO | Priority Queue |
|--------|------|----------------|
| Insert | O(1) | O(log n) |
| Pop | O(1) | O(log n) |
| Honors priority | No | Yes |
| Honors delay | No | Yes (with composite key) |
| Use when | All tasks equal | Any prioritization needed |

For ITSS, FIFO would mean an `urgent` task waits behind 10,000 batch reports.
Priority queue moves it to the front in microseconds.

---

## Real-world comparison

| Feature | Celery | Airflow | **ITSS** |
|---------|--------|---------|----------|
| Distributed execution | ✅ | ✅ | ✅ |
| Priority scheduling | weak | weak | ✅ heap-based |
| Delayed tasks | via ETA | scheduled DAGs | ✅ |
| Retries with backoff | ✅ | ✅ | ✅ exponential |
| ML priority prediction | ❌ | ❌ | ✅ |
| Dependency graphs (DAGs) | weak | ✅ | future work |
| Live UI updates | flower | static refresh | ✅ WebSocket |

ITSS is *simpler* than either — ~1500 LOC of core logic — but covers the most
common production workload patterns.

---

## System design interview talking points

- **Why heap?** Need both priority and delay. O(log n) is good enough for 10k tasks/sec.
- **Why Redis for the worker queue, not the heap itself?** Redis lists give blocking
  pops (`BLPOP`) which lets workers idle without polling. The heap stays in-process
  for fast peek/pop without network round-trips.
- **What if Redis goes down?** Run Redis Sentinel (HA) or Redis Cluster (sharded).
  Scheduler degrades gracefully — accepts tasks, queues them in memory, replays once
  Redis returns.
- **What if a worker hangs?** Heartbeat key expires; supervisor process scans the
  running set, identifies orphaned tasks, requeues them.
- **Idempotency?** Task handlers must be idempotent — duplicate dispatch is rare but
  possible during failure modes. Use task_id as deduplication key in downstream systems.

---

## Tests

```bash
cd backend
python tests/test_scheduler.py     # heap, delay, cancellation, FIFO, backoff
python tests/test_cron.py          # cron expressions, validation, catch-up
```

10 tests cover the core algorithmic surface — runs in <1 second, no Redis or
Postgres required.
