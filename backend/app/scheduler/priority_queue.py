"""
PriorityQueue — heap-based scheduling core.

DESIGN
------
Python's `heapq` is a min-heap of tuples. We push:

    (run_at_epoch, priority, sequence, task_id)

Why this composite key:
  1. `run_at_epoch` first: tasks scheduled for the future stay buried until time arrives.
     The scheduler peeks the root and skips dispatch if `run_at_epoch > now`.
  2. `priority` second: among ready (run_at <= now) tasks, lower number = higher priority.
  3. `sequence` third: monotonic int from `itertools.count()`. Two purposes:
       (a) deterministic FIFO tiebreak for tasks with identical (time, priority).
       (b) prevents Python from comparing the next element (task_id str), which would
           still work but adds nondeterminism.
  4. `task_id` last: just a payload pointer — never participates in ordering because
     `sequence` is unique per push.

Complexity:
  push:   O(log n)
  pop:    O(log n)
  peek:   O(1)        — used every tick to decide if anything is due
  remove: O(n)        — `_invalidate` marks lazily; we filter on pop instead

CANCELLATION / RE-PRIORITIZATION
--------------------------------
Heaps don't support O(log n) arbitrary removal without an index. We use the
"lazy deletion" trick: keep a SET of cancelled task IDs and skip them on pop.
For re-prioritization, push a new entry and add the old sequence to invalidated.
Amortized cost stays O(log n) as long as cancellations are infrequent.
"""
from __future__ import annotations
import heapq
import itertools
import threading
import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass(order=True)
class _HeapEntry:
    run_at_epoch: float
    priority: int
    sequence: int
    task_id: str = field(compare=False)


class PriorityQueue:
    """Thread-safe min-heap priority queue with delayed-execution support."""

    def __init__(self) -> None:
        self._heap: list[_HeapEntry] = []
        self._counter = itertools.count()       # monotonic sequence generator
        self._invalidated: set[int] = set()     # sequences to skip (lazy delete)
        self._lock = threading.RLock()          # heapq is not thread-safe

    # -------- Core operations --------

    def push(self, task_id: str, priority: int, run_at_epoch: Optional[float] = None) -> int:
        """
        Schedule a task. Returns the sequence number (use to cancel later).
        Time: O(log n)
        """
        if run_at_epoch is None:
            run_at_epoch = time.time()
        seq = next(self._counter)
        entry = _HeapEntry(run_at_epoch, priority, seq, task_id)
        with self._lock:
            heapq.heappush(self._heap, entry)
        return seq

    def pop_ready(self) -> Optional[str]:
        """
        Pop the next *ready* task (run_at <= now). Returns task_id or None.
        Skips invalidated entries lazily.
        Time: O(log n) amortized; O(k log n) if k cancellations sit at the root.
        """
        now = time.time()
        with self._lock:
            while self._heap:
                top = self._heap[0]
                # Future task — leave it; nothing else is due either.
                if top.run_at_epoch > now:
                    return None
                # Cancelled — discard and continue.
                if top.sequence in self._invalidated:
                    heapq.heappop(self._heap)
                    self._invalidated.discard(top.sequence)
                    continue
                heapq.heappop(self._heap)
                return top.task_id
        return None

    def peek_next_run_time(self) -> Optional[float]:
        """
        Returns the run_at of the soonest task without removing it. O(1).
        Used by the scheduler loop to sleep precisely until the next event.
        """
        with self._lock:
            return self._heap[0].run_at_epoch if self._heap else None

    def cancel(self, sequence: int) -> None:
        """Mark a sequence as cancelled. Actual removal happens on pop. O(1)."""
        with self._lock:
            self._invalidated.add(sequence)

    def __len__(self) -> int:
        with self._lock:
            return len(self._heap) - len(self._invalidated)

    def stats(self) -> dict:
        """For metrics/monitoring."""
        with self._lock:
            return {
                "heap_size": len(self._heap),
                "invalidated": len(self._invalidated),
                "next_run_in_seconds": (
                    max(0.0, self._heap[0].run_at_epoch - time.time())
                    if self._heap else None
                ),
            }
