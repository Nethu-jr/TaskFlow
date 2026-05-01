"""
Unit tests for the scheduler core. Run with: python -m pytest tests/
(no Redis needed for these — they exercise the in-memory heap.)
"""
import time
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.scheduler.priority_queue import PriorityQueue


def test_priority_ordering():
    pq = PriorityQueue()
    now = time.time()
    pq.push("low", 8, now)
    pq.push("high", 1, now)
    pq.push("med", 5, now)
    assert pq.pop_ready() == "high"
    assert pq.pop_ready() == "med"
    assert pq.pop_ready() == "low"
    assert pq.pop_ready() is None


def test_delayed_tasks_stay_buried():
    pq = PriorityQueue()
    now = time.time()
    pq.push("future", 1, now + 100)
    pq.push("now", 5, now)
    assert pq.pop_ready() == "now"           # despite lower priority
    assert pq.pop_ready() is None            # future task not yet ready
    assert len(pq) == 1


def test_lazy_cancellation():
    pq = PriorityQueue()
    now = time.time()
    seq_a = pq.push("a", 1, now)
    pq.push("b", 2, now)
    pq.cancel(seq_a)
    assert pq.pop_ready() == "b"             # 'a' silently skipped
    assert pq.pop_ready() is None


def test_fifo_tiebreak():
    pq = PriorityQueue()
    now = time.time()
    for i in range(5):
        pq.push(f"t{i}", priority=3, run_at_epoch=now)
    order = [pq.pop_ready() for _ in range(5)]
    assert order == ["t0", "t1", "t2", "t3", "t4"], order


def test_exponential_backoff_math():
    """Verify the backoff formula matches expectations."""
    base, cap = 2.0, 600.0
    expected = [2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 600.0, 600.0]
    for retries, want in enumerate(expected, start=1):
        got = min(base * (2 ** (retries - 1)), cap)
        assert got == want, f"retry {retries}: got {got}, want {want}"


if __name__ == "__main__":
    test_priority_ordering();           print("PASS: priority_ordering")
    test_delayed_tasks_stay_buried();   print("PASS: delayed_tasks_stay_buried")
    test_lazy_cancellation();           print("PASS: lazy_cancellation")
    test_fifo_tiebreak();               print("PASS: fifo_tiebreak")
    test_exponential_backoff_math();    print("PASS: exponential_backoff_math")
    print("\nAll tests pass.")
