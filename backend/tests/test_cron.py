"""
Tests for cron expression parsing and next-fire computation.
Pure logic — no Redis or Postgres needed.
"""
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.models.cron import CronSchedule
from app.models.schemas import TaskType


def test_validate_rejects_garbage():
    try:
        CronSchedule.validate_expr("not a cron")
    except ValueError:
        return
    assert False, "should have raised"


def test_validate_accepts_standard():
    for expr in ["* * * * *", "0 */6 * * *", "0 0 * * 0", "30 2 1 * *"]:
        CronSchedule.validate_expr(expr)


def test_next_fire_strictly_in_future():
    now = time.time()
    c = CronSchedule(name="t", cron_expr="* * * * *",
                     task_type=TaskType.GENERIC, payload={})
    nf = c.next_fire_epoch()
    # Must be strictly in the future, no further than 60s out for "every minute"
    assert nf > now, f"next_fire {nf} not after now {now}"
    assert nf - now <= 60.5, f"next_fire too far: {nf - now}s"


def test_catchup_advances_strictly():
    """When we rebase on past `after`, each successive call must advance."""
    c = CronSchedule(name="t", cron_expr="* * * * *",
                     task_type=TaskType.GENERIC, payload={})
    base = time.time() - 600        # 10 minutes ago
    seq = []
    cursor = base
    for _ in range(5):
        cursor = c.next_fire_epoch(after=cursor)
        seq.append(cursor)
    # Each must be strictly greater than the last
    assert all(seq[i+1] > seq[i] for i in range(len(seq) - 1))
    # First is strictly after base
    assert seq[0] > base


def test_disabled_cron_serializes():
    c = CronSchedule(name="t", cron_expr="0 * * * *",
                     task_type=TaskType.EMAIL, payload={"to": "x"},
                     enabled=False)
    blob = c.model_dump_json()
    restored = CronSchedule.model_validate_json(blob)
    assert restored.enabled is False
    assert restored.cron_expr == "0 * * * *"


if __name__ == "__main__":
    test_validate_rejects_garbage();    print("PASS: validate_rejects_garbage")
    test_validate_accepts_standard();   print("PASS: validate_accepts_standard")
    test_next_fire_strictly_in_future();print("PASS: next_fire_strictly_in_future")
    test_catchup_advances_strictly();   print("PASS: catchup_advances_strictly")
    test_disabled_cron_serializes();    print("PASS: disabled_cron_serializes")
    print("\nAll cron tests pass.")
