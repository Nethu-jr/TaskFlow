"""
Production training pipeline.

PIPELINE
--------
1. Pull completed tasks from Postgres (last N days).
2. Featurize each row using the SAME `featurize()` used at inference time.
   (Feature drift is the #1 cause of broken ML pipelines — share the function.)
3. Use observed `priority` as the label. Yes, this is "self-prediction" — but
   recall priority is set by humans, ML, OR overridden via /run-now. Over time
   the model learns the patterns of override decisions, which is the actual
   signal we want to amplify.
4. Augment with synthetic data while we're cold-starting (n_real < 1000).
5. Train, evaluate, save with timestamp.
6. Atomically swap symlink → priority_model.joblib.

RUN MODE
--------
Run as: `python ml/training/train_from_db.py`

Or schedule it as an ITSS task (eat your own dog food):
  POST /crons {
    "name": "weekly_ml_retrain",
    "cron_expr": "0 3 * * 0",
    "task_type": "ml_inference",
    "payload": {"action": "retrain"}
  }
And add a handler that shells out to this script.
"""
import asyncio
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import joblib
import numpy as np
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "backend"))

from app.ml.predictor import featurize, TASK_TYPES         # noqa: E402
from app.db.audit import init_db, session, close_db         # noqa: E402
from app.db.models import TaskHistory                       # noqa: E402
from sqlalchemy import select                                # noqa: E402

# Reuse the synthetic generator for cold-start augmentation
sys.path.insert(0, str(Path(__file__).resolve().parent))
from train import generate_synthetic_data                   # noqa: E402


MIN_REAL_SAMPLES = 1000        # below this, augment with synthetic
LOOKBACK_DAYS = 90             # how far back to pull training rows


async def fetch_real_data() -> tuple[np.ndarray, np.ndarray]:
    """Pull (X, y) from completed-task history."""
    cutoff = datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)
    async with session() as s:
        rows = (await s.execute(
            select(TaskHistory).where(
                TaskHistory.status == "completed",
                TaskHistory.completed_at >= cutoff,
            )
        )).scalars().all()

    if not rows:
        return np.empty((0, len(TASK_TYPES) + 4)), np.empty((0,))

    X = np.array([
        featurize(r.task_type, r.payload or {}, r.created_at)
        for r in rows
    ])
    y = np.array([float(r.priority) for r in rows])
    return X, y


async def main():
    print("Initializing DB connection...")
    await init_db()

    print(f"Fetching real training data (last {LOOKBACK_DAYS} days)...")
    X_real, y_real = await fetch_real_data()
    print(f"  Real samples: {len(y_real)}")

    if len(y_real) < MIN_REAL_SAMPLES:
        print(f"  Below threshold ({MIN_REAL_SAMPLES}); augmenting with synthetic data.")
        X_syn, y_syn = generate_synthetic_data(n=MIN_REAL_SAMPLES * 2)
        if len(y_real) > 0:
            X = np.vstack([X_real, X_syn])
            y = np.concatenate([y_real, y_syn])
        else:
            X, y = X_syn, y_syn
    else:
        X, y = X_real, y_real

    print(f"Total training samples: {len(y)}")

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    print("Training GradientBoostingRegressor...")
    model = GradientBoostingRegressor(
        n_estimators=200,
        max_depth=4,
        learning_rate=0.05,
        random_state=42,
    )
    model.fit(X_train, y_train)

    pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, pred)
    r2 = r2_score(y_test, pred)
    print(f"Test MAE: {mae:.3f}   R²: {r2:.3f}")

    # Atomic swap: write to a versioned filename, then symlink. Means:
    # if anything crashes mid-write, the old model stays loaded.
    out_dir = Path(__file__).resolve().parents[1] / "models"
    out_dir.mkdir(parents=True, exist_ok=True)
    versioned = out_dir / f"priority_model_{int(time.time())}.joblib"
    canonical = out_dir / "priority_model.joblib"
    joblib.dump(model, versioned)
    if canonical.exists() or canonical.is_symlink():
        canonical.unlink()
    os.symlink(versioned.name, canonical)
    print(f"Saved {versioned}")
    print(f"Linked {canonical} -> {versioned.name}")

    await close_db()


if __name__ == "__main__":
    asyncio.run(main())
