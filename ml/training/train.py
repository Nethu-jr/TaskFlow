"""
Training pipeline for the priority predictor.

In production:
  - Replace `generate_synthetic_data()` with a SQL query against the tasks table:
        SELECT task_type, payload, created_at, priority
        FROM tasks
        WHERE status='completed' AND created_at > NOW() - INTERVAL '30 days'
  - Run on a schedule (Airflow DAG, cron, or as an ITSS task itself!)
  - Track metrics in MLflow / Weights & Biases.

This script generates plausible synthetic data so the model exists from day 1.
"""
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np
import joblib
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "backend"))
from app.ml.predictor import featurize, TASK_TYPES


RNG = np.random.default_rng(42)


def synth_priority(task_type: str, payload: dict, ts: datetime) -> float:
    """
    Ground-truth priority generator. The model learns this function.
    Captures realistic patterns:
      - emails are usually mid-urgency
      - reports are low-urgency unless "urgent" appears
      - large payloads -> lower priority (slower)
      - business-hours submissions slightly more urgent
    """
    base = {"email": 3.5, "report": 6.5, "data_sync": 5.0,
            "ml_inference": 4.0, "generic": 5.5}[task_type]
    payload_kb = len(json.dumps(payload).encode()) / 1024.0
    base += min(payload_kb / 20.0, 2.0)                  # large -> lower priority
    if any(k in str(payload).lower() for k in ("urgent", "asap", "critical")):
        base -= 2.0
    if 9 <= ts.hour <= 17:
        base -= 0.3                                       # business hours
    base += RNG.normal(0, 0.4)                            # noise
    return float(np.clip(base, 1, 10))


def generate_synthetic_data(n: int = 5000):
    X, y = [], []
    base_time = datetime.utcnow() - timedelta(days=60)
    for i in range(n):
        task_type = RNG.choice(TASK_TYPES, p=[0.35, 0.15, 0.20, 0.15, 0.15])
        ts = base_time + timedelta(seconds=int(RNG.uniform(0, 60 * 86400)))
        payload = {"id": i, "size": int(RNG.exponential(500))}
        if RNG.random() < 0.1:
            payload["note"] = "urgent"
        X.append(featurize(task_type, payload, ts))
        y.append(synth_priority(task_type, payload, ts))
    return np.array(X), np.array(y)


def main():
    print("Generating synthetic training data...")
    X, y = generate_synthetic_data(n=5000)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    print(f"Training on {len(X_train)} samples...")
    model = GradientBoostingRegressor(
        n_estimators=200,
        max_depth=4,
        learning_rate=0.05,
        random_state=42,
    )
    model.fit(X_train, y_train)

    pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, pred)
    print(f"Test MAE: {mae:.3f} priority points (target < 1.0)")

    out = Path(__file__).resolve().parents[1] / "models" / "priority_model.joblib"
    out.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, out)
    print(f"Model saved to {out}")


if __name__ == "__main__":
    main()
