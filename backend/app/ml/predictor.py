"""
ML-based priority prediction.

PROBLEM
-------
When a user submits a task without specifying priority, we want to predict a
sensible value (1=urgent..10=can wait) based on:
  - task_type (one-hot)
  - hour_of_day (when submitted — peak hours might warrant lower priority)
  - day_of_week
  - payload_size (proxy for cost — large payloads de-prioritized)
  - historical_avg_runtime (slow tasks pre-empted by fast ones)
  - historical_failure_rate (failure-prone tasks scheduled with retries in mind)

MODEL
-----
GradientBoostingRegressor — handles non-linear interactions, robust to feature
scales, doesn't need one-hot expansion to converge well.

We frame as REGRESSION (continuous priority 1..10) then clip to int. This gives
finer-grained control than classification (10 classes is a lot of labels) and
the loss surface is smoother.

TRAINING
--------
Periodically retrain on completed-task logs from the database. The training
script is in `ml/training/train.py`. The model is loaded once at backend startup
and held in memory.

FALLBACK
--------
If the model is missing or fails to load, we fall back to a heuristic:
  priority = clamp(5 + payload_size_kb / 10 - urgency_keyword_bonus, 1, 10)
This guarantees the system works on day 1 with no training data.
"""
from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
import joblib
import numpy as np

from ..core.config import settings
from ..core.logging import setup_logging

log = setup_logging("ml")

# Order matters — must match training pipeline
TASK_TYPES = ["email", "report", "data_sync", "ml_inference", "generic"]


def featurize(task_type: str, payload: dict[str, Any], submitted_at: datetime) -> np.ndarray:
    """Convert a task spec into a feature vector. Same code is used in training."""
    one_hot = [1.0 if task_type == t else 0.0 for t in TASK_TYPES]
    payload_kb = len(json.dumps(payload).encode("utf-8")) / 1024.0
    hour = submitted_at.hour
    dow = submitted_at.weekday()
    # Heuristic features that the model can learn to weight or ignore
    has_urgent_kw = float(any(
        k in str(payload).lower() for k in ("urgent", "asap", "critical", "now")
    ))
    return np.array(one_hot + [payload_kb, hour, dow, has_urgent_kw], dtype=np.float32)


class PriorityPredictor:
    def __init__(self) -> None:
        self.model = None
        self._load()

    def _load(self) -> None:
        path = Path(settings.ML_MODEL_PATH)
        if not settings.ML_ENABLED or not path.exists():
            log.warning("ml_model_unavailable", extra={"path": str(path)})
            return
        try:
            self.model = joblib.load(path)
            log.info("ml_model_loaded", extra={"path": str(path)})
        except Exception as e:
            log.exception("ml_model_load_failed", extra={"error": str(e)})
            self.model = None

    def predict(self, task_type: str, payload: dict[str, Any], submitted_at: Optional[datetime] = None) -> int:
        submitted_at = submitted_at or datetime.utcnow()
        if self.model is not None:
            try:
                x = featurize(task_type, payload, submitted_at).reshape(1, -1)
                raw = float(self.model.predict(x)[0])
                return int(np.clip(round(raw), 1, 10))
            except Exception as e:
                log.exception("ml_predict_failed", extra={"error": str(e)})
                # fall through to heuristic
        return self._heuristic(task_type, payload)

    @staticmethod
    def _heuristic(task_type: str, payload: dict) -> int:
        """Cold-start fallback. Keep it sane and explainable."""
        base = {"email": 3, "report": 6, "data_sync": 5, "ml_inference": 4, "generic": 5}.get(task_type, 5)
        if any(k in str(payload).lower() for k in ("urgent", "asap", "critical")):
            base = max(1, base - 2)
        return int(np.clip(base, 1, 10))


predictor = PriorityPredictor()
