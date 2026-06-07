"""Time prediction utilities for adaptive ETL scheduling."""

from __future__ import annotations


def average_prediction(history: list[float]) -> float:
    """T_pred = (1 / n) * sum(T_i)."""
    valid_values = [float(value) for value in history if value is not None and value >= 0]
    if not valid_values:
        return 0.0
    return sum(valid_values) / len(valid_values)


def update_prediction_with_smoothing(
    actual_time_sec: float,
    predicted_old_sec: float,
    smoothing_lambda: float = 0.3,
) -> float:
    """T_pred_new = lambda * T_actual + (1 - lambda) * T_pred_old."""
    safe_lambda = min(max(smoothing_lambda, 0.0), 1.0)
    return safe_lambda * max(actual_time_sec, 0.0) + (1.0 - safe_lambda) * max(predicted_old_sec, 0.0)
