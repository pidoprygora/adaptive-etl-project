"""Data models for adaptive ETL scheduling decisions."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class TaskProfile:
    """Static and predicted properties of one ETL task."""

    task_id: str
    data_volume: float
    transformation_complexity: float
    sequential_time_sec: float
    parallelizable_fraction: float = 0.8
    stage: str = "transform"


@dataclass(frozen=True)
class ResourceSnapshot:
    """CPU and RAM usage snapshots."""

    cpu_used: float
    cpu_total: float
    ram_used: float
    ram_total: float


@dataclass(frozen=True)
class TaskDecision:
    """Result of adaptive scheduling for one task."""

    task_id: str
    predicted_time_sec: float
    task_load: float
    avg_task_load: float
    recommended_parallelism: int
    adapted_parallelism: int
    cpu_utilization: float
    ram_utilization: float
    speedup: float
    efficiency: float
    amdahl_speedup: float
    priority_score: float = 0.0
    priority_rank: int = 0
    execution_wave: int = 0


@dataclass(frozen=True)
class SchedulerSummary:
    """Run-level summary metrics."""

    avg_load: float
    max_load: float
    balance_coefficient: float
    sequential_time_sec: float
    parallel_time_sec: float
    dag_critical_path_sec: float
    etl_time_sec: float
    run_speedup: float
    run_efficiency: float
    run_amdahl_speedup: float
    recommended_task_concurrency: int
    max_execution_wave_width: int
    execution_order: list[str] = field(default_factory=list)
    execution_waves: list[list[str]] = field(default_factory=list)
    decisions: list[TaskDecision] = field(default_factory=list)
