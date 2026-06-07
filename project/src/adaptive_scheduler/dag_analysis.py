"""DAG critical-path and ETL total-time helpers."""

from __future__ import annotations

from collections import defaultdict, deque
import heapq


def calculate_critical_path_time(
    graph: dict[str, list[str]],
    task_times_sec: dict[str, float],
) -> float:
    """T_DAG = max(sum(T_i) across all DAG paths)."""
    indegree: dict[str, int] = defaultdict(int)
    for node in graph:
        indegree.setdefault(node, 0)
        for child in graph[node]:
            indegree[child] += 1

    queue = deque([node for node, degree in indegree.items() if degree == 0])
    longest_to: dict[str, float] = {node: max(task_times_sec.get(node, 0.0), 0.0) for node in indegree}

    visited = 0
    while queue:
        node = queue.popleft()
        visited += 1
        base_time = longest_to.get(node, 0.0)
        for child in graph.get(node, []):
            candidate = base_time + max(task_times_sec.get(child, 0.0), 0.0)
            if candidate > longest_to.get(child, 0.0):
                longest_to[child] = candidate
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)

    if visited != len(indegree):
        raise ValueError("Task dependency graph must be acyclic to compute critical path.")
    return max(longest_to.values(), default=0.0)


def calculate_etl_time(
    extract_time_sec: float,
    transform_time_sec: float,
    load_time_sec: float,
    branch_times_sec: list[float] | None = None,
) -> float:
    """
    T_ETL = T_extract + T_transform + T_load
    or for parallel branches: T_ETL = max(T_branch_i) + T_load.
    """
    if branch_times_sec:
        return max(max(value, 0.0) for value in branch_times_sec) + max(load_time_sec, 0.0)
    return max(extract_time_sec, 0.0) + max(transform_time_sec, 0.0) + max(load_time_sec, 0.0)


def prioritize_execution_order(
    graph: dict[str, list[str]],
    task_loads: dict[str, float],
    predicted_times_sec: dict[str, float],
    avg_load: float,
    balance_coefficient: float,
) -> tuple[list[str], list[list[str]], dict[str, float]]:
    """
    Build adaptive topological execution order.

    Priority score combines normalized task load and predicted runtime:
      score = w_l * (L_i / L_avg) + w_t * (T_i / T_max), amplified by imbalance B.
    """
    indegree: dict[str, int] = defaultdict(int)
    for node, children in graph.items():
        indegree.setdefault(node, 0)
        for child in children:
            indegree[child] += 1

    max_pred = max((max(v, 0.0) for v in predicted_times_sec.values()), default=1.0)
    safe_avg_load = avg_load if avg_load > 0 else 1.0
    imbalance_multiplier = 1.0 + max(balance_coefficient - 1.0, 0.0) * 0.25

    def _score(task_id: str) -> float:
        load_ratio = max(task_loads.get(task_id, 0.0), 0.0) / safe_avg_load
        time_ratio = max(predicted_times_sec.get(task_id, 0.0), 0.0) / max_pred
        return ((0.7 * load_ratio) + (0.3 * time_ratio)) * imbalance_multiplier

    priority_by_task = {task_id: _score(task_id) for task_id in indegree}
    ready: list[tuple[float, str]] = [(-priority_by_task[task_id], task_id) for task_id, d in indegree.items() if d == 0]
    heapq.heapify(ready)

    order: list[str] = []
    waves: list[list[str]] = []
    visited = 0
    while ready:
        wave_size = len(ready)
        current_wave: list[str] = []
        next_ready: list[tuple[float, str]] = []
        for _ in range(wave_size):
            _, node = heapq.heappop(ready)
            current_wave.append(node)
            order.append(node)
            visited += 1
            for child in graph.get(node, []):
                indegree[child] -= 1
                if indegree[child] == 0:
                    heapq.heappush(next_ready, (-priority_by_task.get(child, 0.0), child))
        if current_wave:
            waves.append(current_wave)
        ready = next_ready

    if visited != len(indegree):
        raise ValueError("Task dependency graph must be acyclic to build execution order.")
    return order, waves, priority_by_task
