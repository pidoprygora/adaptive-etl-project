"""Adaptive scheduler implementation for Airflow + PySpark ETL tasks."""

from __future__ import annotations

import math

from .dag_analysis import calculate_critical_path_time, calculate_etl_time, prioritize_execution_order
from .models import ResourceSnapshot, SchedulerSummary, TaskDecision, TaskProfile
from .resource_monitor import adapt_parallelism, cpu_utilization, ram_utilization
from .time_prediction import average_prediction


class AdaptiveScheduler:
    """Computes adaptive parallelism decisions for ETL tasks."""

    def __init__(
        self,
        alpha: float = 0.4,
        beta: float = 0.3,
        gamma: float = 0.3,
        p_max: int = 8,
    ) -> None:
        self.alpha = alpha
        self.beta = beta
        self.gamma = gamma
        self.p_max = max(1, p_max)

    def calculate_task_load(self, profile: TaskProfile, predicted_time_sec: float) -> float:
        """L_i = alpha*D_i + beta*C_i + gamma*T_i."""
        return (
            self.alpha * max(profile.data_volume, 0.0)
            + self.beta * max(profile.transformation_complexity, 0.0)
            + self.gamma * max(predicted_time_sec, 0.0)
        )

    @staticmethod
    def calculate_average_load(task_loads: list[float]) -> float:
        """L_avg = (1 / n) * sum(L_i)."""
        if not task_loads:
            return 0.0
        return sum(max(load, 0.0) for load in task_loads) / len(task_loads)

    def recommended_parallelism(self, load_i: float, avg_load: float) -> int:
        """P_i = min(P_max, ceil(L_i / L_avg))."""
        if avg_load <= 0:
            return 1
        return min(self.p_max, max(1, math.ceil(load_i / avg_load)))

    @staticmethod
    def speedup(sequential_time_sec: float, parallel_time_sec: float) -> float:
        """S = T_seq / T_par."""
        if parallel_time_sec <= 0:
            return 0.0
        return max(sequential_time_sec, 0.0) / parallel_time_sec

    @staticmethod
    def efficiency(speedup_value: float, parallelism: int) -> float:
        """E = S / P."""
        if parallelism <= 0:
            return 0.0
        return speedup_value / parallelism

    @staticmethod
    def amdahl_speedup(parallelizable_fraction: float, parallelism: int) -> float:
        """S_Amdahl = 1 / ((1 - q) + q / P)."""
        safe_q = min(max(parallelizable_fraction, 0.0), 1.0)
        safe_p = max(parallelism, 1)
        denominator = (1.0 - safe_q) + safe_q / safe_p
        if denominator <= 0:
            return 0.0
        return 1.0 / denominator

    @staticmethod
    def load_balance_coefficient(task_loads: list[float]) -> float:
        """B = L_max / L_avg."""
        if not task_loads:
            return 0.0
        l_max = max(task_loads)
        l_avg = sum(task_loads) / len(task_loads) if task_loads else 0.0
        if l_avg <= 0:
            return 0.0
        return l_max / l_avg

    @staticmethod
    def recommended_task_concurrency(
        max_execution_wave_width: int,
        avg_cpu_utilization: float,
        avg_ram_utilization: float,
        hard_cap: int = 8,
    ) -> int:
        """Recommend how many DAG tasks to run in parallel."""
        wave_cap = max(int(max_execution_wave_width), 1)
        cap = max(int(hard_cap), 1)
        pressure = max(float(avg_cpu_utilization), float(avg_ram_utilization))
        if pressure >= 0.85:
            pressure_cap = 1
        elif pressure >= 0.70:
            pressure_cap = 2
        elif pressure >= 0.55:
            pressure_cap = 3
        elif pressure >= 0.40:
            pressure_cap = 4
        else:
            pressure_cap = 6
        return max(1, min(wave_cap, cap, pressure_cap))

    def build_schedule(
        self,
        task_profiles: list[TaskProfile],
        task_history: dict[str, list[float]],
        task_resources: dict[str, ResourceSnapshot],
        task_graph: dict[str, list[str]] | None = None,
        branch_times_sec: list[float] | None = None,
    ) -> SchedulerSummary:
        """Compute adaptive decisions for all tasks in one DAG run."""
        if not task_profiles:
            return SchedulerSummary(
                avg_load=0.0,
                max_load=0.0,
                balance_coefficient=0.0,
                sequential_time_sec=0.0,
                parallel_time_sec=0.0,
                dag_critical_path_sec=0.0,
                etl_time_sec=0.0,
                run_speedup=0.0,
                run_efficiency=0.0,
                run_amdahl_speedup=0.0,
                recommended_task_concurrency=1,
                max_execution_wave_width=0,
                execution_order=[],
                execution_waves=[],
                decisions=[],
            )

        predicted_by_task: dict[str, float] = {}
        loads: list[float] = []
        for profile in task_profiles:
            history = task_history.get(profile.task_id, [])
            predicted = average_prediction(history) or max(profile.sequential_time_sec, 0.0)
            predicted_by_task[profile.task_id] = predicted
            loads.append(self.calculate_task_load(profile=profile, predicted_time_sec=predicted))

        avg_load = self.calculate_average_load(loads)
        max_load = max(loads) if loads else 0.0
        balance_coefficient = self.load_balance_coefficient(loads)
        task_loads_by_id = {
            profile.task_id: load for profile, load in zip(task_profiles, loads)
        }
        decisions: list[TaskDecision] = []
        task_parallel_times: dict[str, float] = {}
        task_pred_times: dict[str, float] = {}

        for profile, load in zip(task_profiles, loads):
            predicted = predicted_by_task[profile.task_id]
            p_i = self.recommended_parallelism(load_i=load, avg_load=avg_load)

            snapshot = task_resources.get(
                profile.task_id,
                ResourceSnapshot(cpu_used=0.0, cpu_total=1.0, ram_used=0.0, ram_total=1.0),
            )
            u_cpu = cpu_utilization(snapshot.cpu_used, snapshot.cpu_total)
            u_ram = ram_utilization(snapshot.ram_used, snapshot.ram_total)
            p_new = adapt_parallelism(current_parallelism=p_i, p_max=self.p_max, u_cpu=u_cpu, u_ram=u_ram)

            t_seq = max(profile.sequential_time_sec, predicted)
            t_par = t_seq / p_new if p_new > 0 else t_seq
            speedup_value = self.speedup(sequential_time_sec=t_seq, parallel_time_sec=t_par)
            efficiency_value = self.efficiency(speedup_value=speedup_value, parallelism=p_new)
            amdahl_value = self.amdahl_speedup(
                parallelizable_fraction=profile.parallelizable_fraction,
                parallelism=p_new,
            )

            task_parallel_times[profile.task_id] = t_par
            task_pred_times[profile.task_id] = predicted
            decisions.append(
                TaskDecision(
                    task_id=profile.task_id,
                    predicted_time_sec=predicted,
                    task_load=load,
                    avg_task_load=avg_load,
                    recommended_parallelism=p_i,
                    adapted_parallelism=p_new,
                    cpu_utilization=u_cpu,
                    ram_utilization=u_ram,
                    speedup=speedup_value,
                    efficiency=efficiency_value,
                    amdahl_speedup=amdahl_value,
                    priority_score=0.0,
                    priority_rank=0,
                    execution_wave=0,
                )
            )

        total_sequential = sum(max(profile.sequential_time_sec, 0.0) for profile in task_profiles)
        total_parallel = sum(task_parallel_times.values())

        if task_graph:
            critical_path = calculate_critical_path_time(task_graph, task_pred_times)
            execution_order, execution_waves, priority_by_task = prioritize_execution_order(
                graph=task_graph,
                task_loads=task_loads_by_id,
                predicted_times_sec=task_pred_times,
                avg_load=avg_load,
                balance_coefficient=balance_coefficient,
            )
        else:
            critical_path = total_parallel
            execution_order = [decision.task_id for decision in sorted(decisions, key=lambda d: d.task_load, reverse=True)]
            execution_waves = [execution_order.copy()] if execution_order else []
            priority_by_task = {decision.task_id: decision.task_load for decision in decisions}

        extract_time = sum(
            decision.predicted_time_sec for decision in decisions if decision.task_id.lower().startswith("extract")
        )
        transform_time = sum(
            decision.predicted_time_sec for decision in decisions if decision.task_id.lower().startswith("transform")
        )
        load_time = sum(
            decision.predicted_time_sec for decision in decisions if decision.task_id.lower().startswith("load")
        )
        if extract_time == 0.0 and transform_time == 0.0 and load_time == 0.0:
            etl_time = calculate_etl_time(
                extract_time_sec=total_sequential * 0.2,
                transform_time_sec=total_sequential * 0.6,
                load_time_sec=total_sequential * 0.2,
                branch_times_sec=branch_times_sec,
            )
        else:
            etl_time = calculate_etl_time(
                extract_time_sec=extract_time,
                transform_time_sec=transform_time,
                load_time_sec=load_time,
                branch_times_sec=branch_times_sec,
            )

        avg_parallelism = (
            sum(max(decision.adapted_parallelism, 1) for decision in decisions) / len(decisions)
            if decisions else 1.0
        )
        run_speedup = self.speedup(sequential_time_sec=total_sequential, parallel_time_sec=total_parallel)
        run_efficiency = self.efficiency(speedup_value=run_speedup, parallelism=max(int(round(avg_parallelism)), 1))
        run_amdahl_speedup = (
            sum(decision.amdahl_speedup for decision in decisions) / len(decisions)
            if decisions else 0.0
        )
        avg_cpu_util = (
            sum(decision.cpu_utilization for decision in decisions) / len(decisions)
            if decisions else 0.0
        )
        avg_ram_util = (
            sum(decision.ram_utilization for decision in decisions) / len(decisions)
            if decisions else 0.0
        )
        max_wave_width = max((len(wave) for wave in execution_waves), default=0)
        recommended_concurrency = self.recommended_task_concurrency(
            max_execution_wave_width=max_wave_width,
            avg_cpu_utilization=avg_cpu_util,
            avg_ram_utilization=avg_ram_util,
            hard_cap=self.p_max,
        )

        wave_by_task: dict[str, int] = {}
        for wave_idx, wave in enumerate(execution_waves, start=1):
            for task_id in wave:
                wave_by_task[task_id] = wave_idx
        rank_by_task = {task_id: rank for rank, task_id in enumerate(execution_order, start=1)}

        decisions_with_priority = [
            TaskDecision(
                task_id=decision.task_id,
                predicted_time_sec=decision.predicted_time_sec,
                task_load=decision.task_load,
                avg_task_load=decision.avg_task_load,
                recommended_parallelism=decision.recommended_parallelism,
                adapted_parallelism=decision.adapted_parallelism,
                cpu_utilization=decision.cpu_utilization,
                ram_utilization=decision.ram_utilization,
                speedup=decision.speedup,
                efficiency=decision.efficiency,
                amdahl_speedup=decision.amdahl_speedup,
                priority_score=priority_by_task.get(decision.task_id, 0.0),
                priority_rank=rank_by_task.get(decision.task_id, 0),
                execution_wave=wave_by_task.get(decision.task_id, 0),
            )
            for decision in decisions
        ]

        return SchedulerSummary(
            avg_load=avg_load,
            max_load=max_load,
            balance_coefficient=balance_coefficient,
            sequential_time_sec=total_sequential,
            parallel_time_sec=total_parallel,
            dag_critical_path_sec=critical_path,
            etl_time_sec=etl_time,
            run_speedup=run_speedup,
            run_efficiency=run_efficiency,
            run_amdahl_speedup=run_amdahl_speedup,
            recommended_task_concurrency=recommended_concurrency,
            max_execution_wave_width=max_wave_width,
            execution_order=execution_order,
            execution_waves=execution_waves,
            decisions=decisions_with_priority,
        )
