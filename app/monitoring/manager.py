"""监测任务管理器：单进程单任务的轮询采样与超温事件状态机。

职责
====
* 进程内最多运行一个监测任务；启动/停止互斥，停止中拒绝再启动。
* 复用进程级共享 :class:`~app.modbus_client.ModbusTcpClient`，每轮对 8 个回路
  并发采样（同连接流水线在途），每轮结束后等待 1 秒再开下一轮。
* 每回路判定规则：

  - 连续**两轮成功且超限**才开启超温事件（第一轮 ``over_limit`` 待确认，
    第二轮 ``opened``）。
  - 成功且未超限：若事件已开则按**恢复**结束（``recovered``），否则记
    ``normal``；待确认计数清零。
  - 采集失败（缺测）：待确认计数重置为 0；已开启的事件**只记缺测**
    （``missing``），绝不解除。
* 停止：受理后不再开启新轮次，等待在途轮次完成并落库后才上报停止；
  仍未结束的事件一律标记为**中断**，不能记作恢复。
* 代际隔离：每轮 DB 写入前校验任务代次，旧任务（已停止/被新任务取代）的
  残余协程无法写入新任务或旧库。
* 请求断连不影响停止：停止逻辑在请求任务之外独立运行，重复停止共享同一次收尾。
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from app.modbus_client import ModbusError, ModbusTcpClient
from app.monitoring.repository import (
    EVENT_INTERRUPTED,
    EVENT_RECOVERED,
    JUDGE_MISSING,
    JUDGE_NORMAL,
    JUDGE_OPENED,
    JUDGE_OVER_LIMIT,
    JUDGE_RECOVERED,
    TASK_RUNNING,
    TASK_STOPPED,
    TASK_STOPPING,
    MonitoringRepository,
)
from app.monitoring.schemas import THRESHOLD_MAX, THRESHOLD_MIN


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _to_celsius(raw: int) -> float:
    """有符号 16 位整数除以 10，得到摄氏温度。"""
    return round(raw / 10.0, 2)


@dataclass
class CircuitState:
    """单回路的内存判定状态（权威状态仍以 SQLite 落库为准）。"""

    circuit: int
    threshold_c: float
    confirm_count: int = 0          # 连续成功超限的待确认计数
    open_event: dict[str, Any] | None = None  # 当前开启事件的行快照


@dataclass
class _TaskRuntime:
    task_id: str
    generation: int
    thresholds_c: list[float]
    circuits: list[CircuitState]
    stop_event: asyncio.Event
    done_event: asyncio.Event
    monitor_task: asyncio.Task | None
    started_at: str
    # 已落库轮次编号；停止时若没有在途轮，收尾轮编号取此值。
    last_persisted_round: int = 0


class MonitorError(Exception):
    """监测任务操作错误，映射为 409。"""

    def __init__(self, message: str, code: str = "task_conflict") -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class MonitorManager:
    """单进程监测任务管理器（仅在事件循环线程内使用）。"""

    def __init__(
        self,
        client_provider,
        repository: MonitoringRepository,
        circuit_count: int,
        interval_seconds: float,
    ) -> None:
        # client_provider() 返回进程级共享 Modbus 客户端（允许测试替换单例）。
        self._client_provider = client_provider
        self._repo = repository
        self._circuit_count = circuit_count
        self._interval = interval_seconds
        self._generation = 0
        self._runtime: _TaskRuntime | None = None
        self._lock = asyncio.Lock()

    @property
    def _client(self) -> ModbusTcpClient:
        return self._client_provider()

    # ------------------------------------------------------------------
    # 对外操作
    # ------------------------------------------------------------------

    @property
    def current_task_id(self) -> str | None:
        return self._runtime.task_id if self._runtime is not None else None

    async def start(self, thresholds_c: list[float]) -> dict[str, Any]:
        """启动一个新监测任务；已有运行/收尾中的任务时拒绝（409）。"""
        self._validate_thresholds(thresholds_c)
        async with self._lock:
            if self._runtime is not None:
                raise MonitorError(
                    "监测任务已存在：单进程同时只允许运行一个任务，"
                    f"当前任务 {self._runtime.task_id} 状态为 "
                    f"{await self._task_status(self._runtime.task_id)}，请先停止"
                )
            self._generation += 1
            generation = self._generation
            task_id = uuid.uuid4().hex
            now = _now_iso()
            runtime = _TaskRuntime(
                task_id=task_id,
                generation=generation,
                thresholds_c=list(thresholds_c),
                circuits=[
                    CircuitState(circuit=i, threshold_c=thresholds_c[i])
                    for i in range(self._circuit_count)
                ],
                stop_event=asyncio.Event(),
                done_event=asyncio.Event(),
                monitor_task=None,
                started_at=now,
            )
            await asyncio.to_thread(
                self._repo.insert_task,
                {
                    "id": task_id,
                    "thresholds_c": list(thresholds_c),
                    "status": TASK_RUNNING,
                    "created_at": now,
                    "started_at": now,
                },
            )
            runtime.monitor_task = asyncio.create_task(
                self._monitor_loop(runtime),
                name=f"monitor-loop-{task_id[:8]}",
            )
            self._runtime = runtime
            task = await asyncio.to_thread(self._repo.get_task, task_id)
            return task

    async def stop(self, task_id: str) -> dict[str, Any]:
        """受理停止：不开新轮，等在途轮落库并完成事件中断后返回。

        重复停止同一任务：等待同一次收尾并返回同一份结果。
        """
        async with self._lock:
            runtime = self._runtime
            if runtime is None or runtime.task_id != task_id:
                existing = await asyncio.to_thread(self._repo.get_task, task_id)
                if existing is not None:
                    raise MonitorError(
                        f"任务 {task_id} 已停止，不能重复停止",
                        code="task_not_running",
                    )
                raise MonitorError(
                    f"监测任务 {task_id} 不存在", code="task_not_found"
                )
            # 首个停止请求把状态切到 stopping 并触发停止信号；
            # 后续重复请求落到锁外等待同一个 done_event。
            if not runtime.stop_event.is_set():
                await asyncio.to_thread(
                    self._repo.update_task_status, task_id, TASK_STOPPING
                )
                runtime.stop_event.set()

        # 锁外等待：HTTP 请求即使断连，监测协程仍会完成收尾并 set 事件。
        await runtime.done_event.wait()

        async with self._lock:
            task = await asyncio.to_thread(self._repo.get_task, task_id)
            interrupted = await asyncio.to_thread(
                self._repo.list_events,
                task_id,
                EVENT_INTERRUPTED,
                None,
                10000,
                0,
            )
            # 仅统计收尾时刻关闭的中断事件：取收尾轮关闭的事件。
            final_round = task["last_round_no"]
            interrupted_ids = [
                e["id"]
                for e in interrupted
                if e["closed_round_no"] == final_round
            ]
        return {
            "task": task,
            "finalized_round_no": final_round,
            "interrupted_event_ids": interrupted_ids,
        }

    async def shutdown(self) -> None:
        """应用停机：优雅停止当前任务，保证在途轮落库、事件标中断。"""
        async with self._lock:
            runtime = self._runtime
        if runtime is None:
            return
        if not runtime.stop_event.is_set():
            with contextlib.suppress(Exception):
                await asyncio.to_thread(
                    self._repo.update_task_status,
                    runtime.task_id,
                    TASK_STOPPING,
                )
            runtime.stop_event.set()
        await runtime.done_event.wait()

    # ------------------------------------------------------------------
    # 监测主循环
    # ------------------------------------------------------------------

    async def _monitor_loop(self, runtime: _TaskRuntime) -> None:
        """单任务轮询循环：采样 → 判定落库 → 等待 1 秒，直到收到停止信号。"""
        round_no = 0
        try:
            while not runtime.stop_event.is_set():
                round_no += 1
                await self._run_round(runtime, round_no)
                if runtime.stop_event.is_set():
                    break
                # 每轮结束 1 秒后再采；停止信号可立即唤醒等待。
                try:
                    await asyncio.wait_for(
                        runtime.stop_event.wait(), timeout=self._interval
                    )
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            runtime.stop_event.set()
            raise
        finally:
            await self._finalize(runtime)

    async def _finalize(self, runtime: _TaskRuntime) -> None:
        """停止收尾：不再开新轮；在途轮已在循环内落库，这里只把仍未结束的
        事件标记为中断（不能记作恢复），再把任务置为 stopped。"""
        try:
            # 代际隔离：仅当运行态仍指向本任务时才允许收尾写库；
            # 运行态为空或已被新任务替换时，旧任务一律不得写入。
            if self._runtime is None or self._runtime.task_id != runtime.task_id:
                runtime.done_event.set()
                return
            await self._interrupt_open_events(runtime)
            if self._runtime is None or self._runtime.task_id != runtime.task_id:
                runtime.done_event.set()
                return
            finished_at = _now_iso()
            await asyncio.to_thread(
                self._repo.update_task_status,
                runtime.task_id,
                TASK_STOPPED,
                finished_at,
            )
        except Exception:  # 收尾异常也要解除停止等待，避免请求永久挂起
            with contextlib.suppress(Exception):
                await asyncio.to_thread(
                    self._repo.update_task_status,
                    runtime.task_id,
                    TASK_STOPPED,
                    _now_iso(),
                )
            raise
        finally:
            async with self._lock:
                if (
                    self._runtime is not None
                    and self._runtime.task_id == runtime.task_id
                ):
                    self._runtime = None
            runtime.done_event.set()

    # ------------------------------------------------------------------
    # 单轮采样 + 判定 + 落库
    # ------------------------------------------------------------------

    async def _run_round(self, runtime: _TaskRuntime, round_no: int) -> None:
        started_at = _now_iso()
        # 复用共享 Modbus 客户端：8 笔读取并发在途（流水线）。
        results = await asyncio.gather(
            *(
                self._sample_one(runtime, state)
                for state in runtime.circuits
            )
        )

        opened_events: list[dict[str, Any]] = []
        closed_events: list[dict[str, Any]] = []
        readings: list[dict[str, Any]] = []

        for state, result in zip(runtime.circuits, results):
            reading = self._judge(
                runtime,
                state,
                result,
                round_no,
                opened_events,
                closed_events,
            )
            readings.append(reading)

        open_count = sum(1 for c in runtime.circuits if c.open_event is not None)
        # 中断累计数以库内统计为准（内存只保留当前开启事件）。
        interrupted_total = await asyncio.to_thread(
            self._repo.count_events, runtime.task_id, EVENT_INTERRUPTED, None
        )
        finished_at = _now_iso()

        # 代际校验：旧任务的残余协程不得写入（正常路径下任务未被替换）。
        if self._runtime is None or self._runtime.task_id != runtime.task_id:
            return

        # 事务内回填新事件自增 id 与内存 open_event，保证内存与库一致。
        await asyncio.to_thread(
            self._repo.persist_round,
            runtime.task_id,
            round_no,
            started_at,
            finished_at,
            readings,
            opened_events,
            closed_events,
            open_count,
            interrupted_total,
            runtime.circuits,
        )
        runtime.last_persisted_round = round_no

    async def _sample_one(
        self, runtime: _TaskRuntime, state: CircuitState
    ) -> dict[str, Any]:
        """单回路采样，异常统一转失败结果（不向外抛）。"""
        try:
            raw, _ = await self._client.read_holding_register(state.circuit)
        except ModbusError as exc:
            return {
                "circuit": state.circuit,
                "ok": False,
                "error_code": exc.code.value,
                "error_message": exc.message,
                "sampled_at": _now_iso(),
            }
        except Exception as exc:  # 防御：任何意外异常都按缺测处理
            return {
                "circuit": state.circuit,
                "ok": False,
                "error_code": "protocol_error",
                "error_message": f"采集失败：未预期错误（{exc.__class__.__name__}）",
                "sampled_at": _now_iso(),
            }
        return {
            "circuit": state.circuit,
            "ok": True,
            "raw_register": raw,
            "temperature_c": _to_celsius(raw),
            "sampled_at": _now_iso(),
        }

    def _judge(
        self,
        runtime: _TaskRuntime,
        state: CircuitState,
        result: dict[str, Any],
        round_no: int,
        opened_events: list[dict[str, Any]],
        closed_events: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """单回路单轮判定，并同步更新内存事件状态。"""
        base: dict[str, Any] = {
            "circuit": state.circuit,
            "threshold_c": state.threshold_c,
            "sampled_at": result["sampled_at"],
            "event_id": None,
            "new_event_ref": None,
        }

        # ---- 采集失败：缺测 ----
        if not result["ok"]:
            # 失败重置待确认计数；已开事件只记缺测，绝不解除。
            state.confirm_count = 0
            base.update(
                {
                    "ok": False,
                    "temperature_c": None,
                    "raw_register": None,
                    "error_code": result["error_code"],
                    "error_message": result["error_message"],
                    "judge_status": JUDGE_MISSING,
                    "confirm_count": 0,
                }
            )
            if state.open_event is not None:
                base["event_id"] = state.open_event["id"]
            return base

        temperature_c = result["temperature_c"]
        base.update(
            {
                "ok": True,
                "temperature_c": temperature_c,
                "raw_register": result["raw_register"],
                "error_code": None,
                "error_message": None,
            }
        )
        over_limit = temperature_c > state.threshold_c

        if over_limit:
            if state.open_event is not None:
                # 事件已开启：持续超限，仅记录判定，不重复开事件。
                state.confirm_count = 2
                base.update(
                    {
                        "judge_status": JUDGE_OVER_LIMIT,
                        "confirm_count": 2,
                        "event_id": state.open_event["id"],
                    }
                )
                return base
            # 连续两轮成功超限才开事件。
            state.confirm_count += 1
            if state.confirm_count >= 2:
                event = {
                    "circuit": state.circuit,
                    "threshold_c": state.threshold_c,
                    "opened_at": result["sampled_at"],
                    "opened_temp_c": temperature_c,
                }
                opened_events.append(event)
                base["new_event_ref"] = state.circuit
                base.update(
                    {
                        "judge_status": JUDGE_OPENED,
                        "confirm_count": state.confirm_count,
                    }
                )
                return base
            base.update(
                {
                    "judge_status": JUDGE_OVER_LIMIT,
                    "confirm_count": state.confirm_count,
                }
            )
            return base

        # ---- 成功且未超限 ----
        if state.open_event is not None:
            # 成功不超限才按恢复结束（缺测路径不会走到这里）。
            event = state.open_event
            closed_events.append(
                {
                    "event_id": event["id"],
                    "circuit": state.circuit,
                    "status": EVENT_RECOVERED,
                    "closed_at": result["sampled_at"],
                    "closed_temp_c": temperature_c,
                }
            )
            state.open_event = None
            state.confirm_count = 0
            base.update(
                {
                    "judge_status": JUDGE_RECOVERED,
                    "confirm_count": 0,
                    "event_id": event["id"],
                }
            )
            return base

        state.confirm_count = 0
        base.update(
            {"judge_status": JUDGE_NORMAL, "confirm_count": 0}
        )
        return base

    async def _interrupt_open_events(self, runtime: _TaskRuntime) -> None:
        """在途轮已正常落库的停止路径：把仍开启的事件标记为中断。"""
        if not any(c.open_event is not None for c in runtime.circuits):
            return
        closed_at = _now_iso()
        rows = await asyncio.to_thread(
            self._repo.interrupt_open_events,
            runtime.task_id,
            runtime.last_persisted_round,
            closed_at,
        )
        for row in rows:
            state = runtime.circuits[row["circuit"]]
            if state.open_event is not None and state.open_event["id"] == row["id"]:
                state.open_event = None
            state.confirm_count = 0

    # ------------------------------------------------------------------
    # 查询（全部走 SQLite，保证返回同一已提交状态）
    # ------------------------------------------------------------------

    async def get_task(self, task_id: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._repo.get_task, task_id)

    async def list_tasks(self) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._repo.list_tasks)

    async def list_rounds(
        self, task_id: str, limit: int, offset: int
    ) -> tuple[list[dict[str, Any]], int]:
        rows, total = await asyncio.gather(
            asyncio.to_thread(self._repo.list_rounds, task_id, limit, offset),
            asyncio.to_thread(self._repo.count_rounds, task_id),
        )
        return rows, total

    async def get_round_detail(
        self, task_id: str, round_no: int
    ) -> dict[str, Any] | None:
        round_row = await asyncio.to_thread(
            self._repo.get_round, task_id, round_no
        )
        if round_row is None:
            return None
        readings = await asyncio.to_thread(
            self._repo.list_readings_for_round, task_id, round_no
        )
        round_row["readings"] = readings
        return round_row

    async def list_events(
        self,
        task_id: str,
        status: str | None,
        circuit: int | None,
        limit: int,
        offset: int,
    ) -> tuple[list[dict[str, Any]], int]:
        rows, total = await asyncio.gather(
            asyncio.to_thread(
                self._repo.list_events, task_id, status, circuit, limit, offset
            ),
            asyncio.to_thread(
                self._repo.count_events, task_id, status, circuit
            ),
        )
        return rows, total

    # ------------------------------------------------------------------

    def _validate_thresholds(self, thresholds_c: list[float]) -> None:
        if len(thresholds_c) != self._circuit_count:
            raise MonitorError(
                f"温度上限必须恰好包含 {self._circuit_count} 个回路，"
                f"收到 {len(thresholds_c)} 个",
                code="invalid_thresholds",
            )
        for index, value in enumerate(thresholds_c):
            if not THRESHOLD_MIN <= value <= THRESHOLD_MAX:
                raise MonitorError(
                    f"回路 {index} 的温度上限 {value} 超出允许范围"
                    f"[{THRESHOLD_MIN}, {THRESHOLD_MAX}]",
                    code="invalid_thresholds",
                )

    async def _task_status(self, task_id: str) -> str:
        row = await asyncio.to_thread(self._repo.get_task, task_id)
        return row["status"] if row else "unknown"
