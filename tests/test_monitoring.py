"""持续监测任务测试：两轮确认、缺测不解除、恢复、停止中断与单任务约束。"""

from __future__ import annotations

import asyncio

import pytest

from app.monitoring.manager import MonitorError, MonitorManager
from app.monitoring.repository import (
    EVENT_INTERRUPTED,
    EVENT_OPEN,
    EVENT_RECOVERED,
    JUDGE_MISSING,
    JUDGE_NORMAL,
    JUDGE_OPENED,
    JUDGE_OVER_LIMIT,
    JUDGE_RECOVERED,
    TASK_STOPPED,
)
from simulator import app as simulator

# 模拟器默认温度（℃）：-18, -17.5, 5, 23.5, -20, 30, -4.5, 10.5
DEFAULT_TEMPS = [-18.0, -17.5, 5.0, 23.5, -20.0, 30.0, -4.5, 10.5]
# 仅回路 3（23.5℃）超上限 20℃；其余回路均不超限。
THRESHOLDS = [0.0, 0.0, 10.0, 20.0, 0.0, 40.0, 0.0, 20.0]


async def _wait_round(manager: MonitorManager, task_id: str, minimum: int) -> None:
    """等待任务至少落库 ``minimum`` 轮。"""
    for _ in range(200):
        task = await manager.get_task(task_id)
        if task["last_round_no"] >= minimum:
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"任务 {task_id} 在超时内未达到第 {minimum} 轮")


async def _start(monitor_env, thresholds=None):
    manager, _repo, _client = monitor_env
    return await manager.start(thresholds or THRESHOLDS)


# ---------------------------------------------------------------------------
# 状态机：两轮确认超限才开事件
# ---------------------------------------------------------------------------

async def test_two_consecutive_over_limit_rounds_open_event(monitor_env):
    manager, repo, _ = monitor_env
    task = await _start(monitor_env)
    task_id = task["id"]
    await _wait_round(manager, task_id, 2)

    r1 = await manager.get_round_detail(task_id, 1)
    r2 = await manager.get_round_detail(task_id, 2)
    c3_r1 = next(x for x in r1["readings"] if x["circuit"] == 3)
    c3_r2 = next(x for x in r2["readings"] if x["circuit"] == 3)

    # 第一轮超限仅待确认，不开事件。
    assert c3_r1["judge_status"] == JUDGE_OVER_LIMIT
    assert c3_r1["confirm_count"] == 1
    assert c3_r1["event_id"] is None
    # 第二轮连续超限 → 开事件。
    assert c3_r2["judge_status"] == JUDGE_OPENED
    assert c3_r2["confirm_count"] == 2
    assert c3_r2["event_id"] is not None

    events, total = await manager.list_events(task_id, None, None, 100, 0)
    assert total == 1
    event = events[0]
    assert event["circuit"] == 3
    assert event["status"] == EVENT_OPEN
    assert event["opened_round_no"] == 2
    assert event["opened_temp_c"] == 23.5


async def test_non_over_limit_judged_normal_and_no_event(monitor_env):
    manager, _repo, _ = monitor_env
    task = await _start(monitor_env)
    await _wait_round(manager, task["id"], 1)
    detail = await manager.get_round_detail(task["id"], 1)
    for reading in detail["readings"]:
        if reading["circuit"] == 3:
            continue
        assert reading["judge_status"] == JUDGE_NORMAL
        assert reading["confirm_count"] == 0
        assert reading["ok"] == 1
    _events, total = await manager.list_events(task["id"], None, None, 100, 0)
    assert total == 0


async def test_single_over_limit_then_normal_resets_confirmation(monitor_env):
    manager, _repo, _ = monitor_env
    task = await _start(monitor_env)
    task_id = task["id"]
    await _wait_round(manager, task_id, 1)  # 回路 3 第一轮超限待确认

    # 把回路 3 降到上限以下，下一轮成功不超限 → 计数清零，不开事件。
    registers = list(simulator.DEFAULT_REGISTERS)
    registers[3] = 100  # 10.0℃ < 20℃
    simulator.state.registers = registers
    await _wait_round(manager, task_id, 2)

    r2 = await manager.get_round_detail(task_id, 2)
    c3 = next(x for x in r2["readings"] if x["circuit"] == 3)
    assert c3["judge_status"] == JUDGE_NORMAL
    assert c3["confirm_count"] == 0
    _events, total = await manager.list_events(task_id, None, None, 100, 0)
    assert total == 0


# ---------------------------------------------------------------------------
# 缺测：失败重置计数；已开事件只记缺测，绝不解除
# ---------------------------------------------------------------------------

async def test_failure_resets_pending_confirmation(monitor_env):
    manager, _repo, _ = monitor_env
    task = await _start(monitor_env)
    task_id = task["id"]
    await _wait_round(manager, task_id, 1)  # 回路 3 待确认计数=1

    # 对回路 3 注入一次异常响应 → 该轮缺测，计数重置。
    simulator.state.exceptions[3] = [0x02, 1]
    await _wait_round(manager, task_id, 2)
    r2 = await manager.get_round_detail(task_id, 2)
    c3 = next(x for x in r2["readings"] if x["circuit"] == 3)
    assert c3["ok"] == 0
    assert c3["judge_status"] == JUDGE_MISSING
    assert c3["confirm_count"] == 0
    assert c3["error_code"] == "exception_response"

    # 再恢复成功超限：需要重新连续两轮确认，第三轮只能是待确认。
    await _wait_round(manager, task_id, 3)
    r3 = await manager.get_round_detail(task_id, 3)
    c3r3 = next(x for x in r3["readings"] if x["circuit"] == 3)
    assert c3r3["judge_status"] == JUDGE_OVER_LIMIT
    assert c3r3["confirm_count"] == 1
    _events, total = await manager.list_events(task_id, None, None, 100, 0)
    assert total == 0


@pytest.mark.modbus_timeout(0.3)
async def test_missing_while_event_open_does_not_close_it(monitor_env):
    manager, _repo, _ = monitor_env
    task = await _start(monitor_env)
    task_id = task["id"]
    await _wait_round(manager, task_id, 2)  # 回路 3 事件已开启
    events, _ = await manager.list_events(task_id, EVENT_OPEN, 3, 10, 0)
    assert len(events) == 1
    event_id = events[0]["id"]

    # 模拟器对所有应答延迟 1s，超过客户端 0.3s 超时 → 整轮缺测。
    simulator.state.delay_seconds = 1.0
    await _wait_round(manager, task_id, 3)
    await _wait_round(manager, task_id, 4)
    simulator.state.delay_seconds = 0.0

    r4 = await manager.get_round_detail(task_id, 4)
    c3 = next(x for x in r4["readings"] if x["circuit"] == 3)
    assert c3["judge_status"] == JUDGE_MISSING
    assert c3["event_id"] == event_id  # 仍挂在原事件上

    open_events, _ = await manager.list_events(task_id, EVENT_OPEN, None, 10, 0)
    assert [e["id"] for e in open_events] == [event_id]
    # 整轮全部失败时 failure_count=8。
    assert r4["failure_count"] == 8
    assert r4["success_count"] == 0


@pytest.mark.modbus_timeout(0.3)
async def test_whole_round_failure_persists_reasons(monitor_env):
    manager, _repo, _ = monitor_env
    task = await _start(monitor_env)
    task_id = task["id"]
    simulator.state.delay_seconds = 1.0  # 全部超时缺测
    await _wait_round(manager, task_id, 1)
    simulator.state.delay_seconds = 0.0
    detail = await manager.get_round_detail(task_id, 1)
    assert detail["failure_count"] == 8
    for reading in detail["readings"]:
        assert reading["ok"] == 0
        assert reading["error_code"]
        assert reading["error_message"].startswith("采集失败")
        assert reading["judge_status"] == JUDGE_MISSING


# ---------------------------------------------------------------------------
# 恢复：成功不超限才按恢复结束
# ---------------------------------------------------------------------------

async def test_recovery_closes_event_with_recovered_status(monitor_env):
    manager, _repo, _ = monitor_env
    task = await _start(monitor_env)
    task_id = task["id"]
    await _wait_round(manager, task_id, 2)  # 事件开启

    registers = list(simulator.DEFAULT_REGISTERS)
    registers[3] = 120  # 12.0℃ < 20℃
    simulator.state.registers = registers
    await _wait_round(manager, task_id, 3)

    r3 = await manager.get_round_detail(task_id, 3)
    c3 = next(x for x in r3["readings"] if x["circuit"] == 3)
    assert c3["judge_status"] == JUDGE_RECOVERED
    assert c3["temperature_c"] == 12.0

    events, total = await manager.list_events(task_id, None, None, 100, 0)
    assert total == 1
    event = events[0]
    assert event["status"] == EVENT_RECOVERED
    assert event["closed_round_no"] == 3
    assert event["closed_temp_c"] == 12.0
    # 恢复后再次超限需重新两轮确认。
    task_row = await manager.get_task(task_id)
    assert task_row["open_events"] == 0


async def test_missing_does_not_recover_then_normal_recovers(monitor_env):
    manager, _repo, _ = monitor_env
    task = await _start(monitor_env)
    task_id = task["id"]
    await _wait_round(manager, task_id, 2)  # 事件开启

    simulator.state.exceptions[3] = [0x02, 1]
    await _wait_round(manager, task_id, 3)  # 缺测：不恢复
    events, _ = await manager.list_events(task_id, EVENT_OPEN, 3, 10, 0)
    assert len(events) == 1

    registers = list(simulator.DEFAULT_REGISTERS)
    registers[3] = 50  # 5.0℃
    simulator.state.registers = registers
    await _wait_round(manager, task_id, 4)  # 成功不超限：恢复
    events, _ = await manager.list_events(task_id, EVENT_RECOVERED, 3, 10, 0)
    assert len(events) == 1
    assert events[0]["closed_round_no"] == 4


# ---------------------------------------------------------------------------
# 停止：在途轮落库后才停止；未结束事件标中断；重复停止共享收尾
# ---------------------------------------------------------------------------

async def test_stop_waits_for_inflight_round_and_interrupts_event(monitor_env):
    manager, _repo, _ = monitor_env
    task = await _start(monitor_env)
    task_id = task["id"]
    await _wait_round(manager, task_id, 2)  # 回路 3 事件仍开启

    result = await manager.stop(task_id)
    assert result["task"]["status"] == TASK_STOPPED
    # 停止时在途轮已落库；未结束事件在最后一轮上标记中断。
    finalized = result["finalized_round_no"]
    assert finalized >= 2

    events, _ = await manager.list_events(task_id, None, None, 100, 0)
    assert events[0]["status"] == EVENT_INTERRUPTED
    assert events[0]["id"] in result["interrupted_event_ids"]
    # 中断不能记作恢复：closed_temp_c 必须为空。
    assert events[0]["closed_temp_c"] is None
    assert events[0]["closed_round_no"] == finalized

    # 停止后不再产生新轮次。
    last = (await manager.get_task(task_id))["last_round_no"]
    await asyncio.sleep(0.15)
    assert (await manager.get_task(task_id))["last_round_no"] == last


async def test_stop_during_interval_opens_no_new_round(monitor_env):
    """受理停止后不开新轮：停在轮间等待时，轮次不增加，事件直接标中断。"""
    manager, repo, _ = monitor_env
    task = await _start(monitor_env)
    task_id = task["id"]
    await _wait_round(manager, task_id, 1)
    rounds_before = repo.count_rounds(task_id)

    result = await manager.stop(task_id)
    # 没有补采收尾轮：轮次数保持不变。
    assert repo.count_rounds(task_id) == rounds_before
    assert result["finalized_round_no"] == rounds_before

    # 回路 3 在第 2 轮才会开事件；第 1 轮后停止应无残留开启事件，也无误伤中断。
    events, total = await manager.list_events(task_id, None, None, 100, 0)
    assert total == 0
    assert result["interrupted_event_ids"] == []
    # 已落库轮次的判定状态不被停止动作改写。
    detail = await manager.get_round_detail(task_id, 1)
    statuses = {r["circuit"]: r["judge_status"] for r in detail["readings"]}
    assert "interrupted" not in statuses.values()


async def test_repeated_stop_waits_same_finalization(monitor_env):
    manager, _repo, _ = monitor_env
    task = await _start(monitor_env)
    task_id = task["id"]
    await _wait_round(manager, task_id, 1)

    results = await asyncio.gather(
        manager.stop(task_id), manager.stop(task_id)
    )
    assert results[0]["task"]["status"] == TASK_STOPPED
    assert results[1]["task"]["status"] == TASK_STOPPED
    assert results[0]["finalized_round_no"] == results[1]["finalized_round_no"]


async def test_stop_unknown_task_raises(monitor_env):
    manager, _repo, _ = monitor_env
    with pytest.raises(MonitorError) as exc:
        await manager.stop("nonexistent")
    assert exc.value.code == "task_not_found"


async def test_stop_then_restart_allowed_and_old_state_isolated(monitor_env):
    manager, repo, _ = monitor_env
    first = await _start(monitor_env)
    await _wait_round(manager, first["id"], 2)
    await manager.stop(first["id"])

    # 旧任务停止后允许启动新任务；新任务轮次从 1 重新计数。
    second = await _start(monitor_env)
    assert second["id"] != first["id"]
    await _wait_round(manager, second["id"], 1)
    assert (await manager.get_task(second["id"]))["last_round_no"] == 1
    # 旧任务数据不受新任务影响。
    old_task = await manager.get_task(first["id"])
    old_rounds = repo.count_rounds(first["id"])
    assert old_task["status"] == TASK_STOPPED
    assert old_rounds >= 2
    await manager.stop(second["id"])


async def test_old_task_cannot_write_after_runtime_replaced(monitor_env):
    """代际隔离：直接替换运行态后，旧任务协程的落库与收尾不得影响新任务。"""
    manager, repo, _ = monitor_env
    first = await _start(monitor_env)
    await _wait_round(manager, first["id"], 1)
    old_runtime = manager._runtime

    # 模拟异常代际切换：强行摘除旧运行态（新任务尚未/已接管）。
    manager._runtime = None
    # 触发旧任务收尾：因运行态已不属于它，_finalize 提前返回，不写库、不抛异常。
    old_runtime.stop_event.set()
    await asyncio.sleep(0.1)

    # 旧任务在库中保持 running（未被新代际误改），且轮次不增加。
    assert repo.get_task(first["id"])["last_round_no"] == 1
    assert repo.get_task(first["id"])["status"] == "running"

    # 旧协程已自行结束且不再持有运行态；由仓储显式完成它的收尾，
    # 验证未结束事件仍能被正确标记为中断（不记作恢复）。
    open_events = repo.list_events(first["id"], EVENT_OPEN, None, 100, 0)
    interrupted = repo.interrupt_open_events(
        first["id"], 1, "2026-11-12T00:00:00.000+00:00"
    )
    assert {e["id"] for e in interrupted} == {e["id"] for e in open_events}
    assert all(e["closed_temp_c"] is None for e in interrupted)
    repo.update_task_status(first["id"], TASK_STOPPED, "2026-11-12T00:00:00.000+00:00")
    assert repo.get_task(first["id"])["status"] == TASK_STOPPED


async def test_only_one_running_task_rejected(monitor_env):
    manager, _repo, _ = monitor_env
    first = await _start(monitor_env)
    with pytest.raises(MonitorError) as exc:
        await _start(monitor_env)
    assert exc.value.code == "task_conflict"
    # 停止中也拒绝启动。
    await _wait_round(manager, first["id"], 1)
    stop_task = asyncio.create_task(manager.stop(first["id"]))
    await asyncio.sleep(0)  # 让停止请求先把状态切到 stopping
    with pytest.raises(MonitorError):
        await _start(monitor_env)
    await stop_task


async def test_invalid_thresholds_rejected(monitor_env):
    manager, _repo, _ = monitor_env
    # 超出允许范围。
    with pytest.raises(MonitorError) as exc:
        await manager.start([0.0] * 7 + [999.0])
    assert exc.value.code == "invalid_thresholds"
    # 回路数量不对。
    with pytest.raises(MonitorError) as exc:
        await manager.start([0.0] * 7)
    assert exc.value.code == "invalid_thresholds"
    # 被拒绝后没有留下任务，可正常启动一个合法任务。
    assert manager.current_task_id is None
    task = await _start(monitor_env)
    assert manager.current_task_id == task["id"]
    await manager.stop(task["id"])


# ---------------------------------------------------------------------------
# 节奏与并发：每轮结束后等待；8 回路并发复用共享客户端
# ---------------------------------------------------------------------------

async def test_rounds_are_spaced_by_interval(monitor_env):
    manager, _repo, _ = monitor_env
    task = await _start(monitor_env)
    task_id = task["id"]
    await _wait_round(manager, task_id, 3)
    rows, _ = await manager.list_rounds(task_id, 10, 0)
    rows = sorted(rows, key=lambda r: r["round_no"])
    # interval=0.05s：相邻轮起始时间差应体现轮间等待（>0.03s）。
    for prev, nxt in zip(rows, rows[1:]):
        from datetime import datetime

        t0 = datetime.fromisoformat(prev["started_at"])
        t1 = datetime.fromisoformat(nxt["started_at"])
        assert (t1 - t0).total_seconds() > 0.03


async def test_round_uses_shared_client_concurrently(monitor_env):
    manager, _repo, client = monitor_env
    # 8 笔读取在同一连接流水线：模拟器侧连接数始终为 1。
    await asyncio.sleep(0.02)
    baseline = simulator.state.connections
    task = await _start(monitor_env)
    await _wait_round(manager, task["id"], 2)
    assert simulator.state.connections <= baseline + 1
    await manager.stop(task["id"])


async def test_persisted_state_consistent_in_one_transaction(monitor_env):
    manager, repo, _ = monitor_env
    task = await _start(monitor_env)
    task_id = task["id"]
    await _wait_round(manager, task_id, 2)
    # 每一轮都恰好有 8 条读数，轮次与任务计数同步提交。
    for round_no in (1, 2):
        readings = repo.list_readings_for_round(task_id, round_no)
        assert len(readings) == 8
        assert {r["circuit"] for r in readings} == set(range(8))
    task_row = repo.get_task(task_id)
    assert task_row["total_rounds"] == 2
    assert task_row["last_round_no"] == 2
    await manager.stop(task_id)
    assert repo.get_task(task_id)["status"] == TASK_STOPPED
