"""持续监测 HTTP API 测试（ASGI 进程内调用，覆盖启停与按任务查询）。"""

from __future__ import annotations

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from app import main as web
from app.modbus_client import ModbusTcpClient
from app.monitoring.manager import MonitorManager
from app.monitoring.repository import MonitoringRepository
from simulator import app as simulator

THRESHOLDS = [0.0, 0.0, 10.0, 20.0, 0.0, 40.0, 0.0, 20.0]


@pytest.fixture
async def monitor_api(tmp_path, simulator_server, modbus_port):
    """把监测管理器替换为指向测试模拟器与临时库的实例。"""
    original_manager = web.monitor_manager
    test_client = ModbusTcpClient(
        host="127.0.0.1", port=modbus_port, unit_id=1, timeout=2.0
    )
    repo = MonitoringRepository(str(tmp_path / "api_monitor.db"))
    manager = MonitorManager(
        client_provider=lambda: test_client,
        repository=repo,
        circuit_count=8,
        interval_seconds=0.05,
    )
    web.app.state.monitor_manager = manager
    transport = ASGITransport(app=web.app)
    try:
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac, manager
    finally:
        await manager.shutdown()
        web.app.state.monitor_manager = original_manager
        repo.close()
        await test_client.aclose()


async def _wait_rounds(ac: AsyncClient, task_id: str, minimum: int) -> None:
    for _ in range(200):
        r = await ac.get(f"/monitor/tasks/{task_id}")
        if r.json()["last_round_no"] >= minimum:
            return
        await asyncio.sleep(0.02)
    raise AssertionError("轮次未在超时内落库")


async def test_start_creates_single_task_and_returns_entry_urls(monitor_api):
    ac, _ = monitor_api
    r = await ac.post("/monitor/tasks", json={"thresholds_c": THRESHOLDS})
    assert r.status_code == 201
    body = r.json()
    task = body["task"]
    assert task["status"] == "running"
    assert task["thresholds_c"] == THRESHOLDS
    assert task["last_round_no"] == 0
    assert body["rounds_url"].endswith("/rounds")
    assert body["events_url"].endswith("/events")
    task_id = task["id"]

    # 单进程只允许一个任务：第二次启动被 409 拒绝。
    r2 = await ac.post("/monitor/tasks", json={"thresholds_c": THRESHOLDS})
    assert r2.status_code == 409
    assert r2.json()["error"] == "task_conflict"

    r3 = await ac.get("/monitor/tasks")
    assert len(r3.json()) == 1
    await ac.post(f"/monitor/tasks/{task_id}/stop")


async def test_start_validates_threshold_count(monitor_api):
    ac, _ = monitor_api
    r = await ac.post("/monitor/tasks", json={"thresholds_c": [0.0] * 7})
    # Pydantic 长度校验返回 422。
    assert r.status_code == 422


async def test_full_lifecycle_rounds_events_and_stop(monitor_api):
    ac, _ = monitor_api
    started = await ac.post("/monitor/tasks", json={"thresholds_c": THRESHOLDS})
    task_id = started.json()["task"]["id"]
    await _wait_rounds(ac, task_id, 2)  # 回路 3 两轮确认后开事件

    # 轮次列表分页。
    r = await ac.get(f"/monitor/tasks/{task_id}/rounds?limit=1&offset=0")
    assert r.status_code == 200
    rounds_body = r.json()
    assert rounds_body["total"] >= 2
    assert len(rounds_body["items"]) == 1
    assert "is_finalizing" not in rounds_body["items"][0]

    # 单轮详情：整轮 8 回路读数 + 判定。
    r = await ac.get(f"/monitor/tasks/{task_id}/rounds/2")
    assert r.status_code == 200
    round_body = r.json()
    assert len(round_body["readings"]) == 8
    c3 = next(x for x in round_body["readings"] if x["circuit"] == 3)
    assert c3["judge_status"] == "opened"
    assert c3["temperature_c"] == 23.5
    assert c3["event_id"] is not None

    # 事件查询：open 状态、按回路过滤。
    r = await ac.get(f"/monitor/tasks/{task_id}/events?status=open&circuit=3")
    events_body = r.json()
    assert events_body["total"] == 1
    event = events_body["items"][0]
    assert event["status"] == "open"
    assert event["opened_round_no"] == 2

    # 停止：在途轮落库后完成，未结束事件标中断（非恢复）。
    r = await ac.post(f"/monitor/tasks/{task_id}/stop")
    assert r.status_code == 200
    stop_body = r.json()
    assert stop_body["task"]["status"] == "stopped"
    assert stop_body["task"]["stopped_at"]
    assert event["id"] in stop_body["interrupted_event_ids"]

    r = await ac.get(f"/monitor/tasks/{task_id}/events")
    final_event = r.json()["items"][0]
    assert final_event["status"] == "interrupted"
    assert final_event["closed_temp_c"] is None

    # 停止后可启动新任务，旧任务数据仍可查询（同一已提交状态）。
    second = await ac.post("/monitor/tasks", json={"thresholds_c": THRESHOLDS})
    assert second.status_code == 201
    second_id = second.json()["task"]["id"]
    await _wait_rounds(ac, second_id, 1)
    old = await ac.get(f"/monitor/tasks/{task_id}/rounds")
    assert old.json()["total"] >= 2
    await ac.post(f"/monitor/tasks/{second_id}/stop")


async def test_duplicate_stop_waits_same_finalization(monitor_api):
    ac, _ = monitor_api
    started = await ac.post("/monitor/tasks", json={"thresholds_c": THRESHOLDS})
    task_id = started.json()["task"]["id"]
    await _wait_rounds(ac, task_id, 1)

    results = await asyncio.gather(
        ac.post(f"/monitor/tasks/{task_id}/stop"),
        ac.post(f"/monitor/tasks/{task_id}/stop"),
    )
    assert all(r.status_code == 200 for r in results)
    finals = [r.json()["finalized_round_no"] for r in results]
    assert finals[0] == finals[1]


async def test_stop_unknown_and_query_unknown_return_404(monitor_api):
    ac, _ = monitor_api
    r = await ac.post("/monitor/tasks/does-not-exist/stop")
    assert r.status_code == 404
    assert r.json()["error"] == "task_not_found"

    r = await ac.get("/monitor/tasks/does-not-exist")
    assert r.status_code == 404

    r = await ac.get("/monitor/tasks/does-not-exist/rounds")
    assert r.status_code == 404


async def test_invalid_event_filter_returns_400(monitor_api):
    ac, _ = monitor_api
    started = await ac.post("/monitor/tasks", json={"thresholds_c": THRESHOLDS})
    task_id = started.json()["task"]["id"]
    r = await ac.get(f"/monitor/tasks/{task_id}/events?status=bogus")
    assert r.status_code == 400
    assert r.json()["error"] == "invalid_status"
    await ac.post(f"/monitor/tasks/{task_id}/stop")


async def test_missing_round_recorded_via_api(monitor_api):
    ac, _ = monitor_api
    started = await ac.post("/monitor/tasks", json={"thresholds_c": THRESHOLDS})
    task_id = started.json()["task"]["id"]
    # 对回路 5 注入一次异常响应。
    simulator.state.exceptions[5] = [0x02, 1]
    await _wait_rounds(ac, task_id, 1)
    r = await ac.get(f"/monitor/tasks/{task_id}/rounds/1")
    c5 = next(x for x in r.json()["readings"] if x["circuit"] == 5)
    assert c5["ok"] is False
    assert c5["judge_status"] == "missing"
    assert c5["error_code"] == "exception_response"
    assert c5["temperature_c"] is None
    await ac.post(f"/monitor/tasks/{task_id}/stop")
