"""SQLite 持久化与进程重启恢复测试。"""

from __future__ import annotations

from app.monitoring.repository import (
    EVENT_INTERRUPTED,
    EVENT_RECOVERED,
    TASK_RUNNING,
    TASK_STOPPED,
    MonitoringRepository,
    utc_now_iso,
)


def _make_repo(path: str) -> MonitoringRepository:
    return MonitoringRepository(path)


def _seed_running_task_with_open_event(
    repo: MonitoringRepository, task_id: str = "task-crash"
) -> int:
    repo.insert_task(
        {
            "id": task_id,
            "thresholds_c": [0.0] * 8,
            "status": TASK_RUNNING,
            "created_at": utc_now_iso(),
            "started_at": utc_now_iso(),
        }
    )
    # 直接落一轮含一个开启事件的采样。
    readings = [
        {
            "circuit": i,
            "ok": True,
            "temperature_c": 23.5 if i == 3 else 1.0,
            "raw_register": 235 if i == 3 else 10,
            "threshold_c": 20.0 if i == 3 else 0.0,
            "error_code": None,
            "error_message": None,
            "judge_status": "opened" if i == 3 else "normal",
            "confirm_count": 2 if i == 3 else 0,
            "event_id": None,
            "new_event_ref": 3 if i == 3 else None,
            "sampled_at": utc_now_iso(),
        }
        for i in range(8)
    ]
    opened = [
        {
            "circuit": 3,
            "threshold_c": 20.0,
            "opened_at": utc_now_iso(),
            "opened_temp_c": 23.5,
        }
    ]
    repo.persist_round(
        task_id=task_id,
        round_no=1,
        started_at=utc_now_iso(),
        finished_at=utc_now_iso(),
        readings=readings,
        opened_events=opened,
        closed_events=[],
        open_event_count=1,
        interrupted_event_count=0,
    )
    return opened[0]["id"]


def test_restart_marks_stale_task_stopped_and_events_interrupted(tmp_path):
    db = str(tmp_path / "monitor.db")
    repo = _make_repo(db)
    event_id = _seed_running_task_with_open_event(repo)
    # 模拟进程直接退出（未调用优雅停止）。
    repo.close()

    # 新进程重新打开同一数据库并执行恢复。
    repo2 = _make_repo(db)
    recovered = repo2.recover_after_restart(utc_now_iso())
    assert len(recovered) == 1
    assert recovered[0]["interrupted_event_ids"] == [event_id]

    task = repo2.get_task("task-crash")
    assert task["status"] == TASK_STOPPED
    assert task["stopped_at"] is not None
    assert task["open_events"] == 0
    assert task["interrupted_events"] == 1

    events = repo2.list_events("task-crash", None, None, 100, 0)
    assert len(events) == 1
    event = events[0]
    assert event["status"] == EVENT_INTERRUPTED
    # 中断不能记作恢复，且没有收尾温度。
    assert event["closed_temp_c"] is None
    assert event["closed_round_no"] == 1
    repo2.close()


def test_recover_is_idempotent_and_skips_stopped_tasks(tmp_path):
    db = str(tmp_path / "monitor2.db")
    repo = _make_repo(db)
    _seed_running_task_with_open_event(repo)
    first = repo.recover_after_restart(utc_now_iso())
    assert len(first) == 1
    # 再次恢复：已 stopped 的任务不再处理。
    second = repo.recover_after_restart(utc_now_iso())
    assert second == []
    # 中断计数不被重复累加。
    task = repo.get_task("task-crash")
    assert task["interrupted_events"] == 1
    repo.close()


def test_recovered_events_are_not_rewritten_as_recovery(tmp_path):
    db = str(tmp_path / "monitor3.db")
    repo = _make_repo(db)
    event_id = _seed_running_task_with_open_event(repo, "t1")
    repo.recover_after_restart(utc_now_iso())
    # 历史事件终态只能是 interrupted，绝不出现 recovered。
    recovered = repo.list_events("t1", EVENT_RECOVERED, None, 100, 0)
    interrupted = repo.list_events("t1", EVENT_INTERRUPTED, None, 100, 0)
    assert recovered == []
    assert [e["id"] for e in interrupted] == [event_id]
    repo.close()


def test_round_and_readings_query_reflect_committed_state(tmp_path):
    db = str(tmp_path / "monitor4.db")
    repo = _make_repo(db)
    _seed_running_task_with_open_event(repo, "tq")
    # 整轮 8 条读数同事务可见。
    assert repo.count_rounds("tq") == 1
    round_row = repo.get_round("tq", 1)
    assert round_row["success_count"] == 8
    assert round_row["failure_count"] == 0
    readings = repo.list_readings_for_round("tq", 1)
    assert len(readings) == 8
    opened = next(r for r in readings if r["circuit"] == 3)
    assert opened["judge_status"] == "opened"
    assert opened["event_id"] is not None
    # 不存在的轮次返回 None。
    assert repo.get_round("tq", 99) is None
    repo.close()
