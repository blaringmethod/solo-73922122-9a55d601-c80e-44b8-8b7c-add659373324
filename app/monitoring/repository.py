"""监测任务的 SQLite 持久化层。

设计要点
========
* 同步 ``sqlite3`` 实现，所有访问经一把 :class:`threading.RLock` 串行化，
  调用方通过 ``asyncio.to_thread`` 在线程中执行，不阻塞事件循环。
* 开启 WAL：写入事务与查询互不阻塞，同一时刻的查询只能看到已完整提交的状态，
  保证“查询返回同一已提交状态”。
* 每一轮采样（整轮读数/失败原因、判定状态、事件开启/恢复）在**同一个事务**
  中提交，不会出现只写了一半的轮次。
* 表结构：

  - ``monitor_tasks``   任务（提交的 8 个上限、运行状态、轮次计数）
  - ``sample_rounds``   每一轮：成功/失败计数、起止时间、是否收尾轮
  - ``readings``        每回路每轮：整轮读数或失败原因 + 判定状态
  - ``events``          超温事件：开启/恢复/中断，关联起止轮次
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---- 判定 / 事件状态常量（同时作为入库字符串） ------------------------------

# 单回路单轮判定
JUDGE_OPENED = "opened"          # 本轮成功超限，且连续两轮确认 → 新开事件
JUDGE_OVER_LIMIT = "over_limit"  # 成功超限（待确认中）
JUDGE_NORMAL = "normal"          # 成功且未超限
JUDGE_RECOVERED = "recovered"    # 成功未超限，按恢复结束了已开事件
JUDGE_MISSING = "missing"        # 采集失败：重置待确认；已开事件只记缺测

# 事件生命周期
EVENT_OPEN = "open"
EVENT_RECOVERED = "recovered"
EVENT_INTERRUPTED = "interrupted"

# 任务状态
TASK_RUNNING = "running"
TASK_STOPPING = "stopping"
TASK_STOPPED = "stopped"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS monitor_tasks (
    id               TEXT PRIMARY KEY,
    thresholds_c     TEXT NOT NULL,          -- JSON: 8 个回路的温度上限(℃)
    status           TEXT NOT NULL,          -- running | stopping | stopped
    created_at       TEXT NOT NULL,
    started_at       TEXT NOT NULL,
    stopped_at       TEXT,
    last_round_no    INTEGER NOT NULL DEFAULT 0,
    total_rounds     INTEGER NOT NULL DEFAULT 0,
    open_events      INTEGER NOT NULL DEFAULT 0,
    interrupted_events INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS sample_rounds (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id        TEXT NOT NULL REFERENCES monitor_tasks(id),
    round_no       INTEGER NOT NULL,
    started_at     TEXT NOT NULL,
    finished_at    TEXT NOT NULL,
    success_count  INTEGER NOT NULL,
    failure_count  INTEGER NOT NULL,
    UNIQUE(task_id, round_no)
);

CREATE TABLE IF NOT EXISTS readings (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id          TEXT NOT NULL REFERENCES monitor_tasks(id),
    round_id         INTEGER NOT NULL REFERENCES sample_rounds(id),
    round_no         INTEGER NOT NULL,
    circuit          INTEGER NOT NULL,
    ok               INTEGER NOT NULL,         -- 1 成功 / 0 失败（缺测）
    temperature_c    REAL,
    raw_register     INTEGER,
    threshold_c      REAL NOT NULL,
    error_code       TEXT,
    error_message    TEXT,
    judge_status     TEXT NOT NULL,            -- opened/over_limit/normal/
                                                -- recovered/missing/interrupted
    confirm_count    INTEGER NOT NULL,         -- 本轮判定后的连续确认计数
    event_id         INTEGER REFERENCES events(id),
    sampled_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id         TEXT NOT NULL REFERENCES monitor_tasks(id),
    circuit         INTEGER NOT NULL,
    threshold_c     REAL NOT NULL,
    status          TEXT NOT NULL,             -- open | recovered | interrupted
    opened_round_no INTEGER NOT NULL,
    opened_at       TEXT NOT NULL,
    opened_temp_c   REAL NOT NULL,
    closed_round_no INTEGER,
    closed_at       TEXT,
    closed_temp_c   REAL
);

CREATE INDEX IF NOT EXISTS idx_rounds_task ON sample_rounds(task_id, round_no);
CREATE INDEX IF NOT EXISTS idx_readings_task_round ON readings(task_id, round_no);
CREATE INDEX IF NOT EXISTS idx_events_task ON events(task_id, id);
"""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class MonitoringRepository:
    """线程安全的 SQLite 仓储；方法均为同步，请在线程中调用。"""

    def __init__(self, db_path: str) -> None:
        self._db_path = self._ensure_writable_path(db_path)
        self._lock = threading.RLock()
        parent = Path(self._db_path).parent
        if str(parent) not in ("", "."):
            parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            self._db_path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @staticmethod
    def _ensure_writable_path(db_path: str) -> str:
        """目标父目录不可写时（如本地无 /data 权限）回退到用户缓存目录。

        容器内 ``/data`` 由 compose 挂载卷保证可写，不会触发回退。
        """
        parent = Path(db_path).parent
        try:
            parent.mkdir(parents=True, exist_ok=True)
            probe = parent / ".write_probe"
            probe.touch()
            probe.unlink()
            return db_path
        except OSError:
            fallback_dir = Path.home() / ".cache" / "coldchain"
            fallback_dir.mkdir(parents=True, exist_ok=True)
            return str(fallback_dir / Path(db_path).name)

    # ------------------------------------------------------------------
    # 任务
    # ------------------------------------------------------------------

    def insert_task(self, task: dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO monitor_tasks"
                "(id, thresholds_c, status, created_at, started_at)"
                " VALUES (?, ?, ?, ?, ?)",
                (
                    task["id"],
                    json.dumps(task["thresholds_c"]),
                    task["status"],
                    task["created_at"],
                    task["started_at"],
                ),
            )

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM monitor_tasks WHERE id = ?", (task_id,)
            ).fetchone()
        return self._task_row_to_dict(row) if row is not None else None

    def list_tasks(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM monitor_tasks ORDER BY created_at DESC, id DESC"
            ).fetchall()
        return [self._task_row_to_dict(row) for row in rows]

    def update_task_status(
        self, task_id: str, status: str, stopped_at: str | None = None
    ) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE monitor_tasks SET status = ?,"
                " stopped_at = COALESCE(?, stopped_at) WHERE id = ?",
                (status, stopped_at, task_id),
            )

    # ------------------------------------------------------------------
    # 整轮落库（单事务）
    # ------------------------------------------------------------------

    def persist_round(
        self,
        task_id: str,
        round_no: int,
        started_at: str,
        finished_at: str,
        readings: list[dict[str, Any]],
        opened_events: list[dict[str, Any]],
        closed_events: list[dict[str, Any]],
        open_event_count: int,
        interrupted_event_count: int,
        runtime_circuits: list[Any] | None = None,
    ) -> int:
        """一轮的全部读数与事件变更在一个事务中原子提交。

        ``opened_events`` 中的字典会被原地写入数据库自增 ``id``；
        若提供 ``runtime_circuits``（按回路下标排列的内存状态），
        同时回填其 ``open_event`` 快照，保证内存与库一致。
        """
        with self._lock, self._conn:  # 自动 BEGIN/COMMIT
            round_id = self._conn.execute(
                "INSERT INTO sample_rounds"
                "(task_id, round_no, started_at, finished_at,"
                " success_count, failure_count)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    round_no,
                    started_at,
                    finished_at,
                    sum(1 for r in readings if r["ok"]),
                    sum(1 for r in readings if not r["ok"]),
                ),
            ).lastrowid

            # 先写本轮新开的事件，拿到自增 id 后再回填读数与内存状态。
            event_id_by_circuit: dict[int, int] = {}
            for event in opened_events:
                ev_cur = self._conn.execute(
                    "INSERT INTO events"
                    "(task_id, circuit, threshold_c, status, opened_round_no,"
                    " opened_at, opened_temp_c)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        task_id,
                        event["circuit"],
                        event["threshold_c"],
                        EVENT_OPEN,
                        round_no,
                        event["opened_at"],
                        event["opened_temp_c"],
                    ),
                )
                event_id = ev_cur.lastrowid
                event["id"] = event_id
                event_id_by_circuit[event["circuit"]] = event_id
                if runtime_circuits is not None:
                    circuit_state = runtime_circuits[event["circuit"]]
                    circuit_state.open_event = {
                        "id": event_id,
                        "task_id": task_id,
                        "circuit": event["circuit"],
                        "threshold_c": event["threshold_c"],
                        "status": EVENT_OPEN,
                        "opened_round_no": round_no,
                        "opened_at": event["opened_at"],
                        "opened_temp_c": event["opened_temp_c"],
                        "closed_round_no": None,
                        "closed_at": None,
                        "closed_temp_c": None,
                    }

            # 恢复 / 中断关闭事件。
            for event in closed_events:
                self._conn.execute(
                    "UPDATE events SET status = ?, closed_round_no = ?,"
                    " closed_at = ?, closed_temp_c = ? WHERE id = ? AND task_id = ?",
                    (
                        event["status"],
                        round_no,
                        event["closed_at"],
                        event["closed_temp_c"],
                        event["event_id"],
                        task_id,
                    ),
                )

            for reading in readings:
                event_id = reading.get("event_id")
                if event_id is None and reading.get("new_event_ref") is not None:
                    event_id = event_id_by_circuit[reading["new_event_ref"]]
                self._conn.execute(
                    "INSERT INTO readings"
                    "(task_id, round_id, round_no, circuit, ok, temperature_c,"
                    " raw_register, threshold_c, error_code, error_message,"
                    " judge_status, confirm_count, event_id, sampled_at)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        task_id,
                        round_id,
                        round_no,
                        reading["circuit"],
                        1 if reading["ok"] else 0,
                        reading.get("temperature_c"),
                        reading.get("raw_register"),
                        reading["threshold_c"],
                        reading.get("error_code"),
                        reading.get("error_message"),
                        reading["judge_status"],
                        reading["confirm_count"],
                        event_id,
                        reading["sampled_at"],
                    ),
                )

            self._conn.execute(
                "UPDATE monitor_tasks SET last_round_no = ?, total_rounds = ?,"
                " open_events = ?, interrupted_events = ? WHERE id = ?",
                (
                    round_no,
                    round_no,
                    open_event_count,
                    interrupted_event_count,
                    task_id,
                ),
            )

    def interrupt_open_events(
        self, task_id: str, round_no: int, closed_at: str
    ) -> list[dict[str, Any]]:
        """停止/重启收尾：把仍未结束的事件全部标记为中断（不得记为恢复）。

        返回被中断的事件列表，供调用方同步内存状态。
        """
        with self._lock, self._conn:
            rows = self._conn.execute(
                "SELECT * FROM events WHERE task_id = ? AND status = ?",
                (task_id, EVENT_OPEN),
            ).fetchall()
            for row in rows:
                self._conn.execute(
                    "UPDATE events SET status = ?, closed_round_no = ?,"
                    " closed_at = ?, closed_temp_c = NULL WHERE id = ?",
                    (EVENT_INTERRUPTED, round_no, closed_at, row["id"]),
                )
            self._conn.execute(
                "UPDATE monitor_tasks SET open_events = 0,"
                " interrupted_events = interrupted_events + ? WHERE id = ?",
                (len(rows), task_id),
            )
            return [dict(row) for row in rows]

    def recover_after_restart(self, recovered_at: str) -> list[dict[str, Any]]:
        """进程重启恢复：把上次未正常停止的任务（running/stopping）收尾。

        * 未结束（open）的事件一律标记为 interrupted，绝不记作恢复；
        * 任务状态置为 stopped，记录 stopped_at；
        * 内存中不存在旧任务的运行态，保证“旧任务不得写入新任务”。

        返回每个被恢复任务的摘要字典。
        """
        summaries: list[dict[str, Any]] = []
        with self._lock, self._conn:
            stale_tasks = self._conn.execute(
                "SELECT id, last_round_no FROM monitor_tasks"
                " WHERE status IN (?, ?)",
                (TASK_RUNNING, TASK_STOPPING),
            ).fetchall()
            for task_row in stale_tasks:
                task_id = task_row["id"]
                round_no = task_row["last_round_no"]
                open_rows = self._conn.execute(
                    "SELECT * FROM events WHERE task_id = ? AND status = ?",
                    (task_id, EVENT_OPEN),
                ).fetchall()
                for row in open_rows:
                    self._conn.execute(
                        "UPDATE events SET status = ?, closed_round_no = ?,"
                        " closed_at = ?, closed_temp_c = NULL WHERE id = ?",
                        (EVENT_INTERRUPTED, round_no, recovered_at, row["id"]),
                    )
                self._conn.execute(
                    "UPDATE monitor_tasks SET status = ?, stopped_at = ?,"
                    " open_events = 0,"
                    " interrupted_events = interrupted_events + ? WHERE id = ?",
                    (
                        TASK_STOPPED,
                        recovered_at,
                        len(open_rows),
                        task_id,
                    ),
                )
                summaries.append(
                    {
                        "task_id": task_id,
                        "interrupted_event_ids": [row["id"] for row in open_rows],
                    }
                )
        return summaries

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def list_rounds(
        self, task_id: str, limit: int, offset: int
    ) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM sample_rounds WHERE task_id = ?"
                " ORDER BY round_no DESC LIMIT ? OFFSET ?",
                (task_id, limit, offset),
            ).fetchall()
        return [dict(row) for row in rows]

    def count_rounds(self, task_id: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM sample_rounds WHERE task_id = ?",
                (task_id,),
            ).fetchone()
        return int(row["n"])

    def get_round(self, task_id: str, round_no: int) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM sample_rounds WHERE task_id = ? AND round_no = ?",
                (task_id, round_no),
            ).fetchone()
        return dict(row) if row is not None else None

    def list_readings_for_round(
        self, task_id: str, round_no: int
    ) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM readings WHERE task_id = ? AND round_no = ?"
                " ORDER BY circuit ASC",
                (task_id, round_no),
            ).fetchall()
        return [dict(row) for row in rows]

    def list_events(
        self,
        task_id: str,
        status: str | None = None,
        circuit: int | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM events WHERE task_id = ?"
        params: list[Any] = [task_id]
        if status is not None:
            sql += " AND status = ?"
            params.append(status)
        if circuit is not None:
            sql += " AND circuit = ?"
            params.append(circuit)
        sql += " ORDER BY id ASC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def count_events(
        self, task_id: str, status: str | None, circuit: int | None
    ) -> int:
        sql = "SELECT COUNT(*) AS n FROM events WHERE task_id = ?"
        params: list[Any] = [task_id]
        if status is not None:
            sql += " AND status = ?"
            params.append(status)
        if circuit is not None:
            sql += " AND circuit = ?"
            params.append(circuit)
        with self._lock:
            row = self._conn.execute(sql, params).fetchone()
        return int(row["n"])

    # ------------------------------------------------------------------

    @staticmethod
    def _task_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
        data = dict(row)
        data["thresholds_c"] = json.loads(data["thresholds_c"])
        return data
