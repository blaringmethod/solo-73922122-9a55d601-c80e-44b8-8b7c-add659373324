"""采集服务配置。控制器地址由环境变量固定，进程启动后不再变化。"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env_str(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw in (None, ""):
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw in (None, ""):
        return default
    try:
        return float(raw)
    except ValueError:
        return default


@dataclass(frozen=True, slots=True)
class Settings:
    modbus_host: str
    modbus_port: int
    modbus_unit_id: int
    modbus_timeout: float
    register_count: int  # 回路数量，对应地址 0 ~ register_count-1
    monitor_db_path: str  # 监测任务 SQLite 数据库文件路径
    monitor_interval_seconds: float  # 每轮采集结束后到下一轮的等待秒数


def get_settings() -> Settings:
    return Settings(
        modbus_host=_env_str("MODBUS_HOST", "simulator"),
        modbus_port=_env_int("MODBUS_PORT", 5020),
        modbus_unit_id=_env_int("MODBUS_UNIT_ID", 1),
        modbus_timeout=_env_float("MODBUS_TIMEOUT_SECONDS", 2.0),
        register_count=_env_int("REGISTER_COUNT", 8),
        monitor_db_path=_env_str("MONITOR_DB_PATH", "/data/monitoring.db"),
        monitor_interval_seconds=_env_float("MONITOR_INTERVAL_SECONDS", 1.0),
    )
