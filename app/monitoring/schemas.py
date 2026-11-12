"""监测 API 的请求 / 响应模型。"""

from __future__ import annotations

from pydantic import BaseModel, Field

# 判定 / 事件 / 任务状态常量（字符串值直接入库与出参）。
EVENT_INTERRUPTED = "interrupted"
EVENT_OPEN = "open"
EVENT_RECOVERED = "recovered"
JUDGE_MISSING = "missing"
JUDGE_NORMAL = "normal"
JUDGE_OPENED = "opened"
JUDGE_OVER_LIMIT = "over_limit"
JUDGE_RECOVERED = "recovered"
TASK_RUNNING = "running"
TASK_STOPPED = "stopped"
TASK_STOPPING = "stopping"

# 温度上限允许范围（℃）：覆盖冷库常见负温到常温故障区间。
THRESHOLD_MIN = -100.0
THRESHOLD_MAX = 200.0


class StartMonitorRequest(BaseModel):
    """提交 8 个回路各自的温度上限（摄氏温度）。"""

    thresholds_c: list[float] = Field(
        ...,
        min_length=8,
        max_length=8,
        description=(
            "8 个回路各自的温度上限（℃），下标即回路/寄存器编号 0~7；"
            "严格大于上限判为超限"
        ),
    )


class TaskInfo(BaseModel):
    id: str = Field(..., description="任务 ID（UUID4）")
    thresholds_c: list[float] = Field(..., description="提交时的 8 个温度上限")
    status: str = Field(
        ..., description="任务状态：running（采样中）/ stopping（收尾中）/ stopped（已停止）"
    )
    created_at: str
    started_at: str
    stopped_at: str | None = Field(None, description="停止完成（在途轮次落库）时间")
    last_round_no: int = Field(..., description="已落库的最后一轮轮次编号（从 1 开始）")
    total_rounds: int
    open_events: int = Field(..., description="当前仍开启的超温事件数")
    interrupted_events: int = Field(..., description="累计被中断的事件数")


class StartMonitorResponse(BaseModel):
    task: TaskInfo
    rounds_url: str = Field(..., description="按任务查询采样轮次的入口")
    events_url: str = Field(..., description="按任务查询事件的入口")


class ReadingModel(BaseModel):
    circuit: int
    ok: bool = Field(..., description="该回路本轮是否采集成功")
    temperature_c: float | None = Field(None, description="成功时的摄氏温度")
    raw_register: int | None = Field(None, description="成功时的寄存器原始有符号值")
    threshold_c: float = Field(..., description="该回路提交的温度上限")
    error_code: str | None = Field(None, description="失败原因类别（缺测时）")
    error_message: str | None = Field(None, description="失败中文说明（缺测时）")
    judge_status: str = Field(
        ...,
        description=(
            "判定状态：opened=本轮确认超限并新开事件；over_limit=成功超限待确认；"
            "normal=成功未超限；recovered=成功未超限并按恢复结束事件；"
            "missing=采集失败（缺测，重置确认计数，已开事件不解除）"
        ),
    )
    confirm_count: int = Field(..., description="本轮判定后的连续确认计数")
    event_id: int | None = Field(None, description="关联的超温事件 ID（如有）")
    sampled_at: str


class RoundModel(BaseModel):
    round_no: int
    started_at: str
    finished_at: str
    success_count: int
    failure_count: int
    readings: list[ReadingModel] = Field(..., description="该轮 8 个回路的读数/失败与判定")


class RoundSummary(BaseModel):
    round_no: int
    started_at: str
    finished_at: str
    success_count: int
    failure_count: int


class RoundsResponse(BaseModel):
    task_id: str
    limit: int
    offset: int
    total: int
    items: list[RoundSummary]


class EventModel(BaseModel):
    id: int
    task_id: str
    circuit: int
    threshold_c: float
    status: str = Field(
        ..., description="open=仍开启；recovered=温度恢复后结束；interrupted=停止/重启中断"
    )
    opened_round_no: int
    opened_at: str
    opened_temp_c: float
    closed_round_no: int | None
    closed_at: str | None
    closed_temp_c: float | None = Field(
        None, description="恢复结束时的温度；中断结束为 null"
    )


class EventsResponse(BaseModel):
    task_id: str
    limit: int
    offset: int
    total: int
    items: list[EventModel]


class StopResponse(BaseModel):
    task: TaskInfo
    finalized_round_no: int | None = Field(
        ..., description="停止时完成落库的收尾轮轮次（无在途轮时为最后一轮编号）"
    )
    interrupted_event_ids: list[int] = Field(
        ..., description="本次停止被标记为中断的事件 ID"
    )
