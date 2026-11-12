"""持续监测 API 路由。

入口（均在 /docs 可见）：

* ``POST /monitor/tasks``           提交 8 个上限并启动唯一监测任务
* ``GET  /monitor/tasks``           任务列表
* ``GET  /monitor/tasks/{id}``      按任务查询状态
* ``POST /monitor/tasks/{id}/stop`` 受理停止（等在途轮落库，重复停止共享收尾）
* ``GET  /monitor/tasks/{id}/rounds``        按任务查询采样轮次（分页）
* ``GET  /monitor/tasks/{id}/rounds/{no}``   单轮整轮读数/失败原因与判定
* ``GET  /monitor/tasks/{id}/events``        按任务查询超温事件
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

from app.monitoring.manager import MonitorError
from app.monitoring.schemas import (
    EventModel,
    EventsResponse,
    ReadingModel,
    RoundModel,
    RoundSummary,
    RoundsResponse,
    StartMonitorRequest,
    StartMonitorResponse,
    StopResponse,
    TaskInfo,
)

router = APIRouter(prefix="/monitor", tags=["持续监测"])

# 允许的事件过滤值与库内状态一致。
_EVENT_STATUS_VALUES = {"open", "recovered", "interrupted"}


def _manager(request: Request):
    return request.app.state.monitor_manager


def _task_info(row: dict) -> TaskInfo:
    return TaskInfo(**row)


@router.post(
    "/tasks",
    response_model=StartMonitorResponse,
    status_code=201,
    summary="提交 8 个回路温度上限并启动监测任务（单进程唯一）",
)
async def start_monitor(body: StartMonitorRequest, request: Request) -> StartMonitorResponse:
    manager = _manager(request)
    try:
        row = await manager.start(body.thresholds_c)
    except MonitorError as exc:
        # 任务冲突 409；参数内容错误 400。
        status_code = 400 if exc.code == "invalid_thresholds" else 409
        raise HTTPException(
            status_code=status_code,
            detail={"error": exc.code, "message": exc.message},
        ) from exc
    task_id = row["id"]
    return StartMonitorResponse(
        task=_task_info(row),
        rounds_url=f"/monitor/tasks/{task_id}/rounds",
        events_url=f"/monitor/tasks/{task_id}/events",
    )


@router.get(
    "/tasks",
    response_model=list[TaskInfo],
    summary="查询全部监测任务",
)
async def list_monitor_tasks(request: Request) -> list[TaskInfo]:
    rows = await _manager(request).list_tasks()
    return [_task_info(row) for row in rows]


@router.get(
    "/tasks/{task_id}",
    response_model=TaskInfo,
    summary="按任务 ID 查询监测任务状态",
)
async def get_monitor_task(task_id: str, request: Request) -> TaskInfo:
    row = await _manager(request).get_task(task_id)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "task_not_found",
                "message": f"监测任务 {task_id} 不存在",
            },
        )
    return _task_info(row)


@router.post(
    "/tasks/{task_id}/stop",
    response_model=StopResponse,
    summary="停止监测任务（不开新轮，等在途轮落库后才完成）",
)
async def stop_monitor(task_id: str, request: Request) -> StopResponse:
    manager = _manager(request)
    try:
        result = await manager.stop(task_id)
    except MonitorError as exc:
        status_code = 404 if exc.code == "task_not_found" else 409
        raise HTTPException(
            status_code=status_code,
            detail={"error": exc.code, "message": exc.message},
        ) from exc
    return StopResponse(
        task=_task_info(result["task"]),
        finalized_round_no=result["finalized_round_no"],
        interrupted_event_ids=result["interrupted_event_ids"],
    )


@router.get(
    "/tasks/{task_id}/rounds",
    response_model=RoundsResponse,
    summary="按任务查询采样轮次（最新在前，分页）",
)
async def list_rounds(
    task_id: str,
    request: Request,
    limit: int = Query(50, ge=1, le=500, description="每页条数"),
    offset: int = Query(0, ge=0, description="跳过条数"),
) -> RoundsResponse:
    manager = _manager(request)
    if await manager.get_task(task_id) is None:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "task_not_found",
                "message": f"监测任务 {task_id} 不存在",
            },
        )
    rows, total = await manager.list_rounds(task_id, limit, offset)
    items = [
        RoundSummary(
            round_no=row["round_no"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            success_count=row["success_count"],
            failure_count=row["failure_count"],
        )
        for row in rows
    ]
    return RoundsResponse(
        task_id=task_id, limit=limit, offset=offset, total=total, items=items
    )


@router.get(
    "/tasks/{task_id}/rounds/{round_no}",
    response_model=RoundModel,
    summary="查询单轮整轮读数/失败原因与逐回路判定状态",
)
async def get_round(
    task_id: str, round_no: int, request: Request
) -> RoundModel:
    manager = _manager(request)
    if round_no < 1:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "illegal_round",
                "message": "轮次编号从 1 开始",
            },
        )
    detail = await manager.get_round_detail(task_id, round_no)
    if detail is None:
        # 区分任务不存在与轮次不存在。
        if await manager.get_task(task_id) is None:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": "task_not_found",
                    "message": f"监测任务 {task_id} 不存在",
                },
            )
        raise HTTPException(
            status_code=404,
            detail={
                "error": "round_not_found",
                "message": f"任务 {task_id} 不存在第 {round_no} 轮",
            },
        )
    readings = [
        ReadingModel(
            circuit=r["circuit"],
            ok=bool(r["ok"]),
            temperature_c=r["temperature_c"],
            raw_register=r["raw_register"],
            threshold_c=r["threshold_c"],
            error_code=r["error_code"],
            error_message=r["error_message"],
            judge_status=r["judge_status"],
            confirm_count=r["confirm_count"],
            event_id=r["event_id"],
            sampled_at=r["sampled_at"],
        )
        for r in detail["readings"]
    ]
    return RoundModel(
        round_no=detail["round_no"],
        started_at=detail["started_at"],
        finished_at=detail["finished_at"],
        success_count=detail["success_count"],
        failure_count=detail["failure_count"],
        readings=readings,
    )


@router.get(
    "/tasks/{task_id}/events",
    response_model=EventsResponse,
    summary="按任务查询超温事件（可按状态/回路过滤）",
)
async def list_events(
    task_id: str,
    request: Request,
    status: str | None = Query(
        None,
        description="事件状态过滤：open / recovered / interrupted",
    ),
    circuit: int | None = Query(None, ge=0, le=7, description="回路编号过滤"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> EventsResponse:
    manager = _manager(request)
    if await manager.get_task(task_id) is None:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "task_not_found",
                "message": f"监测任务 {task_id} 不存在",
            },
        )
    if status is not None and status not in _EVENT_STATUS_VALUES:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "invalid_status",
                "message": (
                    "事件状态只能是 "
                    f"{sorted(_EVENT_STATUS_VALUES)} 之一"
                ),
            },
        )
    rows, total = await manager.list_events(
        task_id, status, circuit, limit, offset
    )
    items = [EventModel(**row) for row in rows]
    return EventsResponse(
        task_id=task_id, limit=limit, offset=offset, total=total, items=items
    )
