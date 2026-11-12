#!/usr/bin/env python3
"""持续监测端到端演示：超温确认、缺测不解除、恢复结束与停止中断。

前置：``docker compose up`` 已启动采集 API（:8000）与模拟控制器（:8081）。

用法（容器内或本地均可）：
    python scripts/demo_monitoring.py http://127.0.0.1:8000 http://127.0.0.1:8081

脚本流程：
  1. 提交 8 个回路上限并启动唯一监测任务（回路 3 上限 20℃，当前 23.5℃ 超限）；
  2. 等待两轮，观察回路 3 由 over_limit 待确认变为 opened 开事件；
  3. 对回路 3 注入异常响应制造缺测，事件保持 open 不解除；
  4. 把回路 3 降到 12℃，下一轮成功不超限按 recovered 恢复结束；
  5. 重新升温开新事件后停止任务，未结束事件被标记为 interrupted（非恢复）。
"""

from __future__ import annotations

import asyncio
import sys

import httpx

# 模拟器默认温度（℃）：-18, -17.5, 5, 23.5, -20, 30, -4.5, 10.5
THRESHOLDS = [0.0, 0.0, 10.0, 20.0, 0.0, 35.0, 0.0, 20.0]
DEFAULT_REGISTERS = [-180, -175, 50, 235, -200, 300, -45, 105]


async def wait_rounds(api: httpx.AsyncClient, task_id: str, minimum: int) -> None:
    for _ in range(100):
        task = (await api.get(f"/monitor/tasks/{task_id}")).json()
        if task["last_round_no"] >= minimum:
            return
        await asyncio.sleep(0.25)
    raise RuntimeError("等待轮次落库超时")


async def print_round(api: httpx.AsyncClient, task_id: str, round_no: int) -> None:
    d = (await api.get(f"/monitor/tasks/{task_id}/rounds/{round_no}")).json()
    print(f"  第 {round_no} 轮：成功 {d['success_count']} / 失败 {d['failure_count']}")
    for r in d["readings"]:
        if r["circuit"] != 3:
            continue
        if r["ok"]:
            print(
                f"    回路3: {r['temperature_c']}℃ 判定={r['judge_status']} "
                f"确认={r['confirm_count']} 事件={r['event_id']}"
            )
        else:
            print(
                f"    回路3: 缺测 判定={r['judge_status']} "
                f"原因={r['error_code']} 事件={r['event_id']}"
            )


async def print_events(api: httpx.AsyncClient, task_id: str) -> None:
    body = (await api.get(f"/monitor/tasks/{task_id}/events")).json()
    for e in body["items"]:
        print(
            f"  事件 {e['id']} 回路{e['circuit']} 状态={e['status']} "
            f"开启轮={e['opened_round_no']} 结束轮={e['closed_round_no']} "
            f"结束温度={e['closed_temp_c']}"
        )


async def main(api_base: str, ctrl_base: str) -> None:
    async with httpx.AsyncClient(base_url=api_base, timeout=10.0) as api, \
            httpx.AsyncClient(base_url=ctrl_base, timeout=10.0) as ctrl:
        print("== 1. 提交上限并启动监测任务 ==")
        started = (
            await api.post("/monitor/tasks", json={"thresholds_c": THRESHOLDS})
        ).json()
        task_id = started["task"]["id"]
        print(f"  任务 ID: {task_id}")

        print("== 2. 连续两轮成功超限才开事件 ==")
        await wait_rounds(api, task_id, 2)
        await print_round(api, task_id, 1)
        await print_round(api, task_id, 2)

        print("== 3. 注入异常制造缺测，事件不解除 ==")
        before = (await api.get(f"/monitor/tasks/{task_id}")).json()["last_round_no"]
        await ctrl.post(
            "/exception", json={"address": 3, "code": 2, "count": 10}
        )
        await wait_rounds(api, task_id, before + 1)
        await print_round(api, task_id, before + 1)
        open_events = (
            await api.get(f"/monitor/tasks/{task_id}/events?status=open")
        ).json()["total"]
        print(f"  缺测后仍开启的事件数: {open_events}（预期 1）")

        print("== 4. 降温到 12℃，成功不超限按恢复结束 ==")
        await ctrl.post("/reset")
        values = list(DEFAULT_REGISTERS)
        values[3] = 120
        await ctrl.post("/registers", json={"values": values})
        before = (await api.get(f"/monitor/tasks/{task_id}")).json()["last_round_no"]
        await wait_rounds(api, task_id, before + 1)
        await print_round(api, task_id, before + 1)

        print("== 5. 重新升温开新事件后停止，未结束事件标中断 ==")
        values[3] = 250
        await ctrl.post("/registers", json={"values": values})
        await wait_rounds(
            api, task_id,
            (await api.get(f"/monitor/tasks/{task_id}")).json()["last_round_no"] + 2,
        )
        stop_body = (
            await api.post(f"/monitor/tasks/{task_id}/stop")
        ).json()
        print(
            f"  停止完成：状态={stop_body['task']['status']} "
            f"收尾轮={stop_body['finalized_round_no']} "
            f"中断事件={stop_body['interrupted_event_ids']}"
        )
        await print_events(api, task_id)
        print("演示结束。可在 /docs 查看全部接口，或访问 / 查看可视化页面。")


if __name__ == "__main__":
    api_url = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
    ctrl_url = sys.argv[2] if len(sys.argv) > 2 else "http://127.0.0.1:8081"
    asyncio.run(main(api_url, ctrl_url))
