"""
统计信息路由模块 - 处理 Token 消耗与请求日志相关 API
"""

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse

from log import log
from src.storage_adapter import get_storage_adapter
from src.utils import verify_panel_token

router = APIRouter(prefix="/panel/api/stats", tags=["stats"])


@router.get("/token_logs")
async def get_token_logs(
    date: Optional[str] = Query(None, description="指定查询日期，格式 YYYY-MM-DD，默认今天"),
    page: int = Query(1, ge=1, description="页码，默认 1"),
    page_size: int = Query(50, ge=1, le=200, description="每页条数，默认 50"),
    token: str = Depends(verify_panel_token),
):
    """获取指定日期的 Token 使用量与请求日志明细"""
    try:
        if not date:
            now = datetime.now(tz=timezone.utc).astimezone()
            date = now.strftime("%Y-%m-%d")

        adapter = await get_storage_adapter()
        result = await adapter.get_request_logs(
            date_str=date,
            page=page,
            page_size=page_size,
        )
        return JSONResponse(content=result)
    except Exception as e:
        log.error(f"Error getting token logs: {e}")
        return JSONResponse(
            status_code=500,
            content={
                "error": f"获取 Token 日志失败: {str(e)}",
                "date": date or "",
                "summary": {
                    "total_requests": 0,
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "cached_tokens": 0,
                    "uncached_tokens": 0,
                    "total_tokens": 0,
                    "cache_hit_rate": 0.0,
                },
                "pagination": {"page": page, "page_size": page_size, "total_pages": 0, "total_count": 0},
                "logs": [],
            },
        )


@router.delete("/token_logs")
async def clear_token_logs(
    date: Optional[str] = Query(None, description="指定清除的日期，格式 YYYY-MM-DD 或 all（全部）"),
    token: str = Depends(verify_panel_token),
):
    """清除指定日期或全量的请求日志脏数据"""
    try:
        adapter = await get_storage_adapter()
        success = await adapter.clear_request_logs(date_str=date)
        if success:
            msg = f"已成功清除 {date} 的请求日志" if date and date != "all" else "已成功清空所有请求日志历史脏数据"
            return JSONResponse(content={"success": True, "message": msg})
        else:
            return JSONResponse(status_code=500, content={"success": False, "error": "清除失败"})
    except Exception as e:
        log.error(f"Error clearing token logs: {e}")
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})
