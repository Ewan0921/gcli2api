"""
Antigravity API Client - Handles communication with Google's Antigravity API
处理与 Google Antigravity API 的通信
"""

import asyncio
import copy
import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Callable, Tuple
from pathlib import Path

# 项目根目录 (src/api/antigravity.py 向上 3 级)
project_root = Path(__file__).resolve().parent.parent.parent

from fastapi import Response
from config import (
    get_antigravity_api_url,
    get_antigravity_stream2nostream,
    get_auto_ban_error_codes,
    get_debug_dump_payload_enabled,
)
from log import log

from src.credential_manager import credential_manager
from src.httpx_client import stream_post_async, post_async
from src.models import Model, model_to_dict
from src.utils import ANTIGRAVITY_USER_AGENT, record_request_log_async

# 导入共同的基础功能
from src.api.utils import (
    handle_error_with_retry,
    get_retry_config,
    record_api_call_success,
    record_api_call_error,
    parse_and_log_cooldown,
    collect_streaming_response,
)

# ==================== 全局凭证管理器 ====================

# 使用全局单例 credential_manager，自动初始化


# ==================== 会话状态管理 ====================

SESSION_TTL_SECONDS = 6 * 60 * 60
MAX_SESSION_STATES = 1024
_REDIS_KEY_PREFIX = "antigravity:session:"


@dataclass
class AntigravitySessionState:
    conversation_id: str
    trajectory_id: str
    session_id: str
    step_index: int
    created_at: float
    last_used_at: float
    bound_credential_file: Optional[str] = None


# 内存回退存储
_session_states: Dict[str, AntigravitySessionState] = {}

# Redis 客户端（懒初始化，REDIS_URL 存在时使用）
_redis_client = None
_redis_checked = False


async def _get_redis():
    """懒初始化 Redis 客户端，REDIS_URL 未设置时返回 None。"""
    global _redis_client, _redis_checked
    if _redis_checked:
        return _redis_client
    _redis_checked = True
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        return None
    try:
        import redis.asyncio as aioredis  # type: ignore
        client = aioredis.from_url(redis_url, decode_responses=True)
        await client.ping()
        _redis_client = client
        log.info("[SESSION] Redis session store enabled")
    except Exception as e:
        log.warning(f"[SESSION] Redis unavailable, falling back to in-memory: {e}")
    return _redis_client
def _extract_first_user_text(request_payload: Dict[str, Any]) -> str:
    """
    提取会话 Key 的文本来源。
    优先取包含 <user_query> 标签归属的完整 text 字符串；
    若没有 <user_query>，则过滤纯系统环境/MCP配置，取第一条真实 user 消息的完整 text。
    """
    contents = request_payload.get("contents", [])
    if not isinstance(contents, list):
        return ""

    # 1. 优先扫描寻找包含 <user_query> 标签的那一个 part 的完整 text 信息
    for content in contents:
        if isinstance(content, dict) and content.get("role") == "user":
            parts = content.get("parts", [])
            if isinstance(parts, list):
                for part in parts:
                    if isinstance(part, dict) and part.get("text"):
                        text_str = str(part["text"])
                        if "<user_query>" in text_str:
                            return text_str

    # 2. 保底逻辑：过滤纯静态系统/MCP环境描述，取第一条真实 user 消息的完整 text 拼接
    for content in contents:
        if isinstance(content, dict) and content.get("role") == "user":
            parts = content.get("parts", [])
            if isinstance(parts, list):
                valid_texts = []
                is_system_meta = False
                for part in parts:
                    if isinstance(part, dict) and part.get("text"):
                        text_str = str(part["text"])
                        if "<mcp_server_catalog>" in text_str or "<user_info>" in text_str:
                            is_system_meta = True
                            break
                        valid_texts.append(text_str)
                if not is_system_meta and valid_texts:
                    return "\n".join(valid_texts)

    return ""


<<<<<<< HEAD
def _session_key(request_payload: Dict[str, Any], model: str = "") -> str:
    session_id = request_payload.get("sessionId")
    if session_id:
        return f"session:{session_id}"
    model_prefix = f"model:{model}:" if model else ""
    first_user_text = _extract_first_user_text(request_payload)
    if first_user_text:
        digest = hashlib.sha256(first_user_text.encode("utf-8")).hexdigest()[:32]
        return f"{model_prefix}text:{digest}"
    return f"{model_prefix}default"


def _prune_session_states(now: float) -> None:
    expired = [k for k, s in _session_states.items() if now - s.last_used_at > SESSION_TTL_SECONDS]
    for k in expired:
        _session_states.pop(k, None)
    if len(_session_states) <= MAX_SESSION_STATES:
        return
    overflow = len(_session_states) - MAX_SESSION_STATES
    oldest = sorted(_session_states.items(), key=lambda item: item[1].last_used_at)
    for k, _ in oldest[:overflow]:
        _session_states.pop(k, None)


def _make_new_state(first_user_text: str, now: float) -> AntigravitySessionState:
    if first_user_text:
        digest = hashlib.sha256(first_user_text.encode("utf-8")).digest()
        session_id_val = int.from_bytes(digest[:8], "big") & 0x7FFFFFFFFFFFFFFF
        session_id = f"-{session_id_val}"
    else:
        session_id = f"-{uuid.uuid4().int % 9_000_000_000_000_000_000}"
    return AntigravitySessionState(
        conversation_id=str(uuid.uuid4()),
        trajectory_id=str(uuid.uuid4()),
        session_id=session_id,
        step_index=1,
        created_at=now,
        last_used_at=now,
    )


async def _save_session_state(key: str, state: AntigravitySessionState) -> None:
    """保存或更新会话状态（如记录绑定的凭证文件名）"""
    redis = await _get_redis()
    if redis is not None:
        redis_key = f"{_REDIS_KEY_PREFIX}{key}"
        try:
            await redis.set(redis_key, json.dumps(state.__dict__), ex=SESSION_TTL_SECONDS)
            return
        except Exception as e:
            log.warning(f"[SESSION] Redis save error: {e}")
    _session_states[key] = state


async def _dump_payload_to_project_root_if_enabled(request_payload: Dict[str, Any]) -> None:
    """如果启用了 DEBUG_DUMP_PAYLOAD，将传入的数据完整保存到项目根目录下的 debug_payload.json"""
    try:
        enabled = await get_debug_dump_payload_enabled()
        if enabled:
            dump_file = project_root / "debug_payload.json"
            with open(dump_file, "w", encoding="utf-8") as f:
                json.dump(request_payload, f, ensure_ascii=False, indent=2)
            log.info(f"[DEBUG DUMP] 💥 完整 Request Payload 已成功保存到项目根目录: {dump_file}")
    except Exception as e:
        log.warning(f"[DEBUG DUMP] 保存 debug_payload.json 失败: {e}")


async def _get_session_state(request_payload: Dict[str, Any], model: str = "") -> Tuple[AntigravitySessionState, str]:
    # 检查是否需要导出保存完整 Payload 到根目录
    await _dump_payload_to_project_root_if_enabled(request_payload)

    now = time.time()
    key = _session_key(request_payload, model)
    first_user_text = _extract_first_user_text(request_payload)
    clean_text_preview = first_user_text.replace("\r", "").replace("\n", "\\n")
    text_snippet = (clean_text_preview[:40] + "...") if len(clean_text_preview) > 40 else (clean_text_preview or "无")
    key_hash = hashlib.sha256(first_user_text.encode("utf-8")).hexdigest()[:8] if first_user_text else "none"

    redis = await _get_redis()
    if redis is not None:
        redis_key = f"{_REDIS_KEY_PREFIX}{key}"
        try:
            raw = await redis.get(redis_key)
            if raw:
                data = json.loads(raw)
                state = AntigravitySessionState(**data)
                state.step_index += 1
                state.last_used_at = now
                log.info(f"[SESSION-HIT] 🟢 成功续接会话(Redis) | session_id: {state.session_id} | 当前步数: Step {state.step_index} | 模型: {model} | 哈希: {key_hash} | 首句: '{text_snippet}'")
            else:
                state = _make_new_state(first_user_text, now)
                log.info(f"[SESSION-MISSED] 🟡 新建会话(Redis未命中) | 新 session_id: {state.session_id} | 模型: {model} | 哈希: {key_hash} | 首句: '{text_snippet}'")
            await redis.set(redis_key, json.dumps(state.__dict__), ex=SESSION_TTL_SECONDS)
            return state, key
        except Exception as e:
            log.warning(f"[SESSION] Redis error, falling back to memory: {e}")

    # 内存回退
    _prune_session_states(now)
    state = _session_states.get(key)
    if state:
        state.step_index += 1
        state.last_used_at = now
        log.info(f"[SESSION-HIT] 🟢 成功续接会话(Memory) | session_id: {state.session_id} | 当前步数: Step {state.step_index} | 模型: {model} | 哈希: {key_hash} | 首句: '{text_snippet}'")
        return state, key
    state = _make_new_state(first_user_text, now)
    _session_states[key] = state
    log.info(f"[SESSION-MISSED] 🟡 新建会话(Memory未命中) | 新 session_id: {state.session_id} | 模型: {model} | 哈希: {key_hash} | 首句: '{text_snippet}'")
    return state, key


async def get_sticky_credential_for_session(
    state: AntigravitySessionState,
    session_key: str,
    mode: str = "antigravity",
    model_name: Optional[str] = None
) -> Optional[Tuple[str, Dict[str, Any]]]:
    """
    根据 Session 绑定的凭证优先获取；若未绑定或已被禁用/冷却，则随机获取可用凭证并重新绑定 (实现 100% Session Affinity + 故障转移)
    """
    if state.bound_credential_file:
        cred = await credential_manager.get_credential_by_filename(
            state.bound_credential_file, mode=mode, model_name=model_name
        )
        if cred:
            log.info(f"[SESSION-STICKY] 🎯 命中会话绑定账号 | session_id: {state.session_id} | 账号: {state.bound_credential_file}")
            return cred
        else:
            log.warning(f"[SESSION-STICKY] ⚠️ 绑定账号已失效/冷却/禁用 ({state.bound_credential_file})，触发故障转移自动切换新账号")

    # 首次绑定或故障转移重新分配
    cred = await credential_manager.get_valid_credential(mode=mode, model_name=model_name)
    if cred:
        filename, credential_data = cred
        state.bound_credential_file = filename
        await _save_session_state(session_key, state)
        log.info(f"[SESSION-STICKY] 📌 会话成功绑定新账号 | session_id: {state.session_id} | 账号: {filename}")
        return cred

    return None


def _generate_request_id(conversation_id: str, trajectory_id: str, step: int) -> str:
    unix_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    return f"agent/{conversation_id}/{unix_ms}/{trajectory_id}/{step}"


def _build_labels(model: str, trajectory_id: str, step: int) -> Dict[str, str]:
    used_claude = "claude" in model.lower()
    return {
        "last_step_index": str(step),
        "model_enum": model,
        "trajectory_id": trajectory_id,
        "used_claude": str(used_claude).lower(),
        "used_claude_conservative": str(used_claude).lower(),
    }


def _should_forward_antigravity_header(header_name: str) -> bool:
    normalized = header_name.strip().lower()
    if not normalized:
        return False
    if normalized.startswith("x-b3-"):
        return True
    return normalized in {
        "accept-language",
        "traceparent",
        "tracestate",
        "x-cloud-trace-context",
        "x-goog-api-client",
        "x-goog-request-params",
        "x-goog-user-project",
        "x-request-id",
    }


def _sanitize_antigravity_headers(extra_headers: Optional[Dict[str, str]]) -> Dict[str, str]:
    if not extra_headers:
        return {}
    sanitized: Dict[str, str] = {}
    for key, value in extra_headers.items():
        if _should_forward_antigravity_header(key):
            sanitized[key] = value
    return sanitized


async def wrap_cli_request(
    gemini_request: Dict[str, Any],
    model: str,
    project_id: str,
    bound_filename: Optional[str] = None,
    enable_credit: bool = False,
) -> Tuple[Dict[str, Any], str]:
    """
    将 Gemini 格式请求包装成 Antigravity CLI 格式。
    返回 (payload, request_id)。
    """
    inner = copy.deepcopy(gemini_request)
    first_user_text = _extract_first_user_text(inner)

    # 移除 safetySettings（CLI 不发送）
    inner.pop("safetySettings", None)

    # 获取/更新会话状态
    state, session_key = await _get_session_state(inner, model)

    # 绑定账号粘性
    if bound_filename and state.bound_credential_file != bound_filename:
        state.bound_credential_file = bound_filename
        await _save_session_state(session_key, state)
    # 注入 sessionId
    session_id = str(inner.get("sessionId") or "").strip()
    if not session_id:
        if first_user_text:
            digest = hashlib.sha256(first_user_text.encode("utf-8")).digest()
            session_id_val = int.from_bytes(digest[:8], "big") & 0x7FFFFFFFFFFFFFFF
            session_id = f"-{session_id_val}"
        else:
            session_id = f"-{uuid.uuid4().int % 9_000_000_000_000_000_000}"
        inner["sessionId"] = session_id

    # 注入 labels
    inner["labels"] = _build_labels(model, session_id, 1)

    # toolConfig 默认 VALIDATED
    tool_config = inner.get("toolConfig") or {}
    func_config = tool_config.get("functionCallingConfig") or {}
    func_config["mode"] = "VALIDATED"
    tool_config["functionCallingConfig"] = func_config
    inner["toolConfig"] = tool_config

    request_id = _generate_request_id()

    payload = {
        "project": project_id,
        "requestId": request_id,
        "request": inner,
        "model": model,
        "userAgent": "antigravity",
        "requestType": "agent",
    }
    if enable_credit:
        payload["enabledCreditTypes"] = ["GOOGLE_ONE_AI"]
    return payload, request_id


# ==================== 辅助函数 ====================

def build_antigravity_headers(
    access_token: str,
    extra_headers: Optional[Dict[str, str]] = None,
    model_name: str = "",
) -> Dict[str, str]:
    """构建 Antigravity CLI API 请求头。"""
    headers = {
        "User-Agent": ANTIGRAVITY_USER_AGENT,
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Accept-Encoding": "gzip",
        "Connection": "close",
        "requestId": f"req-{uuid.uuid4()}",
    }

    for key, value in _sanitize_antigravity_headers(extra_headers).items():
        headers.setdefault(key, value)

    # 根据模型名称判断 request_type
    if model_name:
        if "image" in model_name.lower():
            headers["requestType"] = "image_gen"
        else:
            headers["requestType"] = "agent"

    return headers


def _is_retryable_status(status_code: int, disable_error_codes: List[int]) -> bool:
    """统一判断是否属于可重试状态码。"""
    return status_code in (429, 503) or status_code in disable_error_codes


async def _switch_credential_for_retry(
    *,
    next_cred_task: Optional[asyncio.Task],
    retry_interval: float,
    refresh_credential_fast: Callable[[], Any],
    apply_cred_result: Callable[[Tuple[str, Dict[str, Any]]], bool],
    log_prefix: str,
) -> Tuple[bool, Optional[asyncio.Task]]:
    """优先使用预热凭证，失败后退回同步刷新。"""
    if next_cred_task is not None:
        try:
            cred_result = await next_cred_task
            next_cred_task = None
            if cred_result and apply_cred_result(cred_result):
                await asyncio.sleep(retry_interval)
                return True, next_cred_task
        except Exception as e:
            log.warning(f"{log_prefix} 预热凭证任务失败: {e}")
            next_cred_task = None

    await asyncio.sleep(retry_interval)
    if await refresh_credential_fast():
        return True, next_cred_task

    return False, next_cred_task


# ==================== 新的流式和非流式请求函数 ====================

async def stream_request(
    body: Dict[str, Any],
    native: bool = False,
    headers: Optional[Dict[str, str]] = None,
):
    """
    流式请求函数

    Args:
        body: 请求体
        native: 是否返回原生bytes流，False则返回str流
        headers: 额外的请求头

    Yields:
        Response对象（错误时）或 bytes流/str流（成功时）
    """
    await _dump_payload_to_project_root_if_enabled(body)

    model_name = body.get("model", "")
    inner_request = body.get("request", body)
    state, session_key = await _get_session_state(inner_request, model_name)

    # 1. 获取带有 Session 账号粘性的有效凭证
    cred_result = await get_sticky_credential_for_session(
        state, session_key, mode="antigravity", model_name=model_name
    )

    if not cred_result:
        # 如果返回值是None，直接返回错误500
        log.error("[ANTIGRAVITY STREAM] 当前无可用凭证")
        yield Response(
            content=json.dumps({"error": "当前无可用凭证"}),
            status_code=500,
            media_type="application/json"
        )
        return

    current_file, credential_data = cred_result
    access_token = credential_data.get("access_token") or credential_data.get("token")
    project_id = credential_data.get("project_id", "")
    enable_credit = bool(credential_data.get("enable_credit", False))

    if not access_token:
        log.error(f"[ANTIGRAVITY STREAM] No access token in credential: {current_file}")
        yield Response(
            content=json.dumps({"error": "凭证中没有访问令牌"}),
            status_code=500,
            media_type="application/json"
        )
        return

    # 2. 构建URL和请求头
    antigravity_url = await get_antigravity_api_url()
    target_url = f"{antigravity_url}/v1internal:streamGenerateContent?alt=sse"

    auth_headers = build_antigravity_headers(access_token, headers, model_name)

    # 构建 CLI 格式请求体
    inner_request = body.get("request", body)
    final_payload, _ = await wrap_cli_request(inner_request, model_name, project_id, bound_filename=current_file, enable_credit=enable_credit)

    # 3. 调用stream_post_async进行请求
    retry_config = await get_retry_config()
    max_retries = retry_config["max_retries"]
    retry_interval = retry_config["retry_interval"]

    DISABLE_ERROR_CODES = await get_auto_ban_error_codes()  # 禁用凭证的错误码
    last_error_response = None  # 记录最后一次的错误响应
    next_cred_task = None  # 预热的下一个凭证任务

    # 内部函数：快速更新凭证(只更新token和project_id,避免重建整个请求)
    async def refresh_credential_fast():
        nonlocal current_file, access_token, auth_headers, project_id, final_payload
        cred_result = await credential_manager.get_valid_credential(
            mode="antigravity", model_name=model_name
        )
        if not cred_result:
            return None
        current_file, credential_data = cred_result
        access_token = credential_data.get("access_token") or credential_data.get("token")
        project_id = credential_data.get("project_id", "")
        if not access_token:
            return None
        # 只更新token和project_id,不重建整个headers和payload
        auth_headers["Authorization"] = f"Bearer {access_token}"
        final_payload["project"] = project_id
        return True

    def apply_cred_result(cred_result: Tuple[str, Dict[str, Any]]) -> bool:
        nonlocal current_file, access_token, project_id, auth_headers, final_payload
        current_file, credential_data = cred_result
        access_token = credential_data.get("access_token") or credential_data.get("token")
        project_id = credential_data.get("project_id", "")
        if not access_token or not project_id:
            return False
        auth_headers["Authorization"] = f"Bearer {access_token}"
        final_payload["project"] = project_id
        return True

    for attempt in range(max_retries + 1):
        success_recorded = False  # 标记是否已记录成功
        need_retry = False  # 标记是否需要重试
        latest_usage = None

        try:
            async for chunk in stream_post_async(
                url=target_url,
                body=final_payload,
                native=native,
                headers=auth_headers
            ):
                # 判断是否是Response对象
                if isinstance(chunk, Response):
                    status_code = chunk.status_code
                    last_error_response = chunk  # 记录最后一次错误

                    # 缓存错误解析结果,避免重复decode
                    error_body = None
                    try:
                        error_body = chunk.body.decode('utf-8') if isinstance(chunk.body, bytes) else str(chunk.body)
                    except Exception:
                        error_body = ""

                    # 如果错误码是429、503或者在禁用码当中，做好记录后进行重试
                    if _is_retryable_status(status_code, DISABLE_ERROR_CODES):
                        log.warning(f"[ANTIGRAVITY STREAM] 流式请求失败 (status={status_code}), 凭证: {current_file}, 响应: {error_body[:500] if error_body else '无'}")

                        # 解析冷却时间
                        cooldown_until = None
                        if (status_code == 429 or status_code == 503) and error_body:
                            try:
                                cooldown_until = await parse_and_log_cooldown(error_body, mode="antigravity")
                            except Exception:
                                pass

                        # 预热下一个凭证
                        if next_cred_task is None and attempt < max_retries:
                            next_cred_task = asyncio.create_task(
                                credential_manager.get_valid_credential(
                                    mode="antigravity", model_name=model_name
                                )
                            )

                        # 记录错误并切换凭证
                        await record_api_call_error(
                            credential_manager, current_file, status_code,
                            cooldown_until, mode="antigravity", model_name=model_name,
                            error_message=error_body
                        )

                        # 检查是否应该重试
                        should_retry = await handle_error_with_retry(
                            credential_manager, status_code, current_file,
                            retry_config["retry_enabled"], attempt, max_retries, retry_interval,
                            mode="antigravity"
                        )

                        if should_retry and attempt < max_retries:
                            need_retry = True
                            break  # 跳出内层循环，准备重试
                        else:
                            # 不重试，直接返回原始错误
                            log.error(f"[ANTIGRAVITY STREAM] 达到最大重试次数或不应重试，返回原始错误")
                            yield chunk
                            return
                    else:
                        # 错误码不在禁用码当中，直接返回，无需重试
                        log.error(f"[ANTIGRAVITY STREAM] 流式请求失败，非重试错误码 (status={status_code}), 凭证: {current_file}, 响应: {error_body[:500] if error_body else '无'}")
                        await record_api_call_error(
                            credential_manager, current_file, status_code,
                            None, mode="antigravity", model_name=model_name,
                            error_message=error_body
                        )
                        yield chunk
                        return
                else:
                    # 不是Response，说明是真流，直接yield返回
                    # 只在第一个chunk时记录成功
                    if not success_recorded:
                        await record_api_call_success(
                            credential_manager, current_file, mode="antigravity", model_name=model_name
                        )
                        success_recorded = True
                        log.debug(f"[ANTIGRAVITY STREAM] 开始接收流式响应，模型: {model_name}")

                    try:
                        chunk_str = chunk.decode("utf-8") if isinstance(chunk, bytes) else str(chunk)
                        if "usageMetadata" in chunk_str:
                            for line in chunk_str.split("\n"):
                                line_clean = line.strip()
                                if not line_clean:
                                    continue
                                json_str = line_clean[6:].strip() if line_clean.startswith("data: ") else line_clean
                                if json_str and json_str != "[DONE]":
                                    try:
                                        line_data = json.loads(json_str)
                                        if isinstance(line_data, dict):
                                            usage = line_data.get("usageMetadata") or (
                                                line_data.get("response", {}).get("usageMetadata")
                                                if isinstance(line_data.get("response"), dict)
                                                else None
                                            )
                                            if usage:
                                                latest_usage = usage
                                    except Exception:
                                        pass
                    except Exception:
                        pass

                    # 记录原始chunk内容（用于调试）
                    if isinstance(chunk, bytes):
                        log.debug(f"[ANTIGRAVITY STREAM RAW] chunk(bytes): {chunk}")
                    else:
                        log.debug(f"[ANTIGRAVITY STREAM RAW] chunk(str): {chunk}")

                    yield chunk

            # 流式请求完成，检查结果并统一记库 1 次
            if success_recorded:
                log.debug(f"[ANTIGRAVITY STREAM] 流式响应完成，模型: {model_name}")
                if latest_usage:
                    user_email = credential_data.get("user_email") or credential_data.get("email") or ""
                    record_request_log_async(user_email, "antigravity", model_name, latest_usage)
                return
            elif not need_retry:
                # 没有收到任何数据（空回复），需要重试
                log.warning(f"[ANTIGRAVITY STREAM] 收到空回复，无任何内容，凭证: {current_file}")
                await record_api_call_error(
                    credential_manager, current_file, 200,
                    None, mode="antigravity", model_name=model_name,
                    error_message="Empty response from API"
                )
                
                if attempt < max_retries:
                    need_retry = True
                else:
                    log.error(f"[ANTIGRAVITY STREAM] 空回复达到最大重试次数")
                    yield Response(
                        content=json.dumps({"error": "服务返回空回复"}),
                        status_code=500,
                        media_type="application/json"
                    )
                    return
            
            # 统一处理重试
            if need_retry:
                log.info(f"[ANTIGRAVITY STREAM] 重试请求 (attempt {attempt + 2}/{max_retries + 1})...")

                switched, next_cred_task = await _switch_credential_for_retry(
                    next_cred_task=next_cred_task,
                    retry_interval=retry_interval,
                    refresh_credential_fast=refresh_credential_fast,
                    apply_cred_result=apply_cred_result,
                    log_prefix="[ANTIGRAVITY STREAM]",
                )
                if not switched:
                    log.error("[ANTIGRAVITY STREAM] 重试时无可用凭证或令牌")
                    yield Response(
                        content=json.dumps({"error": "当前无可用凭证"}),
                        status_code=500,
                        media_type="application/json"
                    )
                    return
                continue  # 重试

        except Exception as e:
            log.error(f"[ANTIGRAVITY STREAM] 流式请求异常: {e}, 凭证: {current_file}")
            if attempt < max_retries:
                log.info(f"[ANTIGRAVITY STREAM] 异常后重试 (attempt {attempt + 2}/{max_retries + 1})...")
                await asyncio.sleep(retry_interval)
                continue
            else:
                # 所有重试都失败，返回最后一次的错误（如果有）
                log.error(f"[ANTIGRAVITY STREAM] 所有重试均失败，最后异常: {e}")
                if last_error_response:
                    yield last_error_response
                else:
                    # 如果没有记录到错误响应，返回500错误
                    yield Response(
                        content=json.dumps({"error": f"流式请求异常: {str(e)}"}),
                        status_code=500,
                        media_type="application/json"
                    )
                return

    # 所有重试均已耗尽（for循环正常结束），返回最后记录的错误
    log.error("[ANTIGRAVITY STREAM] 所有重试均失败")
    if last_error_response:
        yield last_error_response
    else:
        yield Response(
            content=json.dumps({"error": "请求失败，所有重试均已耗尽"}),
            status_code=429,
            media_type="application/json"
        )


async def non_stream_request(
    body: Dict[str, Any],
    headers: Optional[Dict[str, str]] = None,
) -> Response:
    """
    非流式请求函数

    Args:
        body: 请求体
        headers: 额外的请求头

    Returns:
        Response对象
    """
    # 检查是否启用流式收集模式
    if await get_antigravity_stream2nostream():
        log.debug("[ANTIGRAVITY] 使用流式收集模式实现非流式请求")

        # 调用stream_request获取流
        stream = stream_request(body=body, native=False, headers=headers)

        # 收集流式响应
        # stream_request是一个异步生成器，可能yield Response（错误）或流数据
        # collect_streaming_response会自动处理这两种情况
        return await collect_streaming_response(stream)

    # 否则使用传统非流式模式
    log.debug("[ANTIGRAVITY] 使用传统非流式模式")

    model_name = body.get("model", "")
    inner_request = body.get("request", body)
    state, session_key = await _get_session_state(inner_request, model_name)

    # 1. 获取带有 Session 账号粘性的有效凭证
    cred_result = await get_sticky_credential_for_session(
        state, session_key, mode="antigravity", model_name=model_name
    )

    if not cred_result:
        # 如果返回值是None，直接返回错误500
        log.error("[ANTIGRAVITY] 当前无可用凭证")
        return Response(
            content=json.dumps({"error": "当前无可用凭证"}),
            status_code=500,
            media_type="application/json"
        )

    current_file, credential_data = cred_result
    access_token = credential_data.get("access_token") or credential_data.get("token")
    project_id = credential_data.get("project_id", "")
    enable_credit = bool(credential_data.get("enable_credit", False))

    if not access_token:
        log.error(f"[ANTIGRAVITY] No access token in credential: {current_file}")
        return Response(
            content=json.dumps({"error": "凭证中没有访问令牌"}),
            status_code=500,
            media_type="application/json"
        )

    # 2. 构建URL和请求头
    antigravity_url = await get_antigravity_api_url()
    target_url = f"{antigravity_url}/v1internal:generateContent"

    auth_headers = build_antigravity_headers(access_token, headers, model_name)

    # 构建 CLI 格式请求体
    final_payload, _ = await wrap_cli_request(inner_request, model_name, project_id, bound_filename=current_file, enable_credit=enable_credit)

    # 3. 调用post_async进行请求
    retry_config = await get_retry_config()
    max_retries = retry_config["max_retries"]
    retry_interval = retry_config["retry_interval"]

    DISABLE_ERROR_CODES = await get_auto_ban_error_codes()  # 禁用凭证的错误码
    last_error_response = None  # 记录最后一次的错误响应
    next_cred_task = None  # 预热的下一个凭证任务

    # 内部函数：快速更新凭证(只更新token和project_id,避免重建整个请求)
    async def refresh_credential_fast():
        nonlocal current_file, access_token, auth_headers, project_id, final_payload
        cred_result = await credential_manager.get_valid_credential(
            mode="antigravity", model_name=model_name
        )
        if not cred_result:
            return None
        current_file, credential_data = cred_result
        access_token = credential_data.get("access_token") or credential_data.get("token")
        project_id = credential_data.get("project_id", "")
        if not access_token:
            return None
        # 只更新token和project_id,不重建整个headers和payload
        auth_headers["Authorization"] = f"Bearer {access_token}"
        final_payload["project"] = project_id
        return True

    def apply_cred_result(cred_result: Tuple[str, Dict[str, Any]]) -> bool:
        nonlocal current_file, access_token, project_id, auth_headers, final_payload
        current_file, credential_data = cred_result
        access_token = credential_data.get("access_token") or credential_data.get("token")
        project_id = credential_data.get("project_id", "")
        if not access_token or not project_id:
            return False
        auth_headers["Authorization"] = f"Bearer {access_token}"
        final_payload["project"] = project_id
        return True

    for attempt in range(max_retries + 1):
        need_retry = False  # 标记是否需要重试
        
        try:
            response = await post_async(
                url=target_url,
                json=final_payload,
                headers=auth_headers
            )

            status_code = response.status_code

            # 成功
            if status_code == 200:
                # 检查是否为空回复
                if not response.content or len(response.content) == 0:
                    log.warning(f"[ANTIGRAVITY] 收到200响应但内容为空，凭证: {current_file}")
                    
                    # 记录错误
                    await record_api_call_error(
                        credential_manager, current_file, 200,
                        None, mode="antigravity", model_name=model_name,
                        error_message="Empty response from API"
                    )
                    
                    if attempt < max_retries:
                        need_retry = True
                    else:
                        log.error(f"[ANTIGRAVITY] 空回复达到最大重试次数")
                        return Response(
                            content=json.dumps({"error": "服务返回空回复"}),
                            status_code=500,
                            media_type="application/json"
                        )
                else:
                    # 正常响应
                    await record_api_call_success(
                        credential_manager, current_file, mode="antigravity", model_name=model_name
                    )
                    try:
                        user_email = credential_data.get("user_email") or credential_data.get("email") or ""
                        resp_json = json.loads(response.content)
                        if isinstance(resp_json, dict):
                            usage = resp_json.get("usageMetadata") or (
                                resp_json.get("response", {}).get("usageMetadata")
                                if isinstance(resp_json.get("response"), dict)
                                else None
                            )
                            if usage:
                                record_request_log_async(user_email, "antigravity", model_name, usage)
                    except Exception:
                        pass

                    return Response(
                        content=response.content,
                        status_code=200,
                        headers=dict(response.headers)
                    )

            # 失败 - 记录最后一次错误
            if status_code != 200:
                last_error_response = Response(
                    content=response.content,
                    status_code=status_code,
                    headers=dict(response.headers)
                )

                # 判断是否需要重试
                # 缓存错误文本,避免重复解析
                error_text = ""
                try:
                    error_text = response.text
                except Exception:
                    pass

                if _is_retryable_status(status_code, DISABLE_ERROR_CODES):
                    log.warning(f"[ANTIGRAVITY] 非流式请求失败 (status={status_code}), 凭证: {current_file}, 响应: {error_text[:500] if error_text else '无'}")

                    # 解析冷却时间
                    cooldown_until = None
                    if (status_code == 429 or status_code == 503) and error_text:
                        try:
                            cooldown_until = await parse_and_log_cooldown(error_text, mode="antigravity")
                        except Exception:
                            pass

                    # 并行预热下一个凭证,不阻塞当前处理
                    if next_cred_task is None and attempt < max_retries:
                        next_cred_task = asyncio.create_task(
                            credential_manager.get_valid_credential(
                                mode="antigravity", model_name=model_name
                            )
                        )

                    # 记录错误并切换凭证
                    await record_api_call_error(
                        credential_manager, current_file, status_code,
                        cooldown_until, mode="antigravity", model_name=model_name,
                        error_message=error_text
                    )

                    # 检查是否应该重试
                    should_retry = await handle_error_with_retry(
                        credential_manager, status_code, current_file,
                        retry_config["retry_enabled"], attempt, max_retries, retry_interval,
                        mode="antigravity"
                    )

                    if should_retry and attempt < max_retries:
                        need_retry = True
                    else:
                        # 不重试，直接返回原始错误
                        log.error(f"[ANTIGRAVITY] 达到最大重试次数或不应重试，返回原始错误")
                        return last_error_response
                else:
                    # 错误码不在禁用码当中，直接返回，无需重试
                    log.error(f"[ANTIGRAVITY] 非流式请求失败，非重试错误码 (status={status_code}), 凭证: {current_file}, 响应: {error_text[:500] if error_text else '无'}")
                    await record_api_call_error(
                        credential_manager, current_file, status_code,
                        None, mode="antigravity", model_name=model_name,
                        error_message=error_text
                    )
                    return last_error_response
            
            # 统一处理重试
            if need_retry:
                log.info(f"[ANTIGRAVITY] 重试请求 (attempt {attempt + 2}/{max_retries + 1})...")

                switched, next_cred_task = await _switch_credential_for_retry(
                    next_cred_task=next_cred_task,
                    retry_interval=retry_interval,
                    refresh_credential_fast=refresh_credential_fast,
                    apply_cred_result=apply_cred_result,
                    log_prefix="[ANTIGRAVITY]",
                )
                if not switched:
                    log.error("[ANTIGRAVITY] 重试时无可用凭证或令牌")
                    return Response(
                        content=json.dumps({"error": "当前无可用凭证"}),
                        status_code=500,
                        media_type="application/json"
                    )
                continue  # 重试

        except Exception as e:
            log.error(f"[ANTIGRAVITY] 非流式请求异常: {e}, 凭证: {current_file}")
            if attempt < max_retries:
                log.info(f"[ANTIGRAVITY] 异常后重试 (attempt {attempt + 2}/{max_retries + 1})...")
                await asyncio.sleep(retry_interval)
                continue
            else:
                # 所有重试都失败，返回最后一次的错误（如果有）或500错误
                log.error(f"[ANTIGRAVITY] 所有重试均失败，最后异常: {e}")
                if last_error_response:
                    return last_error_response
                else:
                    return Response(
                        content=json.dumps({"error": f"非流式请求异常: {str(e)}"}),
                        status_code=500,
                        media_type="application/json"
                    )

    # 所有重试都失败，返回最后一次的原始错误（如果有）或500错误
    log.error("[ANTIGRAVITY] 所有重试均失败")
    if last_error_response:
        return last_error_response
    else:
        return Response(
            content=json.dumps({"error": "所有重试均失败"}),
            status_code=500,
            media_type="application/json"
        )


# ==================== 模型和配额查询 ====================

async def fetch_available_models() -> List[Dict[str, Any]]:
    """
    获取可用模型列表，返回符合 OpenAI API 规范的格式
    
    Returns:
        模型列表，格式为字典列表（用于兼容现有代码）
        
    Raises:
        返回空列表如果获取失败
    """
    # 获取凭证管理器和可用凭证
    cred_result = await credential_manager.get_valid_credential(mode="antigravity")
    if not cred_result:
        log.error("[ANTIGRAVITY] No valid credentials available for fetching models")
        return []

    current_file, credential_data = cred_result
    access_token = credential_data.get("access_token") or credential_data.get("token")

    if not access_token:
        log.error(f"[ANTIGRAVITY] No access token in credential: {current_file}")
        return []

    # 构建请求头
    headers = build_antigravity_headers(access_token, model_name="agent")

    try:
        # 使用 POST 请求获取模型列表
        antigravity_url = await get_antigravity_api_url()

        response = await post_async(
            url=f"{antigravity_url}/v1internal:fetchAvailableModels",
            json={},  # 空的请求体
            headers=headers
        )

        if response.status_code == 200:
            data = response.json()
            log.debug(f"[ANTIGRAVITY] Raw models response: {json.dumps(data, ensure_ascii=False)[:500]}")

            # 转换为 OpenAI 格式的模型列表，使用 Model 类
            model_list = []
            current_timestamp = int(datetime.now(timezone.utc).timestamp())

            if 'models' in data and isinstance(data['models'], dict):
                # 遍历模型字典
                for model_id in data['models'].keys():
                    model = Model(
                        id=model_id,
                        object='model',
                        created=current_timestamp,
                        owned_by='google'
                    )
                    model_list.append(model_to_dict(model))
            # 添加额外的 claude-sonnet-4-6-thinking 模型
            if "claude-sonnet-4-6" in data.get('models', {}):
                model = Model(
                    id='claude-sonnet-4-6-thinking',
                    object='model',
                    created=current_timestamp,
                    owned_by='google'
                )
                model_list.append(model_to_dict(model))
            # 添加额外的 claude-opus-4-6 模型
            if "claude-opus-4-6-thinking" in data.get('models', {}):
                claude_opus_model = Model(
                    id='claude-opus-4-6',
                    object='model',
                    created=current_timestamp,
                    owned_by='google'
                )
                model_list.append(model_to_dict(claude_opus_model))

            log.info(f"[ANTIGRAVITY] Fetched {len(model_list)} available models")
            return model_list
        else:
            log.error(f"[ANTIGRAVITY] Failed to fetch models ({response.status_code}): {response.text[:500]}")
            return []

    except Exception as e:
        import traceback
        log.error(f"[ANTIGRAVITY] Failed to fetch models: {e}")
        log.error(f"[ANTIGRAVITY] Traceback: {traceback.format_exc()}")
        return []


async def fetch_quota_info(access_token: str) -> Dict[str, Any]:
    """
    获取指定凭证的额度信息
    
    Args:
        access_token: Antigravity 访问令牌
        
    Returns:
        包含额度信息的字典，格式为：
        {
            "success": True/False,
            "models": {
                "model_name": {
                    "remaining": 0.95,
                    "resetTime": "12-20 10:30",
                    "resetTimeRaw": "2025-12-20T02:30:00Z"
                }
            },
            "error": "错误信息" (仅在失败时)
        }
    """

    headers = build_antigravity_headers(access_token, model_name="agent")

    try:
        antigravity_url = await get_antigravity_api_url()

        response = await post_async(
            url=f"{antigravity_url}/v1internal:fetchAvailableModels",
            json={},
            headers=headers,
            timeout=30.0
        )

        if response.status_code == 200:
            data = response.json()
            log.debug(f"[ANTIGRAVITY QUOTA] Raw response: {json.dumps(data, ensure_ascii=False)[:500]}")

            quota_info = {}

            if 'models' in data and isinstance(data['models'], dict):
                for model_id, model_data in data['models'].items():
                    if isinstance(model_data, dict) and 'quotaInfo' in model_data:
                        quota = model_data['quotaInfo']
                        remaining = quota.get('remainingFraction', 0)
                        reset_time_raw = quota.get('resetTime', '')

                        # 转换为北京时间
                        reset_time_beijing = 'N/A'
                        if reset_time_raw:
                            try:
                                utc_date = datetime.fromisoformat(reset_time_raw.replace('Z', '+00:00'))
                                # 转换为北京时间 (UTC+8)
                                from datetime import timedelta
                                beijing_date = utc_date + timedelta(hours=8)
                                reset_time_beijing = beijing_date.strftime('%m-%d %H:%M')
                            except Exception as e:
                                log.warning(f"[ANTIGRAVITY QUOTA] Failed to parse reset time: {e}")

                        quota_info[model_id] = {
                            "remaining": remaining,
                            "resetTime": reset_time_beijing,
                            "resetTimeRaw": reset_time_raw
                        }

            return {
                "success": True,
                "models": quota_info
            }
        else:
            log.error(f"[ANTIGRAVITY QUOTA] Failed to fetch quota ({response.status_code}): {response.text[:500]}")
            return {
                "success": False,
                "error": f"API返回错误: {response.status_code}"
            }

    except Exception as e:
        import traceback
        log.error(f"[ANTIGRAVITY QUOTA] Failed to fetch quota: {e}")
        log.error(f"[ANTIGRAVITY QUOTA] Traceback: {traceback.format_exc()}")
        return {
            "success": False,
            "error": str(e)
        }