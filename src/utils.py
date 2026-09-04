from typing import Any, Dict, List, Optional

from config import get_api_password, get_panel_password
from fastapi import Depends, HTTPException, Header, Query, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from log import log

# HTTP Bearer security scheme
security = HTTPBearer()

# ====================== OAuth Configuration ======================

_GEMINICLI_VERSION = "0.55.1"
_GEMINICLI_PLATFORM = "win32"
_GEMINICLI_ARCH = "x64"
_GEMINICLI_SURFACE = "cloud-shell"

def get_geminicli_user_agent(model: str = "") -> str:
    """生成动态 User-Agent: GeminiCLI/{version}/{model} ({platform}; {arch}; {surface})"""
    if model:
        return f"GeminiCLI/{_GEMINICLI_VERSION}/{model} ({_GEMINICLI_PLATFORM}; {_GEMINICLI_ARCH}; {_GEMINICLI_SURFACE})"
    return f"GeminiCLI/{_GEMINICLI_VERSION} ({_GEMINICLI_PLATFORM}; {_GEMINICLI_ARCH}; {_GEMINICLI_SURFACE})"

# 静态常量
GEMINICLI_USER_AGENT = get_geminicli_user_agent()

# Antigravity CLI 客户端仿真常量
ANTIGRAVITY_CLI_VERSION = "1.1.24"
ANTIGRAVITY_CLI_PLATFORM = "windows/amd64"
ANTIGRAVITY_USER_AGENT = f"antigravity/cli/{ANTIGRAVITY_CLI_VERSION} {ANTIGRAVITY_CLI_PLATFORM}"

# OAuth Configuration - 标准模式
CLIENT_ID = "681255809395-oo8ft2oprdrnp9e3aqf6av3hmdib135j.apps.googleusercontent.com"
CLIENT_SECRET = "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"
SCOPES = [
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
]

# Antigravity OAuth Configuration
ANTIGRAVITY_CLIENT_ID = "1071006060591-tmhssin2h21lcre235vtolojh4g403ep.apps.googleusercontent.com"
ANTIGRAVITY_CLIENT_SECRET = "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"
ANTIGRAVITY_SCOPES = [
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/userinfo.email',
    'https://www.googleapis.com/auth/userinfo.profile',
    'https://www.googleapis.com/auth/cclog',
    'https://www.googleapis.com/auth/experimentsandconfigs'
]

# 统一的 Token URL（两种模式相同）
TOKEN_URL = "https://oauth2.googleapis.com/token"

# 回调服务器配置
CALLBACK_HOST = "localhost"

# Model name lists for different features
BASE_MODELS = [
    "gemini-2.5-pro",
    "gemini-2.5-flash",
    "gemini-3-flash-preview",
    "gemini-3.1-pro-preview",
    "gemini-3.1-flash-lite",
    "gemini-3.5-flash",
]


# ====================== Model Helper Functions ======================

def is_fake_streaming_model(model_name: str) -> bool:
    """Check if model name indicates fake streaming should be used."""
    return model_name.startswith("假流式/")


def is_anti_truncation_model(model_name: str) -> bool:
    """Check if model name indicates anti-truncation should be used."""
    return model_name.startswith("抗截断/")


def get_base_model_from_feature_model(model_name: str) -> str:
    """Get base model name from feature model name."""
    # Remove feature prefixes
    for prefix in ["假流式/", "抗截断/", "流式抗截断/"]:
        if model_name.startswith(prefix):
            return model_name[len(prefix) :]
    return model_name


async def apply_custom_model_mapping(model_name: str) -> str:
    """
    应用自定义模型别名映射。
    若存在配置文件/环境变量且命中别名，则替换为映射目标；
    若包含功能前缀（如 '假流式/'），替换后保留前缀；
    若未命中或无配置，原样返回。
    """
    if not model_name:
        return model_name

    from config import get_custom_model_mappings
    mappings = await get_custom_model_mappings()
    if not mappings:
        return model_name

    # 提取功能前缀
    prefix = ""
    clean_model = model_name
    for p in ["假流式/", "流式抗截断/"]:
        if model_name.startswith(p):
            prefix = p
            clean_model = model_name[len(p) :]
            break

    # 匹配 (支持精确匹配和大小写不敏感匹配)
    target_model = None
    if clean_model in mappings:
        target_model = mappings[clean_model]
    else:
        # 大小写不敏感查找
        clean_model_lower = clean_model.lower()
        for k, v in mappings.items():
            if k.lower() == clean_model_lower:
                target_model = v
                break

    if target_model:
        log.info(f"[MODEL_MAPPING] Mapped custom alias: {model_name} -> {prefix}{target_model}")
        return f"{prefix}{target_model}"

    return model_name



def get_available_models(router_type: str = "openai") -> List[str]:
    """
    Get available models with feature prefixes.

    Args:
        router_type: "openai" or "gemini"

    Returns:
        List of model names with feature prefixes
    """
    models = []

    for base_model in BASE_MODELS:
        # 基础模型
        models.append(base_model)

        # 假流式模型 (前缀格式)
        models.append(f"假流式/{base_model}")

        # 抗截断模型 (前缀格式)
        models.append(f"抗截断/{base_model}")

        # 定义思考后缀（根据模型系列不同）
        thinking_suffixes = []

        # Gemini 2.5 系列: 使用思考预算后缀
        if "gemini-2.5" in base_model:
            thinking_suffixes = ["-max", "-high", "-medium", "-low", "-minimal"]
        # Gemini 3 系列: 使用思考等级后缀
        elif "gemini-3" in base_model:
            if "flash" in base_model:
                # 3-flash-preview: 支持 high/medium/low/minimal
                thinking_suffixes = ["-high", "-medium", "-low", "-minimal"]
            elif "pro" in base_model:
                # 3-pro-preview: 支持 high/low
                thinking_suffixes = ["-high", "-low"]

        search_suffix = "-search"

        # 1. 单独的 thinking 后缀
        for thinking_suffix in thinking_suffixes:
            models.append(f"{base_model}{thinking_suffix}")
            models.append(f"假流式/{base_model}{thinking_suffix}")
            models.append(f"抗截断/{base_model}{thinking_suffix}")

        # 2. 单独的 search 后缀
        models.append(f"{base_model}{search_suffix}")
        models.append(f"假流式/{base_model}{search_suffix}")
        models.append(f"抗截断/{base_model}{search_suffix}")

        # 3. thinking + search 组合后缀
        for thinking_suffix in thinking_suffixes:
            combined_suffix = f"{thinking_suffix}{search_suffix}"
            models.append(f"{base_model}{combined_suffix}")
            models.append(f"假流式/{base_model}{combined_suffix}")
            models.append(f"抗截断/{base_model}{combined_suffix}")

    return models


# ====================== Authentication Functions ======================

async def authenticate_flexible(
    request: Request,
    authorization: Optional[str] = Header(None),
    x_api_key: Optional[str] = Header(None, alias="x-api-key"),
    access_token: Optional[str] = Header(None, alias="access_token"),
    x_goog_api_key: Optional[str] = Header(None, alias="x-goog-api-key"),
    x_anthropic_auth_token: Optional[str] = Header(None, alias="x-anthropic-auth-token"),
    anthropic_auth_token: Optional[str] = Header(None, alias="anthropic-auth-token"),
    key: Optional[str] = Query(None)
) -> str:
    """
    统一的灵活认证函数，支持多种认证方式

    此函数可以直接用作 FastAPI 的 Depends 依赖

    支持的认证方式:
        - URL 参数: key
        - HTTP 头部: Authorization (Bearer token)
        - HTTP 头部: x-api-key
        - HTTP 头部: access_token
        - HTTP 头部: x-goog-api-key
        - HTTP 头部: x-anthropic-auth-token
        - HTTP 头部: anthropic-auth-token

    Args:
        request: FastAPI Request 对象
        authorization: Authorization 头部值（自动注入）
        x_api_key: x-api-key 头部值（自动注入）
        access_token: access_token 头部值（自动注入）
        x_goog_api_key: x-goog-api-key 头部值（自动注入）
        x_anthropic_auth_token: x-anthropic-auth-token 头部值（自动注入）
        anthropic_auth_token: anthropic-auth-token 头部值（自动注入）
        key: URL 参数 key（自动注入）

    Returns:
        验证通过的token

    Raises:
        HTTPException: 认证失败时抛出异常

    使用示例:
        @router.post("/endpoint")
        async def endpoint(token: str = Depends(authenticate_flexible)):
            # token 已验证通过
            pass
    """
    password = await get_api_password()
    token = None
    auth_method = None

    # 1. 尝试从 URL 参数 key 获取（Google 官方标准方式）
    if key:
        token = key
        auth_method = "URL parameter 'key'"

    # 2. 尝试从 x-goog-api-key 头部获取（Google API 标准方式）
    elif x_goog_api_key:
        token = x_goog_api_key
        auth_method = "x-goog-api-key header"

    # 3. 尝试从 x-anthropic-auth-token 头部获取（Anthropic 标准方式）
    elif x_anthropic_auth_token:
        token = x_anthropic_auth_token
        auth_method = "x-anthropic-auth-token header"

    # 4. 尝试从 anthropic-auth-token 头部获取（Anthropic 替代方式）
    elif anthropic_auth_token:
        token = anthropic_auth_token
        auth_method = "anthropic-auth-token header"

    # 5. 尝试从 x-api-key 头部获取
    elif x_api_key:
        token = x_api_key
        auth_method = "x-api-key header"

    # 6. 尝试从 access_token 头部获取
    elif access_token:
        token = access_token
        auth_method = "access_token header"

    # 7. 尝试从 Authorization 头部获取
    elif authorization:
        if not authorization.startswith("Bearer "):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication scheme. Use 'Bearer <token>'",
                headers={"WWW-Authenticate": "Bearer"},
            )
        token = authorization[7:]  # 移除 "Bearer " 前缀
        auth_method = "Authorization Bearer header"

    # 检查是否提供了任何认证凭据
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authentication credentials. Use 'key' URL parameter, 'x-goog-api-key', 'x-anthropic-auth-token', 'anthropic-auth-token', 'x-api-key', 'access_token' header, or 'Authorization: Bearer <token>'",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # 验证 token
    if token != password:
        log.debug(f"Authentication failed using {auth_method}")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="密码错误"
        )
    
    log.debug(f"Authentication successful using {auth_method}")
    return token


# 为了保持向后兼容，保留旧函数名作为别名
authenticate_bearer = authenticate_flexible
authenticate_gemini_flexible = authenticate_flexible


# ====================== Panel Authentication Functions ======================

async def verify_panel_token(credentials: HTTPAuthorizationCredentials = Depends(security)) -> str:
    """
    简化的控制面板密码验证函数

    直接验证Bearer token是否等于控制面板密码

    Args:
        credentials: HTTPAuthorizationCredentials 自动注入

    Returns:
        验证通过的token

    Raises:
        HTTPException: 密码错误时抛出401异常
    """

    password = await get_panel_password()
    if credentials.credentials != password:
        raise HTTPException(status_code=401, detail="密码错误")
    return credentials.credentials


def record_request_log_async(
    email: Optional[str],
    mode: str,
    model: str,
    usage_metadata: Optional[Dict[str, Any]],
):
    """
    后台安全异步记录请求日志及 Token 消耗，防阻塞。
    """
    if not usage_metadata or not isinstance(usage_metadata, dict):
        return

    prompt_tokens = int(usage_metadata.get("promptTokenCount", 0) or 0)
    cached_tokens = int(usage_metadata.get("cachedContentTokenCount", 0) or 0)
    output_tokens = int(usage_metadata.get("candidatesTokenCount", 0) or 0)
    uncached_tokens = max(prompt_tokens - cached_tokens, 0)
    total_tokens = int(
        usage_metadata.get("totalTokenCount", prompt_tokens + output_tokens)
        or (prompt_tokens + output_tokens)
    )

    if prompt_tokens > 0 or output_tokens > 0 or cached_tokens > 0:
        async def _worker():
            try:
                from src.storage_adapter import get_storage_adapter

                adapter = await get_storage_adapter()
                await adapter.add_request_log(
                    email=email,
                    mode=mode,
                    model=model,
                    input_tokens=prompt_tokens,
                    output_tokens=output_tokens,
                    cached_tokens=cached_tokens,
                    uncached_tokens=uncached_tokens,
                    total_tokens=total_tokens,
                )
            except Exception as e:
                log.warning(f"Async record_request_log_async worker failed: {e}")

        import asyncio
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_worker())
        except RuntimeError:
            try:
                import threading
                threading.Thread(target=lambda: asyncio.run(_worker()), daemon=True).start()
            except Exception as e:
                log.warning(f"Async record_request_log_async failed: {e}")


from datetime import datetime, timezone, timedelta
from typing import Tuple

CHINA_TZ = timezone(timedelta(hours=8))


def get_timezone_timestamp_range(date_str: str, tz_offset_hours: int = 8) -> Tuple[float, float]:
    """
    将给定的 date_str (YYYY-MM-DD) 解析为指定时区（默认 UTC+8 北京时间）下的 00:00:00.000000 至 23:59:59.999999 的 Unix 时间戳范围
    """
    tz = timezone(timedelta(hours=tz_offset_hours))
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        dt_start = dt.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=tz)
        dt_end = dt.replace(hour=23, minute=59, second=59, microsecond=999999, tzinfo=tz)
        return dt_start.timestamp(), dt_end.timestamp()
    except Exception:
        now = datetime.now(tz=tz)
        dt_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        dt_end = now.replace(hour=23, minute=59, second=59, microsecond=999999)
        return dt_start.timestamp(), dt_end.timestamp()
