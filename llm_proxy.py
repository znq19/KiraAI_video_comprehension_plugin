"""
LLM 代理 — 多模型路由 + API 调用

每组模型固定 mode（native / frames），API Key 直接传入，
并支持自定义额外请求头（extra_headers）与额外请求体（extra_body）。
"""
from __future__ import annotations

import json

from openai import AsyncOpenAI

# 原生视频模式的大小上限（base64 后约 ×1.33，多数厂商请求体限制在几十 MB）
NATIVE_MAX_MB = 20


def _as_dict(val) -> dict:
    """把配置里的 JSON 字段统一成 dict。

    KiraAI 的 json 字段可能以 dict 或字符串形式到达（取决于前端是否已解析），
    两种都要兼容；解析失败返回空 dict（不因为一个配置项写错就整个失败）。
    """
    if isinstance(val, dict):
        return dict(val)
    if isinstance(val, (list, tuple)):
        return {}
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return {}
        try:
            parsed = json.loads(s)
        except Exception:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


class ModelProfile:
    """单组模型配置"""
    __slots__ = (
        "enabled", "group", "name", "api_base", "api_key",
        "mode", "priority", "max_video_sec", "label", "native_audio",
        "extra_headers", "extra_body",
    )

    def __init__(self, group: int, enabled: bool, name: str,
                 api_base: str | None, api_key: str,
                 mode: str, priority: int, max_video_sec: int,
                 label: str = "",
                 native_audio: bool = False,
                 extra_headers: dict | None = None,
                 extra_body: dict | None = None):
        self.enabled = enabled
        self.group = group
        self.name = name
        self.api_base = api_base or None
        self.api_key = api_key or "sk-dummy"
        self.mode = mode
        self.priority = priority
        self.max_video_sec = max_video_sec
        self.label = (label or "").strip()   # 别名，供 bot 按名字指定（如 "Agnes"）
        # 该组模型是否自带音视频理解（能自己"听"）→ 决定要不要跑 ASR 转写
        self.native_audio = bool(native_audio)
        # 附加到每次请求的自定义头/体（OpenAI SDK 的 extra_headers / extra_body）
        self.extra_headers = extra_headers or {}
        self.extra_body = extra_body or {}

    @classmethod
    def from_cfg(cls, cfg: dict, group: int):
        """从插件配置读取模型组。

        KiraAI 传给插件的 cfg 是**嵌套**结构：
            {"section_model_1": {"enabled_1": true, "model_name_1": "...", ...}, ...}
        所以必须先取 section 再取字段（旧版直接取扁平键永远读不到）。
        """
        sec = cfg.get(f"section_model_{group}")
        if not isinstance(sec, dict):
            # 兼容扁平结构（老配置/测试夹具）
            sec = cfg if f"enabled_{group}" in cfg else {}
        enabled = sec.get(f"enabled_{group}", False)
        if not enabled:
            return None
        return cls(
            group=group, enabled=True,
            name=sec.get(f"model_name_{group}", ""),
            api_base=sec.get(f"api_base_{group}", ""),
            api_key=sec.get(f"api_key_{group}", ""),
            mode=sec.get(f"mode_{group}", "native"),
            priority=int(sec.get(f"priority_{group}", 9)),
            max_video_sec=int(sec.get(f"max_video_sec_{group}", 600)),
            label=str(sec.get(f"label_{group}", "") or ""),
            native_audio=bool(sec.get(f"native_audio_{group}", False)),
            extra_headers=_as_dict(sec.get(f"extra_headers_{group}")),
            extra_body=_as_dict(sec.get(f"extra_body_{group}")),
        )


def select_model(profiles: list[ModelProfile], duration: float,
                 default_group: str = "auto") -> ModelProfile | None:
    """按优先级升序选第一个时长匹配的"""
    if default_group and default_group != "auto":
        for p in profiles:
            if str(p.group) == default_group and p.enabled:
                return p
    for p in sorted(profiles, key=lambda x: x.priority):
        if duration <= p.max_video_sec:
            return p
    # 都不匹配时返回优先级最低的
    return min(profiles, key=lambda x: x.priority) if profiles else None


def build_meta(duration: float = 0, total_frames: int = 0,
               grid_count: int = 0, scene_count: int = 0,
               timestamps: list[float] = None) -> str:
    parts = ["【视频帧分析】以下是一段视频的关键帧拼图："]
    if duration > 0:
        parts.append(f"总时长: {duration:.1f}秒")
    if total_frames > 0:
        parts.append(f"采集帧: {total_frames}")
    if grid_count > 0:
        parts.append(f"拼图: {grid_count}张（按时间顺序）")
    if scene_count > 0:
        parts.append(f"场景变化: {scene_count}次")
    if timestamps:
        parts.append(f"帧时间: {_fts(timestamps[0])} ~ {_fts(timestamps[-1])}")
    parts.append("\n每帧左上角标注了：时间戳(MM:SS.mmm)、帧类型([I]关键帧/[P])、#帧序号。拼图从左到右从上到下按时间排列。")
    return "\n".join(parts)


def _fts(s: float) -> str:
    m = int(s // 60)
    return f"{m:02d}:{s - m * 60:06.3f}"


async def analyze_gemini_native(profile: "ModelProfile", video_path: str,
                                 question: str, default_prompt: str) -> str:
    """Gemini **原生** generateContent（inline_data 传视频）。

    Gemini 的 OpenAI 兼容层不支持 video_url，但原生 API 支持把视频/音频
    直接内联进来（音视频一起理解）。这里用 Gemini 原生格式调用。

    受 Gemini inline 请求上限约束（约 20MB，base64 后算），超了会抛错，
    由上层自动降级到帧模式。
    """
    import base64, os, httpx
    size_mb = os.path.getsize(video_path) / (1024 * 1024)
    if size_mb > NATIVE_MAX_MB:
        raise RuntimeError(
            f"视频 {size_mb:.1f}MB 超过 Gemini 内联上限 {NATIVE_MAX_MB}MB，已改用帧模式"
        )
    with open(video_path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("utf-8")

    base = (profile.api_base or "https://generativelanguage.googleapis.com/v1beta").rstrip("/")
    # 归一化成 Gemini 原生路径：
    #   .../v1beta/openai  → .../v1beta        （OpenAI 兼容端点）
    #   .../v1             → .../v1beta        （OpenAI 风格的 base，中转常见）
    #   .../v1beta         → 原样
    #   其它               → 追加 /v1beta
    if base.endswith("/openai"):
        base = base[: -len("/openai")]
    if base.endswith("/v1"):
        base = base[: -len("/v1")] + "/v1beta"
    if "/v1beta" not in base:
        base = base + "/v1beta"
    url = f"{base}/models/{profile.name}:generateContent"

    payload = {
        "contents": [{
            "role": "user",
            "parts": [
                {"inline_data": {"mime_type": "video/mp4", "data": b64}},
                {"text": f"{default_prompt}\n\n{question}"},
            ],
        }],
    }
    headers = {"x-goog-api-key": profile.api_key,
               "Content-Type": "application/json"}
    headers.update(profile.extra_headers or {})

    async with httpx.AsyncClient(timeout=300) as c:
        r = await c.post(url, json={**payload, **(profile.extra_body or {})}, headers=headers)
    if r.status_code != 200:
        raise RuntimeError(f"Gemini 原生调用失败 HTTP {r.status_code}: {r.text[:200]}")
    d = r.json()
    try:
        parts = d["candidates"][0]["content"]["parts"]
        return "".join(p.get("text", "") for p in parts) or "(无回复)"
    except Exception:
        raise RuntimeError(f"Gemini 返回格式异常: {str(d)[:200]}")


async def analyze_frames(profile: ModelProfile, grids: list[str],
                          meta: str, question: str, default_prompt: str) -> str:
    """拼图帧分析。

    grids 为空 = 纯文字模式（会话画面帧被回收后仍可基于文字资料追问）：
    此时只发文本，不做任何 vision 假设。
    """
    client = AsyncOpenAI(api_key=profile.api_key, base_url=profile.api_base)
    content = [{"type": "text", "text": meta}]
    for b in grids:
        content.append({"type": "image_url", "image_url": {"url": b, "detail": "high"}})
    tail = (default_prompt + "\n\n" + question) if default_prompt else question
    content.append({"type": "text", "text": tail})
    resp = await client.chat.completions.create(
        model=profile.name,
        messages=[{"role": "user", "content": content}],
        timeout=180, max_tokens=4096,
        extra_headers=profile.extra_headers or None,
        extra_body=profile.extra_body or None,
    )
    return resp.choices[0].message.content or "(无回复)"


async def analyze_native(profile: ModelProfile, video_path: str,
                          question: str, default_prompt: str,
                          video_url: str | None = None) -> str:
    """原生视频分析。

    video_url 给定时直接用该公网链接（模型自行拉流，不受本地体积限制）；
    否则把本地文件 base64 内联（受 NATIVE_MAX_MB 限制）。
    """
    import base64, os
    # ⚠️ 硬约束：配置为 frames 的模型组**绝不能**走原生视频调用。
    #    这类模型可能根本不支持 video_url（只吃 image_url），传了会失败或行为异常。
    #    这里做防御性拦截 —— 即使将来某处调用点写错，也会立刻暴露而不是静默走错。
    if profile.mode != "native":
        raise RuntimeError(
            f"模型组「{profile.label or profile.name}」配置的是 {profile.mode} 模式，"
            f"不允许走原生视频调用（应走拼图帧模式）"
        )
    base = (profile.api_base or "").lower()
    name_l = (profile.name or "").lower()
    # ── Gemini 特判 ───────────────────────────────────────────────
    # Gemini 官方支持的视频输入只有四种：Files API（上传）、Cloud Storage、
    # 内嵌数据（base64，<100MB）、**YouTube 网址**。
    # 官方列表里**没有「任意公开 HTTPS 视频链接」**这个选项 —— 给它 B站直链
    # 之类的 URL 它不认；而它的 OpenAI 兼容层更是连 video_url 字段都没有，
    # 传了会被**静默忽略**（表现为模型回答"我没看到视频"，属假成功，最坑）。
    #
    # 所以 Gemini 只走「上传」：官方端点 → 原生 generateContent + inline_data
    #（内联上传）；第三方中转 → 明确失败，交给上层降级到帧模式，绝不假成功。
    looks_gemini = ("generativelanguage" in base) or ("gemini" in name_l)
    if looks_gemini:
        # Gemini **只走「上传」路径，绝不把公网链接发过去**：
        # Gemini 官方支持的视频输入只有 Files API(上传) / Cloud Storage /
        #   base64 内联(<100MB) / YouTube 网址 —— **没有「任意公网视频链接」**；
        # 而它的 OpenAI 兼容层连 video_url 字段都没有，传了会被**静默忽略**
        #   （模型照常返回、却说"没看到视频"，属假成功，最难排查）。
        #
        # 两条子路径：
        #   1) 官方端点 → 直接调原生 API
        #   2) 第三方中转 → **也先试原生格式**（不少中转会把
        #      /v1beta/models/…:generateContent 转发给官方）；
        #      路径不存在/不支持会抛错 → 由上层降级到帧模式（拼图）。
        #      无论哪种，都不会出现"传了链接却静默忽略"的假成功。
        return await analyze_gemini_native(profile, video_path, question,
                                           default_prompt)
    if video_url:
        url = video_url
    else:
        size_mb = os.path.getsize(video_path) / (1024 * 1024)
        if size_mb > NATIVE_MAX_MB:
            raise RuntimeError(
                f"视频 {size_mb:.1f}MB 超过原生模式上限 {NATIVE_MAX_MB}MB（base64 后约 ×1.33），已改用帧模式"
            )
        with open(video_path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("utf-8")
        url = f"data:video/mp4;base64,{b64}"
    client = AsyncOpenAI(api_key=profile.api_key, base_url=profile.api_base)
    resp = await client.chat.completions.create(
        model=profile.name,
        messages=[{
            "role": "user",
            "content": [
                {"type": "video_url", "video_url": {"url": url}},
                {"type": "text", "text": default_prompt + "\n\n" + question},
            ],
        }],
        timeout=300, max_tokens=4096,
        extra_headers=profile.extra_headers or None,
        extra_body=profile.extra_body or None,
    )
    return resp.choices[0].message.content or "(无回复)"