"""
语音转写 —— 走 OpenAI 兼容的 `/audio/transcriptions`。

设计目标是**尽量不切块**：
  1) 先带 `response_format=verbose_json` + `timestamp_granularities[]`
     请求原生时间戳（Whisper 系 / Groq / 本地 faster-whisper 都支持）
  2) 服务不支持就降级为纯文本（如硅基流动 SenseVoice —— 只返回 text）
  3) 调方拿不到分段时，再由调用方决定是否用本地 VAD 切块补轴

返回格式兼容面尽量宽：
  - OpenAI style: {"text":…, "segments":[{"start","end","text"}]}
  - 阿里云 style: {"text":…, "sentences":[{"begin_time","end_time","text"}]}（毫秒）
  - 词级: {"words":[{"word","start","end"}]} → 按 1.2s 间隔合并成句
  - 只有 {"text": …}，甚至直接返回裸字符串
时间值自动识别毫秒/秒。
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import httpx

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

_START_KEYS = ("start", "start_time", "begin_time", "from", "startTime", "beginTime", "offset")
_END_KEYS = ("end", "end_time", "stop", "to", "endTime", "finish_time")
_TEXT_KEYS = ("text", "content", "sentence", "transcript", "utterance", "words")

# 这些字段名（阿里云 Paraformer 系）返回的是**毫秒**
_MS_KEYS = ("begin_time", "end_time", "beginTime", "endTime")

AUDIO_MIME = {
    ".wav": "audio/wav", ".mp3": "audio/mpeg", ".m4a": "audio/mp4",
    ".aac": "audio/aac", ".flac": "audio/flac", ".ogg": "audio/ogg",
    ".opus": "audio/opus", ".amr": "audio/amr", ".webm": "audio/webm",
}


class ASRError(Exception):
    pass


def _pick(d: dict, keys):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def _to_seconds(v, is_ms: bool = False) -> float:
    """时间值归一化成秒。is_ms 为真按毫秒处理（阿里云 begin_time/end_time 惯例）。"""
    try:
        x = float(v)
    except (TypeError, ValueError):
        return 0.0
    return x / 1000.0 if is_ms else x


def _pick_time(d: dict, keys) -> float:
    """按字段名取时间：命中毫秒字段名就按毫秒换算"""
    for k in keys:
        if k in d and d[k] is not None:
            return _to_seconds(d[k], is_ms=(k in _MS_KEYS))
    return 0.0


def normalize_segments_by_duration(segments, duration: float, factor: float = 10.0):
    """兜底校正：若分段最大时间远超音频实际时长，说明是毫秒，统一换算成秒。

    有些服务用 start_time/end_time 却给毫秒，字段名线索不够时靠这条兜住。
    """
    if not segments or duration <= 0:
        return segments
    try:
        mx = max(float(s.get("end") or 0) for s in segments)
    except Exception:
        return segments
    if mx > duration * factor:
        for s in segments:
            s["start"] = float(s.get("start") or 0) / 1000.0
            s["end"] = float(s.get("end") or 0) / 1000.0
    return segments


def parse_transcript(payload) -> dict:
    """把各种 ASR 返回归一化成 {text, segments, source}"""
    if isinstance(payload, str):
        s = payload.strip()
        if not s:
            return {"text": "", "segments": [], "source": "empty"}
        try:                                  # 有些服务把 JSON 塞在字符串里
            return parse_transcript(json.loads(s))
        except Exception:
            return {"text": s, "segments": [], "source": "text"}

    if not isinstance(payload, dict):
        return {"text": "", "segments": [], "source": "empty"}

    text = str(_pick(payload, ("text", "result", "transcript", "content")) or "").strip()

    segs = []
    for key in ("segments", "sentences", "utterances", "results", "items"):
        arr = payload.get(key)
        if isinstance(arr, list):
            for it in arr:
                if not isinstance(it, dict):
                    continue
                tx = _pick(it, _TEXT_KEYS)
                if tx is None:
                    continue
                tx = str(tx).strip()
                if not tx:
                    continue
                segs.append({"start": _pick_time(it, _START_KEYS),
                             "end": _pick_time(it, _END_KEYS),
                             "text": tx})
            if segs:
                break

    if not segs:
        arr = payload.get("words")
        if isinstance(arr, list):
            words = []
            for it in arr:
                if not isinstance(it, dict):
                    continue
                w = str(_pick(it, ("word", "text")) or "").strip()
                if not w:
                    continue
                words.append({"start": _pick_time(it, _START_KEYS),
                              "end": _pick_time(it, _END_KEYS),
                              "text": w})
            # 词级 → 按 1.2 秒间隔合并成句
            cur = None
            for w in words:
                if cur and w["start"] - cur["end"] <= 1.2:
                    cur["text"] += w["text"]
                    cur["end"] = w["end"]
                else:
                    if cur: segs.append(cur)
                    cur = dict(w)
            if cur: segs.append(cur)

    if not text and segs:
        text = "".join(s["text"] for s in segs)

    return {"text": text, "segments": segs,
            "source": "segments" if segs else ("text" if text else "empty")}


async def transcribe(audio_path: str, base_url: str, api_key: str, model: str, *,
                     timeout: float = 300.0, language: str = "",
                     prompt_text: str = "", use_proxy: bool = False,
                     extra_headers: dict | None = None,
                     extra_body: dict | None = None) -> dict:
    """调用 OpenAI 兼容的 /audio/transcriptions。

    先尝试拿原生时间戳（verbose_json），失败再退回纯文本。

    :param extra_headers: 额外请求头（JSON 对象），合并进 HTTP 头
    :param extra_body: 额外**表单字段**（JSON 对象）。ASR 走的是 multipart/form-data，
                       所以这里是"再往表单里塞几个字段"，如 {"temperature": 0}，
                       不是 JSON body 合并。插件自己设置的字段（file/model/
                       response_format 等）不会被覆盖。
    """
    p = Path(audio_path)
    if not p.is_file() or p.stat().st_size == 0:
        raise ASRError(f"音频文件不存在或为空: {audio_path}")

    base = (base_url or "").strip().rstrip("/")
    if not base:
        raise ASRError("未配置 ASR base_url")
    if not base.startswith(("http://", "https://")):
        base = "https://" + base
    url = base + "/audio/transcriptions"
    mime = AUDIO_MIME.get(p.suffix.lower(), "application/octet-stream")
    tmo = httpx.Timeout(connect=20.0, read=timeout, write=timeout, pool=20.0)

    headers = {"User-Agent": UA}
    if api_key:
        # ⚠️ 之前漏了这行：认证头根本没发出去，导致所有请求 401
        headers["Authorization"] = f"Bearer {api_key}"
    for k, v in (extra_headers or {}).items():
        try:
            headers[str(k)] = str(v)     # 自定义头可覆盖上面的默认鉴权方式
        except Exception:
            continue

    async def _post(form: dict, use_env_proxy: bool):
        with open(p, "rb") as fh:
            files = {"file": (p.name, fh, mime)}
            async with httpx.AsyncClient(headers=headers, timeout=tmo,
                                         follow_redirects=True, trust_env=use_env_proxy) as c:
                return await c.post(url, data=form, files=files)

    base_form = {"model": model or "whisper-1"}
    if language:
        base_form["language"] = language
    if prompt_text:
        base_form["prompt"] = prompt_text
    # 额外表单字段（用户自定义；不覆盖上面的关键字段）
    for k, v in (extra_body or {}).items():
        k = str(k)
        if not k or k in base_form or k == "file":
            continue
        if isinstance(v, (list, tuple)):
            base_form[k] = [x if isinstance(x, str) else json.dumps(x, ensure_ascii=False)
                            for x in v]
        elif isinstance(v, (dict, bool)):
            base_form[k] = json.dumps(v, ensure_ascii=False)
        elif isinstance(v, (int, float)):
            base_form[k] = str(v)
        else:
            base_form[k] = str(v)

    attempts = [
        {**base_form, "response_format": "verbose_json",
         "timestamp_granularities[]": "segment"},
        {**base_form, "response_format": "verbose_json"},
        dict(base_form),                       # 最后退回默认（只要文本）
    ]

    last_err = None
    for form in attempts:
        for flag in ([use_proxy] if use_proxy else [False, True]):
            try:
                resp = await _post(form, flag)
            except Exception as e:
                last_err = f"{type(e).__name__}: {e}"
                continue
            if resp.status_code != 200:
                last_err = f"HTTP {resp.status_code}: {(resp.text or '')[:200]}"
                continue
            body = resp.text or ""
            try:
                payload = json.loads(body)
            except Exception:
                payload = None
            parsed = parse_transcript(payload if payload is not None else body)
            # ⚠️ HTTP 200 + 合法 JSON 且含 text 字段 = **有效响应**，
            #    即使 text 为空也属于"这段音频没有人声"，不能当失败去重试
            #    （否则一个静音视频会白调 3 次 ASR）
            if (parsed.get("text") or parsed.get("segments")
                    or (isinstance(payload, dict) and "text" in payload)):
                parsed["http_form"] = form.get("response_format", "default")
                if not parsed.get("text") and not parsed.get("segments"):
                    parsed["silent"] = True          # 标记：识别成功但无语音
                return parsed
            last_err = f"返回内容既没有 text 也不是可解析 JSON: {body[:160]}"
    raise ASRError(f"转写失败: {last_err}")


# ── 时间轴组装（含 L2 非语音段） ────────────────────────────

def _fmt(sec: float) -> str:
    m = int(sec // 60)
    return f"{m:02d}:{sec - m * 60:06.3f}"


def subtract_speech(speech_ranges, segments, min_len: float = 0.6):
    """有声段 － 已识别语音段 = 非语音声音段（音乐/音效/环境音）"""
    out = []
    for rs, re_ in speech_ranges or []:
        covered = sorted(
            (max(rs, float(g.get("start") or 0)), min(re_, float(g.get("end") or 0)))
            for g in (segments or [])
            if float(g.get("end") or 0) > rs and float(g.get("start") or 0) < re_
        )
        cursor = rs
        for cs, ce in covered:
            if cs - cursor >= min_len:
                out.append((cursor, cs))
            cursor = max(cursor, ce)
        if re_ - cursor >= min_len:
            out.append((cursor, re_))
    return out


def build_timeline_doc(segments, speech_ranges=None, total_dur: float = 0,
                       header: str | None = None) -> str:
    """把 ASR 分段 + 非语音段组装成给模型看的时间轴文档。"""
    segs = sorted((s for s in (segments or []) if (s.get("text") or "").strip()),
                  key=lambda x: float(x.get("start") or 0))
    nonspeech = subtract_speech(speech_ranges, segs) if speech_ranges else []

    lines = []
    for s in segs:
        st, en = float(s.get("start") or 0), float(s.get("end") or 0)
        span = f"{_fmt(st)} → {_fmt(en)}" if en > st else _fmt(st)
        lines.append(f"[{span}] {s['text'].strip()}")

    for st, en in nonspeech:
        lines.append(f"[{_fmt(st)} → {_fmt(en)}] 【非语音声音】持续 {en - st:.1f} 秒，"
                     f"该段有声音但未识别出人声（可能是音乐/音效/环境音/杂音）")

    if not lines:
        return ""

    # 按时间重排（解析行首时间）
    def _key(line: str):
        m = re.match(r"\[(\d+):([\d.]+)", line)
        return (int(m.group(1)) * 60 + float(m.group(2))) if m else 1e9
    lines.sort(key=_key)

    head = header or ("【视频语音转写（自动识别，可能有误；时间轴与画面帧上的时间戳口径一致）】")
    body = "\n".join(lines)
    tail = ""
    if total_dur > 0 and not segs:
        tail = f"\n（整段音频约 {total_dur:.0f} 秒，未识别出可转写的语音内容）"
    return f"{head}\n{body}{tail}"
