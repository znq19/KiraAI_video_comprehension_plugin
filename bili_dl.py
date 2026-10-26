"""
Bilibili API 封装 — 搜索/信息/AI总结/视频下载
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import shutil
import time
import urllib.parse
from pathlib import Path

import httpx

from process_utils import run_subprocess

try:                      # 有 KiraAI 就用它的 logger，没有也不影响独立使用
    from core.plugin import logger
except Exception:         # pragma: no cover
    import logging
    logger = logging.getLogger("VC.bili")

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
REFERER = "https://www.bilibili.com"
HEADERS = {"User-Agent": UA, "Referer": REFERER}
BV_RE = re.compile(r"BV[0-9A-Za-z]{10}")
SHORT_RE = re.compile(r"(?:https?://)?b23\.tv/[0-9A-Za-z]+")
API_BASE = "https://api.bilibili.com"


class BiliError(Exception): pass
class BiliNetError(BiliError): pass


def _client(cookie="", timeout=60.0, use_proxy=False):
    h = dict(HEADERS)
    if cookie: h["Cookie"] = cookie
    return httpx.AsyncClient(headers=h, follow_redirects=True, timeout=timeout, trust_env=use_proxy)


async def _get_json(client, url, params=None):
    try:
        resp = await client.get(url, params=params)
    except httpx.HTTPError as e:
        raise BiliNetError(f"网络请求失败: {e}") from e
    try: return resp.json()
    except ValueError:
        raise BiliError(f"B站接口返回异常 (HTTP {resp.status_code})") from None


class _Requester:
    def __init__(self, cookie="", timeout=60.0, proxy_mode="auto"):
        self.cookie = cookie; self.timeout = timeout; self.proxy_mode = proxy_mode
    async def _get_once(self, url, params, use_proxy):
        async with _client(self.cookie, self.timeout, use_proxy) as c:
            return await _get_json(c, url, params)
    async def get_json(self, url, params=None):
        try: return await self._get_once(url, params, False)
        except BiliNetError:
            if self.proxy_mode != "auto": raise
            return await self._get_once(url, params, True)
    async def stream(self, url, fpath):
        """流式写 .part，成功后原子替换；网络取消时不会留下半截正式缓存。"""
        fpath = Path(fpath)
        tmp = Path(str(fpath) + ".part")
        for use_proxy in (False, True):
            try:
                async with _client(self.cookie, self.timeout, use_proxy) as c:
                    async with c.stream("GET", url) as resp:
                        if resp.status_code != 200:
                            raise BiliError(f"下载失败 HTTP {resp.status_code}")
                        with open(tmp, "wb") as f:
                            async for chunk in resp.aiter_bytes(1024 * 1024):
                                await asyncio.to_thread(f.write, chunk)
                await asyncio.to_thread(os.replace, tmp, fpath)
                return
            except (BiliNetError, httpx.HTTPError):
                try:
                    tmp.unlink()
                except OSError:
                    pass
                if self.proxy_mode != "auto" or use_proxy:
                    raise
            except BaseException:
                try:
                    tmp.unlink()
                except OSError:
                    pass
                raise


# WBI 签名固定重排表（B站前端 mixinKeyEncTab）
_MIXIN_KEY_ENC_TAB = [
    46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35, 27, 43, 5, 49,
    33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13, 37, 48, 7, 16, 24, 55, 40,
    61, 26, 17, 0, 1, 60, 51, 30, 4, 22, 25, 54, 21, 56, 59, 6, 63, 57, 62, 11,
    36, 20, 34, 44, 52,
]


def _mixin_key(orig: str) -> str:
    return "".join(orig[i] for i in _MIXIN_KEY_ENC_TAB)[:32]


async def _wbi_sign(req, params):
    """B站 WBI 签名。

    算法：img_key + sub_key → 按 mixinKeyEncTab 重排取前 32 位 = mixin_key；
    参数转字符串 + 追加 wts → 按 key 排序 urlencode → 去掉 !'()* 字符 →
    md5(query + mixin_key) = w_rid。

    旧实现用 sorted([img, sub]) 拼串、不做 urlencode，签名恒不通过（接口返回 -403）。
    """
    nav = await req.get_json(f"{API_BASE}/x/web-interface/nav")
    wbi = nav.get("data", {}).get("wbi_img", {}) or {}
    img_key = (wbi.get("img_url") or "").rsplit("/", 1)[-1].split(".")[0]
    sub_key = (wbi.get("sub_url") or "").rsplit("/", 1)[-1].split(".")[0]
    if not img_key or not sub_key: raise BiliError("WBI签名初始化失败")
    mixin_key = _mixin_key(img_key + sub_key)
    p = {k: str(v) for k, v in params.items()}
    p["wts"] = int(time.time())
    query = urllib.parse.urlencode(sorted(p.items()))
    query = re.sub(r"[!'()*]", "", query)
    p["w_rid"] = hashlib.md5((query + mixin_key).encode()).hexdigest()
    return p


# ══════════════════════════════════════════════════════
#  搜索 — duration 可能是 "4:24" 字符串
# ══════════════════════════════════════════════════════

def _parse_dur(dur) -> int:
    if isinstance(dur, (int, float)): return int(dur)
    if isinstance(dur, str):
        dur = dur.strip()
        if ":" in dur:
            parts = dur.split(":")
            if len(parts) == 2:
                try: return int(parts[0])*60 + int(parts[1])
                except: pass
        try: return int(dur)
        except: pass
    return 0

def _parse_num(val) -> int:
    if isinstance(val, (int, float)): return int(val)
    if isinstance(val, str):
        try: return int(val.replace(",", "").strip())
        except: return 0
    return 0


async def search_bili(keyword, count=5, cookie="", proxy_mode="auto"):
    req = _Requester(cookie, proxy_mode=proxy_mode)
    d = await req.get_json(f"{API_BASE}/x/web-interface/search/all/v2", {"keyword": keyword, "page": 1})
    if d.get("code") != 0: raise BiliError(f"搜索失败 (code={d.get('code')})")
    results = []
    for item in d.get("data", {}).get("result", []):
        if item.get("result_type") != "video": continue
        for v in item.get("data", [])[:count]:
            results.append({
                "bvid": v.get("bvid", ""),
                "title": re.sub(r"<[^>]+>", "", v.get("title", "")),
                "author": v.get("author", ""),
                "duration": _parse_dur(v.get("duration", 0)),
                "play": _parse_num(v.get("play", 0)),
                "desc": re.sub(r"<[^>]+>", "", v.get("description", "") or ""),
            })
        break
    return results[:count]


# ══════════════════════════════════════════════════════
#  视频信息
# ══════════════════════════════════════════════════════

async def get_bili_info(bvid, cookie="", timeout=60.0, proxy_mode="auto"):
    req = _Requester(cookie, timeout, proxy_mode)
    d = await req.get_json(f"{API_BASE}/x/web-interface/view", {"bvid": bvid})
    if d.get("code") != 0: raise BiliError(f"获取信息失败 (code={d.get('code')})")
    data = d["data"]
    return {
        "bvid": bvid, "cid": data.get("cid", 0),
        "title": data.get("title", bvid),
        "duration": int(data.get("duration", 0)),
        "desc": data.get("desc", ""),
        "up_mid": data.get("owner", {}).get("mid", 0),
        "pic": data.get("pic", ""),
    }


# ══════════════════════════════════════════════════════
#  AI 总结
# ══════════════════════════════════════════════════════

async def get_ai_summary(bvid, cid, up_mid=0, cookie="", proxy_mode="auto"):
    req = _Requester(cookie, proxy_mode=proxy_mode)
    params = {"bvid": bvid, "cid": cid}
    if up_mid: params["up_mid"] = up_mid
    signed = await _wbi_sign(req, params)
    d = await req.get_json(f"{API_BASE}/x/web-interface/view/conclusion/get", signed)
    if d.get("code") != 0: raise BiliError(f"AI总结接口失败 (code={d.get('code')})")
    data = d.get("data", {}); code = data.get("code", -1)
    if code == -1: return {"has_summary": False, "reason": "不支持AI摘要"}
    if code == 1: return {"has_summary": False, "reason": "无AI摘要（正在生成或无语音）"}
    model = data.get("model_result", {})
    if model.get("result_type", 0) == 0: return {"has_summary": False, "reason": "AI摘要为空"}
    return {
        "has_summary": True,
        "summary": model.get("summary", ""),
        "outline": model.get("outline", []),
        "subtitle": model.get("subtitle", []),
    }


# ══════════════════════════════════════════════════════
#  下载视频（音视频合并）
# ══════════════════════════════════════════════════════

async def _get_video_urls(req, bvid, cid, quality_hint: int = 0):
    """获取视频流直链，quality_hint 按高度过滤：0=最高, 360=低, 720=中, 1080=高"""
    params = {"cid": cid, "bvid": bvid, "fnval": 4048, "fnver": 0, "fourk": 1, "platform": "web"}
    d = await req.get_json(f"{API_BASE}/x/player/playurl", params)
    if d.get("code") != 0:
        signed = await _wbi_sign(req, params)
        d = await req.get_json(f"{API_BASE}/x/player/wbi/playurl", signed)
    if d.get("code") != 0: raise BiliError(f"获取视频流失败 (code={d.get('code')})")
    data = d.get("data", {})
    dash = data.get("dash", {})
    video = dash.get("video", [])
    audio = dash.get("audio", [])
    audio_url = audio[0]["baseUrl"] if audio else None

    # original 必须选最高档；低/中档选不超过上限的最高流，都不依赖 API 返回顺序。
    def _num(v, key):
        try:
            return int(v.get(key, 0) or 0)
        except (TypeError, ValueError):
            return 0

    ordered = sorted(video, key=lambda v: (_num(v, "height"), _num(v, "bandwidth")),
                     reverse=True)
    chosen = None
    if quality_hint > 0:
        candidates = [v for v in ordered if 0 < _num(v, "height") <= quality_hint]
        chosen = candidates[0] if candidates else (ordered[-1] if ordered else None)
    elif ordered:
        chosen = ordered[0]
    video_url = chosen.get("baseUrl") if chosen else None
    # mp4 兜底
    durl = data.get("durl", [])
    return {"video_url": video_url or (durl[0]["url"] if durl else None), "audio_url": audio_url}


async def get_bili_direct_url(bvid, cid, cookie="", timeout=30.0, proxy_mode="auto"):
    """取 B 站 **html5 MP4 直链**（音视频合一）。

    用 `fnval=1 + platform=html5` 走 B 站的 MP4 接口，特点是：
      - **不需要 Referer**（实测不带 Referer 也能拉）
      - **音视频合一**：下载后不用 ffmpeg 合并
      - 代价：清晰度上限较低（通常 360p/480p），但足够给模型分析

    返回 (url, quality_code, length_ms)。
    """
    req = _Requester(cookie, timeout, proxy_mode)
    params = {"bvid": bvid, "cid": cid, "fnval": 1, "platform": "html5", "high_quality": 1}
    d = await req.get_json(f"{API_BASE}/x/player/playurl", params)
    if d.get("code") != 0:
        try:
            signed = await _wbi_sign(req, params)
            d = await req.get_json(f"{API_BASE}/x/player/wbi/playurl", signed)
        except Exception:
            pass
    if d.get("code") != 0:
        raise BiliError(f"获取B站直链失败 (code={d.get('code')})")
    data = d.get("data") or {}
    durl = data.get("durl") or []
    if not durl:
        raise BiliError("该视频没有可用的 MP4 直链")
    first = durl[0]
    url = (first.get("url") or "").strip()
    if not url:
        raise BiliError("MP4 直链为空")
    return url, int(data.get("quality") or 0), int(first.get("length") or 0)


def _unlink_quiet(path):
    try:
        Path(path).unlink()
    except OSError:
        pass


async def _valid_cached_video(path) -> bool:
    """非空还不够：有 ffprobe 时必须能解析出正时长，排除旧半截缓存。"""
    p = Path(path)
    try:
        if not p.is_file() or p.stat().st_size == 0:
            return False
    except OSError:
        return False
    if not shutil.which("ffprobe"):
        return True
    cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration",
           "-of", "json", str(p)]
    try:
        r = await run_subprocess(cmd, timeout=30)
        if r.returncode != 0:
            return False
        duration = float((json.loads(r.stdout or "{}").get("format") or {}).get("duration") or 0)
        return duration > 0
    except Exception:
        return False


async def download_bili_video(bvid, out_dir, info=None, cookie="",
                               timeout=120.0, max_seconds=0, proxy_mode="auto",
                               quality: str = "",
                               prefer_html5: bool = True) -> tuple[str, dict]:
    """按质量下载并原子写缓存；low 保留 html5 免合并优先策略。"""
    quality = quality if quality in ("low", "medium", "original") else "low"
    quality_hint = {"low": 360, "medium": 720, "original": 0}[quality]
    # medium/original 需要对应 DASH 档，不能被 html5 的较低清晰度悄悄替代。
    prefer_html5 = bool(prefer_html5 and quality == "low")
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    req = _Requester(cookie, timeout, proxy_mode)
    info = info or await get_bili_info(bvid, cookie, timeout, proxy_mode)
    if max_seconds and info["duration"] > max_seconds:
        raise BiliError(f"视频时长 {info['duration']}s 超过上限 {max_seconds}s")
    # 保留旧 low 文件命名；其他质量分开缓存，避免低清文件冒充原画。
    fname = f"{bvid}.mp4" if quality == "low" else f"{bvid}_{quality}.mp4"
    fpath = out_dir / fname
    if await _valid_cached_video(fpath):
        return str(fpath), info

    if prefer_html5:
        try:
            direct, qcode, _ms = await get_bili_direct_url(
                bvid, info["cid"], cookie, timeout, proxy_mode)
            await req.stream(direct, fpath)
            if await _valid_cached_video(fpath):
                logger.info("[VC] B站视频下载完成（html5 MP4 单文件，quality=%s）", qcode)
                return str(fpath), info
        except Exception as e:
            logger.info("[VC] html5 MP4 直链不可用，回退 DASH 流程: %s", e)
            _unlink_quiet(str(fpath) + ".part")

    urls = await _get_video_urls(req, bvid, info["cid"], quality_hint=quality_hint)
    if not urls["video_url"]:
        raise BiliError("无法获取视频下载链接")

    vpath = out_dir / f"{fpath.stem}_v.mp4"
    apath = out_dir / f"{fpath.stem}_a.m4a"
    await req.stream(urls["video_url"], vpath)
    if not vpath.exists() or vpath.stat().st_size == 0:
        raise BiliError("视频流下载文件为空")

    has_audio = bool(urls.get("audio_url"))
    if has_audio:
        try:
            await req.stream(urls["audio_url"], apath)
            if not apath.exists() or apath.stat().st_size == 0:
                has_audio = False
        except Exception:
            has_audio = False

    if has_audio:
        merge_path = out_dir / f".{fpath.stem}_merge.mp4"
        try:
            r = await run_subprocess([
                "ffmpeg", "-y", "-i", str(vpath), "-i", str(apath),
                "-c:v", "copy", "-c:a", "aac", "-b:a", "128k",
                "-movflags", "+faststart", str(merge_path),
            ], timeout=300)
            if r.returncode != 0:
                raise BiliError(f"音视频合并失败: {r.stderr[:200]}")
            await asyncio.to_thread(os.replace, merge_path, fpath)
            _unlink_quiet(vpath)
            _unlink_quiet(apath)
        except FileNotFoundError:
            # 没有 ffmpeg 时保留旧降级：仅视频流，不伪称含音轨。
            await asyncio.to_thread(os.replace, vpath, fpath)
        except asyncio.CancelledError:
            _unlink_quiet(merge_path)
            _unlink_quiet(vpath)
            _unlink_quiet(apath)
            raise
        except Exception as e:
            _unlink_quiet(merge_path)
            _unlink_quiet(vpath)
            _unlink_quiet(apath)
            if isinstance(e, BiliError):
                raise
            raise BiliError(f"音视频合并异常: {e}") from e
    else:
        await asyncio.to_thread(os.replace, vpath, fpath)

    _unlink_quiet(apath)
    if not await _valid_cached_video(fpath):
        raise BiliError("最终视频文件无效或不完整")
    return str(fpath), info


# ══════════════════════════════════════════════════════
#  字幕（B 站官方 CC/AI 字幕，带精确时间轴）
# ══════════════════════════════════════════════════════

# 语言优先级：中文优先，其次英文
_SUB_LAN_PRIORITY = ("zh-cn", "zh-hans", "zh", "ai-zh", "zh-tw", "ai-zh-tw", "en")


async def get_subtitle_tracks(bvid, cid, cookie="", timeout=30.0, proxy_mode="auto"):
    """取字幕轨列表 → [{lan, lan_doc, url}]

    优先用 wbi 签名端点（新版 B 站要求），失败回退旧端点。
    AI 生成的字幕需要登录 cookie 才会返回。
    """
    req = _Requester(cookie, timeout, proxy_mode)
    params = {"bvid": bvid, "cid": cid}
    data = None
    try:
        signed = await _wbi_sign(req, params)
        d = await req.get_json(f"{API_BASE}/x/player/wbi/v2", signed)
        if d.get("code") == 0:
            data = d.get("data")
    except Exception:
        pass
    if not data:
        try:
            d = await req.get_json(f"{API_BASE}/x/player/v2", params)
            if d.get("code") == 0:
                data = d.get("data")
        except Exception:
            pass
    if not data:
        return []
    subs = (data.get("subtitle") or {}).get("subtitles") or []
    out = []
    for s in subs:
        u = (s.get("subtitle_url") or "").strip()
        if not u:
            continue
        if u.startswith("//"):
            u = "https:" + u
        elif u.startswith("/"):
            u = "https://aisubtitle.hdslb.com" + u
        out.append({"lan": (s.get("lan") or "").strip(),
                    "lan_doc": (s.get("lan_doc") or "").strip(),
                    "url": u})
    return out


def pick_subtitle_track(tracks, prefer=""):
    """按语言挑一条字幕轨"""
    if not tracks:
        return None
    if prefer:
        p = prefer.strip().lower()
        for t in tracks:
            if t["lan"].lower() == p:
                return t
    for want in _SUB_LAN_PRIORITY:
        for t in tracks:
            if t["lan"].lower() == want:
                return t
    return tracks[0]


async def fetch_subtitle_segments(url, timeout=60.0, proxy_mode="auto"):
    """下载字幕 JSON → [{start, end, text}]（与 ASR 分段同构）"""
    req = _Requester("", timeout, proxy_mode)
    d = await req.get_json(url)
    body = (d or {}).get("body") or []
    segs = []
    for it in body:
        if not isinstance(it, dict):
            continue
        txt = str(it.get("content") or "").strip()
        if not txt:
            continue
        try:
            st = float(it.get("from") or 0)
            en = float(it.get("to") or 0)
        except (TypeError, ValueError):
            continue
        segs.append({"start": st, "end": en, "text": txt})
    return segs


async def get_bilibili_subtitle(bvid, cid, cookie="", prefer_lan="", timeout=30.0,
                                proxy_mode="auto"):
    """一步到位：取字幕轨 → 挑一条 → 下载 → 返回 (segments, lan_doc, 轨数量)

    没有字幕时返回 ([], "", 0)。
    """
    tracks = await get_subtitle_tracks(bvid, cid, cookie, timeout, proxy_mode)
    if not tracks:
        return [], "", 0
    track = pick_subtitle_track(tracks, prefer_lan) or tracks[0]
    segs = await fetch_subtitle_segments(track["url"], timeout=timeout,
                                         proxy_mode=proxy_mode)
    return segs, track.get("lan_doc") or track.get("lan") or "", len(tracks)


# ══════════════════════════════════════════════════════
#  BV 号提取
# ══════════════════════════════════════════════════════

async def extract_bvid(text, timeout=15.0, proxy_mode="auto"):
    m = BV_RE.search(text)
    if m: return m.group(0)
    m2 = SHORT_RE.search(text)
    if m2:
        url = m2.group(0)
        if not url.startswith("http"):
            url = "https://" + url
        url = url.split("?")[0]
        # 直连和代理都试
        for use_proxy in (False, True):
            try:
                async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True, timeout=timeout, trust_env=use_proxy) as c:
                    r = await c.get(url)
                    final_url = str(r.url)
                m3 = BV_RE.search(final_url)
                if m3: return m3.group(0)
            except Exception:
                continue
    return ""