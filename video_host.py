"""
文件中转上传 —— 把本地视频换成公开链接。

为什么需要：部分模型（如智谱 GLM）的 video_url 只接受**公网可访问的 URL**，
不接受 base64 data URI。这里把本地文件上传到 filehost2 / 0x0.st 类服务换取 URL。

支持站点（协议相同，都是 multipart/form-data POST，字段名 file，返回纯文本 URL）：
  - x0.at      （默认，filehost2，单文件上限 1024MiB，保留 3~100 天）
  - 0x0.st     （同源实现）
  - 任何自建 filehost2 实例

响应校验：必须是单行 http(s) URL；否则视为失败（服务端出错时会返回提示文本）。
"""
from __future__ import annotations

import re
from pathlib import Path

import httpx

URL_RE = re.compile(r"^https?://\S+$")

DEFAULT_HOST = "https://litterbox.catbox.moe"
# 默认上传源顺序：litterbox（临时 24h，上传端国内可达性好）→ x0.at（filehost2，兜底）
DEFAULT_HOSTS = ["https://litterbox.catbox.moe", "https://x0.at"]

MIME_BY_EXT = {
    ".mp4": "video/mp4",
    ".mov": "video/quicktime",
    ".m4v": "video/x-m4v",
    ".mkv": "video/x-matroska",
    ".webm": "video/webm",
    ".avi": "video/x-msvideo",
    ".flv": "video/x-flv",
    ".ts": "video/mp2t",
    ".wmv": "video/x-ms-wmv",
}

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")


class UploadError(Exception):
    pass


def detect_kind(host: str) -> str:
    """按域名识别服务类型（决定 API 路径与字段名）。

    - filehost2：x0.at / 0x0.st / 自建实例 → POST / 字段 file
    - litterbox：catbox 的临时存储 → POST /resources/internals/api.php 字段 fileToUpload
    - catbox：catbox 的永久存储 → POST /user/api.php 字段 fileToUpload
    """
    h = (host or "").lower()
    if "litterbox.catbox.moe" in h:
        return "litterbox"
    if "catbox.moe" in h:
        return "catbox"
    return "filehost2"


def guess_mime(path: str) -> str:
    return MIME_BY_EXT.get(Path(path).suffix.lower(), "application/octet-stream")


def _normalize_host(host: str) -> str:
    host = (host or DEFAULT_HOST).strip().rstrip("/")
    if not host.startswith(("http://", "https://")):
        host = "https://" + host
    return host


async def upload_file(path: str, host: str = DEFAULT_HOST, *,
                      timeout: float = 300.0, keep_name: bool = True,
                      use_proxy: bool = False, litterbox_time: str = "24h") -> str:
    """上传文件到文件托管服务，返回公开 URL。

    :param path: 本地文件路径
    :param host: 上传服务地址（x0.at / 0x0.st / filehost2 / litterbox / catbox）
    :param timeout: 读写超时（大文件要放宽）
    :param keep_name: 是否让链接保留原文件名（仅 filehost2 类支持）
    :param use_proxy: 是否使用环境变量里的 HTTP 代理
    :param litterbox_time: litterbox 保留时长（1h/12h/24h/72h）
    :raises UploadError: 文件问题 / 网络失败 / 返回内容不是 URL
    """
    p = Path(path)
    if not p.is_file():
        raise UploadError(f"文件不存在: {path}")
    if p.stat().st_size == 0:
        raise UploadError(f"文件为空: {path}")

    host = _normalize_host(host)
    kind = detect_kind(host)
    if kind == "filehost2":
        url = host + "/"
        field, form = "file", ({"keep_name": "1"} if keep_name else None)
    elif kind == "litterbox":
        url = host + "/resources/internals/api.php"
        field, form = "fileToUpload", {"reqtype": "fileupload", "time": litterbox_time}
    else:  # catbox
        url = host + "/user/api.php"
        field, form = "fileToUpload", {"reqtype": "fileupload"}

    tmo = httpx.Timeout(connect=20.0, read=timeout, write=timeout, pool=20.0)
    headers = {"User-Agent": UA}

    async def _post(use_env_proxy: bool):
        with open(p, "rb") as fh:
            files = {field: (p.name, fh, guess_mime(str(p)))}
            async with httpx.AsyncClient(headers=headers, timeout=tmo, follow_redirects=True,
                                         trust_env=use_env_proxy) as c:
                return await c.post(url, data=form, files=files)

    last_err = None
    # 先按配置直连/走代理；失败再换另一种方式试一次
    for flag in ([use_proxy] if use_proxy else [False, True]):
        try:
            resp = await _post(flag)
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            continue
        body = (resp.text or "").strip()
        if resp.status_code != 200:
            last_err = f"HTTP {resp.status_code}: {body[:160]}"
            continue
        if not URL_RE.match(body):
            last_err = f"返回内容不是 URL: {body[:160]}"
            continue
        return body

    raise UploadError(f"上传失败（{host}）: {last_err}")


async def upload_to_any(path: str, hosts, **kwargs) -> tuple:
    """依次尝试多个上传源，返回 (url, 成功的 host)。

    全部失败时抛 UploadError（含每个源的失败原因）。
    """
    if isinstance(hosts, str):
        hosts = [hosts]
    hosts = [_normalize_host(h) for h in (hosts or []) if str(h).strip()] or list(DEFAULT_HOSTS)
    errs = []
    for h in hosts:
        try:
            url = await upload_file(path, h, **kwargs)
            return url, h
        except UploadError as e:
            errs.append(f"{h}: {e}")
        except Exception as e:
            errs.append(f"{h}: {type(e).__name__}: {e}")
    raise UploadError("全部上传源失败 → " + " | ".join(errs))
