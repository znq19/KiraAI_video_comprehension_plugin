"""
Video Comprehension 插件 — QQ/B站视频理解

功能：
  - send_video 工具：下载B站视频+压缩+发送到QQ
  - search_bili_video 工具：搜索B站视频
  - analyze_video 工具（默认关）：视频内容分析
  - 自动链接检测钩子（默认关）
  - 缓存双向：B站 → files/video_cache/，其他 → files/video_analysis_cache/
"""
from __future__ import annotations

# ── 导入区 ──

import asyncio
import base64
import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Optional

_PLUGIN_DIR = os.path.dirname(os.path.abspath(__file__))
if _PLUGIN_DIR not in sys.path:
    sys.path.insert(0, _PLUGIN_DIR)

from core.plugin import BasePlugin, logger, on, Priority, register
from core.chat.message_utils import KiraMessageEvent, KiraMessageBatchEvent
from core.chat import MessageChain
from core.chat.message_elements import Text
from core.provider import LLMRequest
from core.utils.path_utils import get_data_path

from video_processor import (process_video, compress_video, clip_video, download_video,
                             extract_audio, detect_speech_ranges, merge_ranges, slice_audio)
from asr import (transcribe as asr_transcribe, build_timeline_doc, ASRError,
                  normalize_segments_by_duration)
from video_host import upload_to_any, UploadError, DEFAULT_HOSTS as DEFAULT_UPLOAD_HOSTS
from llm_proxy import (ModelProfile, select_model, build_meta, analyze_frames,
                       analyze_native, NATIVE_MAX_MB, _as_dict)
from bili_dl import (search_bili, get_bili_info, get_ai_summary, download_bili_video,
                     extract_bvid, BiliError, get_bilibili_subtitle,
                     get_bili_direct_url)

BILI_RE = re.compile(r"(BV[0-9A-Za-z]{10}|b23\.tv/[^\s]+|bilibili\.com/(?:video/|BV))", re.I)


def _collect_chain_text(chain) -> str:
    """收集消息链里所有可能带链接的文本。

    ⚠️ 不能只看 Text 元素：QQ 小程序卡片（app=com.tencent.miniapp_01）之类的
    元素不是 Text 类型，但它们的字段里带着 qqdocurl（B站短链）。
    这里把每个元素的常见字符串字段（含 dict）都展开收集，只用于「找链接」，
    不影响其它逻辑。
    """
    parts: list = []
    try:
        items = list(chain or [])
    except Exception:
        return ""
    for ele in items:
        try:
            if isinstance(ele, Text):
                t = getattr(ele, "text", "") or ""
                if t:
                    parts.append(t)
                continue
            # 非 Text 元素：展开它的属性（含 __dict__），只收字符串与 dict
            attrs = {}
            try:
                attrs = dict(vars(ele))
            except Exception:
                attrs = {}
            for key in ("text", "data", "json", "raw", "url", "content", "summary",
                        "qqdocurl", "prompt", "desc", "title"):
                v = getattr(ele, key, None)
                if v is not None and key not in attrs:
                    attrs[key] = v
            for v in attrs.values():
                if isinstance(v, str):
                    if v:
                        parts.append(v)
                elif isinstance(v, dict):
                    try:
                        parts.append(json.dumps(v, ensure_ascii=False))
                    except Exception:
                        pass
                elif isinstance(v, (list, tuple)):
                    for x in v:
                        if isinstance(x, str) and x:
                            parts.append(x)
        except Exception:
            continue
    return "\n".join(parts)
BVID_RE = re.compile(r"BV[0-9A-Za-z]{10}")

# 时间段分析限制
MAX_SEGMENTS = 5          # 一次最多几段
MAX_SEGMENT_SEC = 300     # 单段最长秒数


class VideoSession:
    __slots__ = (
        "session_id", "sid", "source", "source_url", "title",
        "compressed_path", "duration", "width", "height",
        "grids_base64", "scene_count", "total_frames", "timestamps",
        "file_size_mb", "compressed_size_mb",
        "analysis", "analysis_model", "analysis_mode",
        "history", "last_interact", "bili_ai_summary",
        "host_url", "transcript_doc", "model_tag",
    )
    def __init__(self, session_id, sid, source, source_url):
        self.session_id = session_id
        self.sid = sid
        self.source = source
        self.source_url = source_url
        self.title = ""
        self.compressed_path = ""
        self.duration = 0.0
        self.width = self.height = 0
        self.grids_base64 = []
        self.scene_count = 0
        self.total_frames = 0
        self.timestamps = []
        self.file_size_mb = self.compressed_size_mb = 0.0
        self.analysis = ""
        self.analysis_model = ""
        self.analysis_mode = ""
        self.history = []
        self.last_interact = time.time()
        self.bili_ai_summary = None
        self.host_url = ""          # 上传到文件中转后的公开直链（若有）
        self.transcript_doc = ""    # 语音转写时间轴文档（若有）
        self.model_tag = ""         # 该会话首次分析用的模型组（追问默认沿用，避免"换模型"）

    def is_stale(self, ttl: int) -> bool:
        return time.time() - self.last_interact > ttl * 60

    def add_turn(self, q: str, a: str):
        self.history.append({"role": "user", "text": q})
        self.history.append({"role": "bot", "text": a})
        self.last_interact = time.time()


class VideoTask:
    """一次后台分析任务的显式记录。

    设计要点（对应「与框架工具超时脱钩」）：
      - 工具调用只负责建任务并立刻返回（L1：提交路径永不 await 分析体）
      - 真正的分析体跑在独立 Task 上，且内部重活全部离开事件循环（L2）
      - 即使工具协程被框架 wait_for 取消，任务依然存活并最终通告（L3）
    """
    __slots__ = ("task_id", "sid", "kind", "state", "created_at", "started_at",
                 "finished_at", "session_id", "title", "detail", "result", "error",
                 "progress", "asyncio_task", "cancel_requested", "budget_exceeded",
                 "notified", "slot_released", "queue_timer", "_dup_key", "reserved")

    def __init__(self, task_id: str, sid: str, kind: str, title: str = "", detail: str = "",
                 dup_key: str = ""):
        self.task_id = task_id
        self.sid = sid
        self.kind = kind                  # first | segment | followup
        self.state = "queued"             # queued | running | done | failed | rejected | cancelled
        self.created_at = time.time()
        self.started_at = 0.0
        self.finished_at = 0.0
        self.session_id = ""
        self.title = title
        self.detail = detail              # 头部展示用的一行（时长/画质等）
        self.result = ""                  # 完整结果文本（通告用）
        self.error = ""
        self.progress = "已排队"
        self.asyncio_task: Optional[asyncio.Task] = None
        self.cancel_requested = False
        self.budget_exceeded = False
        self.notified = False
        self.slot_released = False
        self.queue_timer: Optional[asyncio.Task] = None
        self._dup_key = dup_key           # 去重键（防 bot 重复调用导致重复下载）
        self.reserved = False             # 是否已占住并发槽（提交时同步决定）

    @property
    def elapsed(self) -> float:
        end = self.finished_at or time.time()
        return max(0.0, end - (self.started_at or self.created_at))


class VideoComprehensionPlugin(BasePlugin):
    def __init__(self, ctx, cfg: dict):
        super().__init__(ctx, cfg)
        # 运行状态（热重载配置时保留）
        self._pending: dict[str, dict] = {}
        self._sessions: dict[str, VideoSession] = {}
        self._sid_sessions: dict[str, list[str]] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._cleanup: Optional[asyncio.Task] = None
        self._auto_sent: dict[str, dict] = {}  # sid → {bvid, title, file_path, text}
        self._ffmpeg_ok = False
        self._stream_unsupported = False
        self._upload_cache: dict[str, str] = {}   # 本地路径 → 已上传的公开 URL
        self._cached_videos: dict[str, list] = {}   # sid → [{orig_name, path, rel, size_mb, ts}, ...]
        self._video_failures: dict[str, dict] = {}  # sid → {原文件名: 失败原因}（供消息改写）
        self._asr_tasks: dict[str, asyncio.Task] = {}   # 转写缓存 key → 进行中的任务
        # ── 异步分析任务（v1.17.0） ──
        self._tasks: dict[str, VideoTask] = {}          # task_id → VideoTask
        self._task_seq: dict[str, int] = {}             # sid → 已发放的短号计数
        self._chat_running: dict[str, int] = {}         # sid → 正在跑的分析任务数
        self._global_running = 0
        self._slot_events: dict[str, asyncio.Event] = {}  # sid → 槽位释放唤醒
        self._notice_buffer: dict[str, list] = {}       # sid → [已完成待合并通告的任务]
        self._flushing: set = set()                     # 正在发通告的会话（防竞态）
        self._notice_tasks: set = set()
        self._background_tasks: set = set()             # 后台任务引用（防被 GC 回收）
        self._migrated_keys: set = set()
        self._bg_sem: Optional[asyncio.Semaphore] = None   # 缓存/转写用的后台闸
        self._bg_sem_limit = 0                             # 上面那个闸的上限（配置变了要重建）
        self._load_cfg(cfg)

    def _load_cfg(self, cfg: dict):
        """读取/热重载配置（不影响会话与后台任务状态）"""
        # 配置自动迁移：必须在读取之前，保证本次运行就用上新默认值
        try:
            self._migrate_config(cfg)
        except Exception as e:
            logger.warning("[VC] 配置迁移异常（已忽略，不影响加载）: %s", e)
        basic = cfg.get("section_basic", {}) or {}
        self.enabled = basic.get("enabled", True)
        self.video_analysis_enabled = basic.get("video_analysis_enabled", False)
        self.default_model = str(basic.get("default_model", "auto"))
        self.allowed_adapters = basic.get("allowed_adapters", [])
        self.max_session_per_user = int(basic.get("max_session_per_user", 5))
        self.auto_cache_video = bool(basic.get("auto_cache_video", True))

        # 双缓存
        cs = cfg.get("section_cache", {}) or {}
        self.bili_cache_dir = str(Path(get_data_path()) / cs.get("bili_cache_dir", "files/video_cache").lstrip("/"))
        self.bili_max_cache = int(cs.get("bili_max_cache_files", 100))
        self.bili_cleanup = int(cs.get("bili_cleanup_count", 20))
        self.other_cache_dir = str(Path(get_data_path()) / cs.get("other_cache_dir", "files/video_analysis_cache").lstrip("/"))
        self.other_max_cache = int(cs.get("other_max_cache_files", 200))
        self.other_cleanup = int(cs.get("other_cleanup_count", 30))
        self.cache_max_file_mb = int(cs.get("cache_max_file_mb", 20))
        self.cache_ttl_hours = float(cs.get("cache_ttl_hours", 24))
        self.cache_max_total_mb = int(cs.get("cache_max_total_mb", 2048))

        self._profiles: list[ModelProfile] = []
        for g in range(1, 5):
            p = ModelProfile.from_cfg(cfg, g)
            if p: self._profiles.append(p)

        fs = cfg.get("section_frame", {}) or {}
        self.target_frames = int(fs.get("target_frames", 40))
        self.max_per_grid = int(fs.get("max_frames_per_grid", 20))
        self.grid_cols = int(fs.get("grid_cols", 5))
        self.scene_threshold = float(fs.get("scene_threshold", 0.3))
        self.cell_width = int(fs.get("frame_width", 320))
        self.cell_ratio = fs.get("frame_ratio", "16:9")

        lm = cfg.get("section_limits", {}) or {}
        self.max_file_mb = int(lm.get("max_file_size_mb", 200))
        _md = int(lm.get("max_duration_sec", 0) or 0)
        if _md <= 0:
            # 0 = 自动跟随：取所有已启用模型组里最大的时长上限。
            # 避免"组里配了 1800 秒，却被这里 600 秒的硬上限先拦死"的矛盾。
            _md = max([p.max_video_sec for p in self._profiles] or [600])
        self.max_duration = _md
        self.max_duration_auto = int(lm.get("max_duration_sec", 0) or 0) <= 0
        self.dl_timeout = int(lm.get("download_timeout_sec", 120))

        bs = cfg.get("section_bili", {}) or {}
        self.bili_enabled = bs.get("bili_enabled", True)
        self.bili_cookie = bs.get("bili_cookie", "")
        self.bili_use_ai = bs.get("bili_use_ai_summary", False)
        self.bili_search_n = int(bs.get("bili_search_count", 5))
        self.bili_max_dl = int(bs.get("bili_max_download_sec", 1800))
        self.auto_send_link = bs.get("auto_send_link", False)
        # 诊断用：只检测不发送（用于定位"收到链接就崩"是检测阶段还是发送阶段）
        self.auto_send_dry_run = bool(bs.get("auto_send_dry_run", False))
        # NapCat 的 upload_file_stream（第三方扩展 action，分块上传）—— 默认关闭。
        # 官方 NapCat 没有它，同类软件（AstrBot 等）也不用它；直接用本地路径发送
        # 对 NapCat / SnowLuma 都够用。开启后才尝试分块上传（失败仍会自动降级）。
        self.napcat_stream = bool(bs.get("napcat_stream_upload", False))
        self.auto_send_allowed_sid = [str(s).strip() for s in bs.get("auto_send_allowed_sid", []) if str(s).strip()]
        self.search_show_desc = bs.get("search_show_desc", True)
        self.search_desc_max_chars = int(bs.get("search_desc_max_chars", 100))
        self.bili_download_quality = bs.get("bili_download_quality", "low")
        self.bili_compress_quality = bs.get("bili_compress_quality", "original")

        ss = cfg.get("section_session", {}) or {}
        self.session_ttl = int(ss.get("session_ttl_minutes", 30))
        self.send_video_quality = ss.get("send_video_quality", "low")
        self.default_prompt = ss.get("default_prompt",
            "你刚刚收到一个视频。请分析其内容，包括：\n1. 视频整体描述\n2. 关键事件时间线\n3. 值得注意的细节\n4. 语音/对话内容")

        us = cfg.get("section_upload", {}) or {}
        self.upload_enabled = bool(us.get("upload_enabled", True))
        # 多源：upload_hosts（list）优先；兼容配置里遗留的 upload_host（string）
        #   ⚠️ 兼容读取要保留（老用户配置里可能只有 upload_host），
        #      但不必再存一份 self.upload_host —— 那是改造前的死代码。
        hosts = us.get("upload_hosts")
        if hosts is None:
            old = us.get("upload_host")
            hosts = [old] if old else []
        if isinstance(hosts, str):
            hosts = [hosts]
        self.upload_hosts = [str(h).strip() for h in (hosts or []) if str(h).strip()] \
            or list(DEFAULT_UPLOAD_HOSTS)

        # ── 语音转写（给模型补上"声音"信息） ──
        au = cfg.get("section_audio", {}) or {}
        self.audio_enabled = bool(au.get("audio_transcribe_enabled", True))
        self.audio_base_url = str(au.get("audio_stt_base_url", "") or "").strip()
        self.audio_api_key = str(au.get("audio_stt_api_key", "") or "").strip()
        self.audio_model = str(au.get("audio_stt_model", "") or "").strip()
        self.audio_wait_sec = float(au.get("audio_wait_sec", 60) or 60)
        self.audio_max_sec = float(au.get("audio_max_sec", 0) or 0)
        self.audio_language = str(au.get("audio_language", "") or "").strip()
        self.audio_timeout = float(au.get("audio_timeout_sec", 300) or 300)
        self.audio_use_proxy = bool(au.get("audio_use_proxy", False))
        self.audio_block_sec = float(au.get("audio_block_sec", 30) or 30)
        self.audio_gap_sec = float(au.get("audio_gap_sec", 2.5) or 2.5)
        self.bili_use_subtitle = bool(au.get("bili_use_subtitle", True))
        self.bili_direct_url = bool(bs.get("bili_direct_url", True))
        self.audio_extra_headers = _as_dict(au.get("audio_stt_extra_headers"))
        self.audio_extra_body = _as_dict(au.get("audio_stt_extra_body"))
        self.cache_scope = str(cs.get("cache_scope", "mentioned") or "mentioned").strip().lower()
        if self.cache_scope not in ("mentioned", "batch", "all"):
            self.cache_scope = "mentioned"
        self.audio_max_blocks = int(au.get("audio_max_blocks", 80) or 80)
        self.audio_concurrency = max(1, min(32, int(au.get("audio_concurrency", 10) or 10)))
        self.audio_silence_db = float(au.get("audio_silence_db", -35) or -35)
        self.upload_max_mb = int(us.get("upload_max_mb", 200))
        self.upload_compress_over_mb = int(us.get("upload_compress_over_mb", 20))
        self.upload_timeout = int(us.get("upload_timeout_sec", 300))
        self.upload_keep_name = bool(us.get("upload_keep_name", True))
        self.upload_use_proxy = bool(us.get("upload_use_proxy", False))

        # ── 异步与并发（v1.17.0） ──
        ac = cfg.get("section_async", {}) or {}
        self.async_analyze = bool(ac.get("async_analyze", True))
        self.async_followup = bool(ac.get("async_followup", False))
        self.max_parallel_per_chat = max(1, min(10, int(ac.get("max_parallel_per_chat", 3) or 3)))
        self.max_parallel_global = max(0, min(32, int(ac.get("max_parallel_global", 6) or 0)))
        self.queue_timeout_sec = max(0, int(ac.get("queue_timeout_sec", 600) or 0))
        self.analysis_budget_sec = max(1, int(ac.get("analysis_budget_sec", 200) or 200))
        self.pipeline_hard_timeout_sec = max(0, int(ac.get("pipeline_hard_timeout_sec", 0) or 0))
        try:
            self.notice_coalesce_sec = max(0.0, float(ac.get("notice_coalesce_sec", 2.0) or 0))
        except (TypeError, ValueError):
            self.notice_coalesce_sec = 2.0
        self.task_keep_minutes = max(1, int(ac.get("task_keep_minutes", 30) or 30))

    # ── 配置自动迁移（只做一次，原子安全） ──

    # (标记名, section, key, 旧默认值, 新默认值)
    _CONFIG_MIGRATIONS = (
        ("bili_max_download_sec_600_to_1800", "section_bili", "bili_max_download_sec", 600, 1800),
        ("audio_concurrency_5_to_10", "section_audio", "audio_concurrency", 5, 10),
    )

    def _migration_marker_path(self) -> str:
        """迁移标记写在**插件自己的数据目录**，不碰框架的配置文件结构。"""
        try:
            base = Path(self.ctx.get_plugin_data_dir())
        except Exception as e:
            logger.warning("[VC] 无法定位插件数据目录，跳过配置迁移: %s", e)
            return ""
        return str(base / "config_migrations.json")

    def _migrate_config(self, cfg: dict) -> None:
        """把「用户从未改过的」旧默认值升级到新默认值。

        为什么不能靠「当前值 == 旧默认」判断：框架在启动时会把 schema 里所有
        缺失的 key 补成默认值（plugin_registry._ensure_plugin_config），
        所以配置文件里一定有这两个 key —— 光看值分不清「用户改的 600」和
        「一直是默认的 600」。因此必须带标记，且**先写标记、后应用**：
        万一写标记失败，宁可这次不迁移，也绝不会二次覆盖用户的手动修改。
        """
        marker_path = self._migration_marker_path()
        if not marker_path:
            return

        # 1) 读标记（损坏 → 视为空，并告警）
        marks: dict = {}
        try:
            if os.path.isfile(marker_path):
                with open(marker_path, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                if isinstance(loaded, dict):
                    marks = loaded
        except Exception as e:
            logger.warning("[VC] 配置迁移标记读取失败（将按未迁移处理）: %s", e)

        # 2) 挑出「未标记」且「当前值 == 旧默认」的迁移项
        pending = []
        for name, section, key, old_default, new_default in self._CONFIG_MIGRATIONS:
            if marks.get(name):
                self._migrated_keys.add(f"{section}.{key}")
                continue
            sec = cfg.get(section)
            if not isinstance(sec, dict) or key not in sec:
                continue                       # 用户配置里没这一项 → 无需迁移
            try:
                cur = int(sec.get(key))
            except (TypeError, ValueError):
                continue
            if cur == old_default:
                pending.append((name, section, key, old_default, new_default))

        if not pending:
            return

        # 3) 先写标记（原子）—— 顺序不能反
        try:
            for name, section, key, old_default, new_default in pending:
                marks[name] = {"at": int(time.time()), key: new_default}
            self._atomic_write_json(marker_path, marks)
        except Exception as e:
            logger.warning("[VC] 配置迁移标记写入失败，本次不迁移（下次启动再试）: %s", e)
            return

        # 4) 再改内存配置
        applied = []
        for name, section, key, old_default, new_default in pending:
            cfg.setdefault(section, {})[key] = new_default
            self._migrated_keys.add(f"{section}.{key}")
            applied.append(f"{key} {old_default} → {new_default}")

        # 5) 原子写回插件配置（保留其它字段；失败只告警，不影响插件加载）
        try:
            self._persist_plugin_cfg(cfg)
            logger.info("[VC] 配置迁移完成（仅迁移未修改过的项）：%s；已标记，不会重复迁移",
                        "，".join(applied))
        except Exception as e:
            logger.warning("[VC] 配置迁移已生效但写回失败（本次运行仍用新值）: %s", e)

    @staticmethod
    def _atomic_write_json(path: str, data: dict) -> None:
        """tmp + os.replace 原子写，避免写一半把配置搞坏。"""
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)
        tmp = f"{path}.tmp.{os.getpid()}"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)

    def _persist_plugin_cfg(self, cfg: dict) -> None:
        """把插件配置写回 data/config/plugins/{plugin_id}.json（原子）。

        ⚠️ 只改内容、不改结构：先读盘上现有内容并合并（以盘上为准做基底，
        用我们的改动覆盖），这样即使是并行更新也不会丢掉别人的字段。
        """
        path = self._plugin_cfg_path()
        if not path:
            return
        on_disk: dict = {}
        try:
            if os.path.isfile(path):
                with open(path, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                if isinstance(loaded, dict):
                    on_disk = loaded
        except Exception as e:
            logger.warning("[VC] 读取插件配置失败，跳过写回: %s", e)
            return

        merged = dict(on_disk)
        for section, values in cfg.items():
            if isinstance(values, dict) and isinstance(merged.get(section), dict):
                merged[section] = {**merged[section], **values}
            else:
                merged[section] = values
        self._atomic_write_json(path, merged)

    def _plugin_cfg_path(self) -> str:
        """定位框架的插件配置文件 data/config/plugins/{plugin_id}.json"""
        try:
            base = Path(get_data_path()) / "config" / "plugins"
        except Exception:
            return ""
        pid = ""
        try:
            import json as _json
            mf = os.path.join(_PLUGIN_DIR, "manifest.json")
            if os.path.isfile(mf):
                with open(mf, "r", encoding="utf-8") as f:
                    pid = str((_json.load(f) or {}).get("plugin_id") or "")
        except Exception:
            pid = ""
        if not pid:
            pid = os.path.basename(_PLUGIN_DIR)
        return str(base / f"{pid}.json")


    async def _ensure_ffmpeg_async(self):
        """后台异步确保 ffmpeg 可用

        策略：shutil.which() 检查 PATH → 有就用
              → 没有就下载静态 ffmpeg 到缓存目录
              → 加到 os.environ['PATH'] 全局生效
        """
        if self._ffmpeg_ok:
            return True

        # 1) 检查 PATH（ffmpeg + ffprobe 都需要）
        ffmpeg_path = shutil.which("ffmpeg")
        if ffmpeg_path:
            if not shutil.which("ffprobe"):
                logger.warning("[VC] ⚠️ ffmpeg 存在但 ffprobe 缺失，抽帧/时长探测会失败")
            self._ffmpeg_ok = True
            logger.info("[VC] ✅ ffmpeg 已可用: %s", ffmpeg_path)
            return True

        import platform as _pf
        system = _pf.system().lower()
        logger.info("[VC] ffmpeg 不在 PATH 中，系统=%s，准备下载静态版本...", system)

        # 2) 尝试系统包管理器（仅 Linux）
        if system == "linux":
            for pm, cmd in [("apk", ["apk", "add", "ffmpeg"]),
                            ("apt-get", ["apt-get", "install", "-y", "ffmpeg"])]:
                try:
                    which_pm = shutil.which(pm)
                    if not which_pm: continue
                    proc = await asyncio.create_subprocess_exec(
                        *cmd, stdout=asyncio.subprocess.DEVNULL,
                        stderr=asyncio.subprocess.DEVNULL,
                    )
                    if await proc.wait() == 0 and shutil.which("ffmpeg"):
                        self._ffmpeg_ok = True
                        logger.info("[VC] ✅ ffmpeg 通过 %s 安装成功", pm)
                        return True
                except: continue

        # 3) 下载静态 ffmpeg（Win/Linux 通用兜底）
        static_dir = os.path.join(self.bili_cache_dir, ".ffmpeg")
        os.makedirs(static_dir, exist_ok=True)
        exe_name = "ffmpeg.exe" if system == "windows" else "ffmpeg"
        static_bin = os.path.join(static_dir, exe_name)

        # 检查之前是否已下载
        if os.path.isfile(static_bin):
            os.environ["PATH"] = static_dir + os.pathsep + os.environ.get("PATH", "")
            self._ffmpeg_ok = True
            logger.info("[VC] ✅ 使用已下载的静态 ffmpeg: %s", static_bin)
            return True

        logger.info("[VC] ⬇️ 下载静态 ffmpeg → %s ...", static_bin)
        try:
            import httpx as _hx
            arch = _pf.machine().lower()

            if system == "windows":
                # Windows: gyan.dev 提供的 zip
                dl_url = "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip"
                async with _hx.AsyncClient(follow_redirects=True, timeout=180) as c:
                    resp = await c.get(dl_url)
                    if resp.status_code == 200:
                        import zipfile, io
                        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                            got = False
                            for name in zf.namelist():
                                if name.endswith(("ffmpeg.exe", "ffprobe.exe")):
                                    zf.extract(name, static_dir)
                                    exe_path = os.path.join(static_dir, name)
                                    shutil.move(exe_path, os.path.join(static_dir, os.path.basename(name)))
                                    got = True
                            if got:
                                # 清理多余文件
                                for d in os.listdir(static_dir):
                                    dp = os.path.join(static_dir, d)
                                    if os.path.isdir(dp) and d.startswith("ffmpeg"):
                                        shutil.rmtree(dp, ignore_errors=True)
                                os.environ["PATH"] = static_dir + os.pathsep + os.environ.get("PATH", "")
                                self._ffmpeg_ok = True
                                logger.info("[VC] ✅ Windows 静态 ffmpeg+ffprobe 下载完成")
                                return True
            else:
                # Linux: johnvansickle 提供的 tar.xz（同时包含 ffmpeg 与 ffprobe）
                dl_url = "https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-arm64-static.tar.xz" if arch in ("aarch64", "arm64") else "https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz"
                async with _hx.AsyncClient(follow_redirects=True, timeout=180) as c:
                    resp = await c.get(dl_url)
                    if resp.status_code == 200:
                        import tarfile, io
                        with tarfile.open(fileobj=io.BytesIO(resp.content)) as tar:
                            got = False
                            for m in tar.getmembers():
                                base = os.path.basename(m.name)
                                if base in ("ffmpeg", "ffprobe"):
                                    tar.extract(m, path=static_dir)
                                    exe_path = os.path.join(static_dir, m.name)
                                    shutil.move(exe_path, os.path.join(static_dir, base))
                                    os.chmod(os.path.join(static_dir, base), 0o755)
                                    got = True
                            if got:
                                # 清理
                                for d in os.listdir(static_dir):
                                    dp = os.path.join(static_dir, d)
                                    if os.path.isdir(dp) and d.startswith("ffmpeg"):
                                        shutil.rmtree(dp, ignore_errors=True)
                                os.environ["PATH"] = static_dir + os.pathsep + os.environ.get("PATH", "")
                                self._ffmpeg_ok = True
                                logger.info("[VC] ✅ Linux 静态 ffmpeg+ffprobe 下载完成")
                                return True
        except Exception as e:
            logger.warning("[VC] 静态 ffmpeg 下载失败: %s", e)

        logger.warning("[VC] ❌ 无法获取 ffmpeg，视频压缩/抽帧/音视频合并功能不可用")
        return False

    async def initialize(self):
        if not self.enabled: return
        # 后台异步安装 ffmpeg（首条日志提示用户）
        logger.info("[VC] 🔍 检查 ffmpeg...（若缺失将后台自动安装，视频压缩/抽帧需要它）")
        asyncio.create_task(self._ensure_ffmpeg_async())
        os.makedirs(self.bili_cache_dir, exist_ok=True)
        os.makedirs(self.other_cache_dir, exist_ok=True)
        await self._do_cleanup(self.bili_cache_dir, self.bili_max_cache, self.bili_cleanup, "B站")
        await self._do_cleanup(self.other_cache_dir, self.other_max_cache, self.other_cleanup, "其他")
        self._cleanup = asyncio.create_task(self._cleanup_loop())
        # 并发闸（按当前配置重建）
        self._rebuild_gates()
        logger.info("[VC] 分析=%s B站=%s | 异步分析=%s(追问异步=%s) | 并发=%d/会话 %s/全局 | 软预算=%ds",
                     self.video_analysis_enabled, self.bili_enabled,
                     self.async_analyze, self.async_followup,
                     self.max_parallel_per_chat,
                     self.max_parallel_global or "∞", self.analysis_budget_sec)
        if self.video_analysis_enabled and self.async_analyze:
            logger.info("[VC] ℹ️ 异步分析已启用：本插件的时间与框架「工具调用超时」相互独立，"
                        "即使框架工具超时小于本插件软预算（%ds）也不受影响（工具毫秒级返回）",
                        self.analysis_budget_sec)
        if self.bili_use_ai and not self.bili_cookie:
            logger.info("[VC] ℹ️ 已开启「优先B站AI总结」但未配置 B站 Cookie："
                        "该接口需要登录态才会返回内容，多数情况下会白跑一次请求再降级，可考虑关闭")

    async def terminate(self):
        if self._cleanup and not self._cleanup.done():
            self._cleanup.cancel()
            try: await self._cleanup
            except asyncio.CancelledError: pass
        # 取消所有后台分析任务与待合并通告
        for t in list(self._tasks.values()):
            if t.asyncio_task and not t.asyncio_task.done():
                t.cancel_requested = True
                t.asyncio_task.cancel()
            if t.queue_timer and not t.queue_timer.done():
                t.queue_timer.cancel()
        self._tasks.clear()
        for nt in list(self._notice_tasks):
            if not nt.done():
                nt.cancel()
        self._notice_tasks.clear()
        for bt in list(self._background_tasks):
            if not bt.done():
                bt.cancel()
        self._background_tasks.clear()
        self._notice_buffer.clear()
        self._flushing.clear()
        self._pending.clear(); self._sessions.clear(); self._sid_sessions.clear()
        self._cached_videos.clear()
        self._video_failures.clear()
        for t in list(self._asr_tasks.values()):
            if not t.done():
                t.cancel()
        self._asr_tasks.clear()

    # ── 缓存清理（通用） ──

    @staticmethod
    def _dir_size(path: str) -> int:
        total = 0
        for root, _dirs, files in os.walk(path):
            for f in files:
                try:
                    total += os.path.getsize(os.path.join(root, f))
                except Exception:
                    pass
        return total

    def _scan_cache(self, cache_dir: str, max_files: int, cleanup_n: int, label: str = "",
                    ttl_hours: float = 0, max_total_mb: float = 0):
        """返回待删除条目名列表。

        三个维度叠加：① 超龄(TTL) → ② 超总容量 → ③ 超条数。
        文件与「分析子目录」都算条目（子目录按其中所有文件的总大小计）。
        """
        try:
            if not os.path.isdir(cache_dir): return []
            entries = []
            for name in os.listdir(cache_dir):
                if name.startswith("."):
                    continue  # 跳过 .ffmpeg 等隐藏目录
                p = os.path.join(cache_dir, name)
                try:
                    st = os.stat(p)
                except Exception:
                    continue
                is_dir = os.path.isdir(p)
                entries.append({
                    "name": name, "mtime": st.st_mtime, "is_dir": is_dir,
                    "size": st.st_size if not is_dir else self._dir_size(p),
                })
            if not entries:
                return []
            entries.sort(key=lambda e: e["mtime"])   # 最旧优先
            to_del = []
            keep = entries
            # ① TTL：超龄一律删
            if ttl_hours and ttl_hours > 0:
                cutoff = time.time() - ttl_hours * 3600
                to_del = [e for e in entries if e["mtime"] < cutoff]
                keep = [e for e in entries if e["mtime"] >= cutoff]
            # ② 总容量超限：从最旧的删起
            if max_total_mb and max_total_mb > 0:
                limit = max_total_mb * 1024 * 1024
                total = sum(e["size"] for e in keep)
                i = 0
                while total > limit and i < len(keep):
                    to_del.append(keep[i]); total -= keep[i]["size"]; i += 1
                keep = keep[i:]
            # ③ 条数超限：删最旧的若干（不超过实际超出的数量）
            if max_files and max_files > 0 and len(keep) > max_files:
                n = min(cleanup_n, len(keep) - max_files)
                to_del.extend(keep[:n])
            return [e["name"] for e in to_del]
        except Exception as e:
            logger.warning("[VC] 缓存[%s]扫描异常: %s", label or cache_dir, e)
            return []

    async def _do_cleanup(self, cache_dir: str, max_files: int, cleanup_n: int, label: str = "",
                          ttl_hours: float = 0, max_total_mb: float = 0):
        to_del = self._scan_cache(cache_dir, max_files, cleanup_n, label,
                                  ttl_hours=ttl_hours, max_total_mb=max_total_mb)
        if not to_del: return
        deleted = 0
        for name in to_del:
            p = os.path.join(cache_dir, name)
            try:
                if os.path.isdir(p):
                    shutil.rmtree(p, ignore_errors=True)
                else:
                    os.remove(p)
                deleted += 1
            except: pass
            await asyncio.sleep(0)
        try:
            remain = len([x for x in os.listdir(cache_dir) if not x.startswith(".")])
        except Exception:
            remain = 0
        logger.info("[VC] 缓存清理[%s]: 删%d个余%d个", label or cache_dir, deleted, remain)

    async def _cleanup_loop(self):
        while True:
            await asyncio.sleep(300)
            await self._do_cleanup(self.bili_cache_dir, self.bili_max_cache, self.bili_cleanup, "B站")
            await self._do_cleanup(self.other_cache_dir, self.other_max_cache, self.other_cleanup, "其他",
                                   ttl_hours=self.cache_ttl_hours,
                                   max_total_mb=self.cache_max_total_mb)
            stale = [k for k, v in self._sessions.items() if v.is_stale(self.session_ttl)]
            for k in stale:
                self._sessions.pop(k, None)
                for sl in self._sid_sessions.values():
                    if k in sl: sl.remove(k)
            self._sid_sessions = {k: v for k, v in self._sid_sessions.items() if v}
            # 未被告知 bot 的缓存记录（30 分钟未用则丢弃）
            _now = time.time()
            self._cached_videos = {k: [c for c in v if _now - c.get("ts", 0) < 1800]
                                   for k, v in self._cached_videos.items() if v}

    def _sid(self, event) -> str:
        return getattr(event.session, "sid", None) or getattr(event, "sid", "") or ""

    @staticmethod
    def _is_self_message(event) -> bool:
        """判断这条消息是不是 bot 自己发的（防御用）。

        正常情况下适配器不会把自身消息上报：
        - SnowLuma / NapCat 的 reportSelfMessage 默认 false
        - 自身消息的 post_type 是 message_sent，而框架只处理 post_type == "message"

        但这两条都是"外部默认值"，万一被改（或换实现），bot 自己发的B站链接
        就会被自动钩子再发一遍 → **重复发送**。所以这里自己再挡一道。
        """
        try:
            msg = getattr(event, "message", None)
            if msg is None:
                return False
            self_id = str(getattr(msg, "self_id", "") or "")
            sender = getattr(msg, "sender", None)
            sender_id = str(getattr(sender, "user_id", "") or "")
            if self_id and sender_id and self_id == sender_id:
                return True
            raw = getattr(msg, "raw_message", None)
            if isinstance(raw, dict):
                if str(raw.get("post_type") or "") == "message_sent":
                    return True
                # 兼容：部分实现把 sender/self 放在 sender 里
                s = raw.get("sender")
                if isinstance(s, dict) and str(raw.get("self_id") or "") and \
                        str(s.get("user_id") or "") == str(raw.get("self_id")):
                    return True
        except Exception:
            pass
        return False

    @staticmethod
    def _normalize_bvid(raw) -> str:
        """把 bot 传来的值规范成 BV 号。

        bot 可能传：`BV1xx411c7mD`、带空格的、完整的视频链接、或 b23 短链文本。
        不规范化就原样发给 B站 → code=-400，报错很难懂。

        ⚠️ BV 号**大小写敏感**（实测小写会返回 -404），所以只做以下处理：
        去空白 / 从链接里抠出 BV 号 / 只把 `bv` 前缀转成大写（尾号保持原样）。
        """
        s = str(raw or "").strip()
        if not s:
            return ""
        m = re.search(r"BV[0-9A-Za-z]{10}", s)
        if m:
            return m.group(0)          # 保留原始大小写
        m2 = re.match(r"^bv([0-9A-Za-z]{10})$", s)
        if m2:
            return "BV" + m2.group(1)  # 只修前缀，尾号不猜
        return ""

    @staticmethod
    def _msg_id(event) -> str:
        """取消息 ID（用于在上下文里精确定位某条消息）。取不到就返回空串。"""
        try:
            for holder in (getattr(event, "message", None), event):
                if holder is None:
                    continue
                v = getattr(holder, "message_id", None)
                if v:
                    return str(v)
        except Exception:
            pass
        return ""

    def _is_qq(self, event) -> bool:
        """判断是否 QQ 平台（platform 来自适配器 manifest.name，内置为 "QQ"，大小写不敏感）"""
        return str(getattr(event.adapter, "platform", "") or "").strip().lower() == "qq"

    def _ok(self, event) -> bool:
        if not self.allowed_adapters: return True
        allow = {str(a).strip().lower() for a in self.allowed_adapters}
        n = str(getattr(event.adapter, "name", "") or "").strip().lower()
        p = str(getattr(event.adapter, "platform", "") or "").strip().lower()
        return n in allow or p in allow

    def _register_session(self, sess, sid):
        self._sessions[sess.session_id] = sess
        if sid not in self._sid_sessions: self._sid_sessions[sid] = []
        lst = self._sid_sessions[sid]
        if sess.session_id in lst: lst.remove(sess.session_id)
        lst.insert(0, sess.session_id)
        # M3：超上限时不直接销毁，而是「降级保留」——把内存里最贵的拼图丢掉，
        #     但留下分析结果/历史/转写，这样追问仍然可用（只是不能再问画面细节），
        #     否则第 6 个视频一来，第 1 个的会话连追问都直接 404。
        while len(lst) > self.max_session_per_user:
            old_id = lst.pop()
            old = self._sessions.get(old_id)
            if old is None:
                continue
            if old.grids_base64:
                old.grids_base64 = []
                old.analysis_mode = (old.analysis_mode or "") + "(已降级:拼图已回收)"
                logger.info("[VC] 会话 %s 超出保留上限，已回收拼图（分析文本仍可用于追问）",
                            old_id)
            else:
                self._sessions.pop(old_id, None)

    def _get_by_session_id(self, sid):
        return self._sessions.get(sid)

    def _list_sessions(self, sid):
        return [self._sessions[s] for s in self._sid_sessions.get(sid, []) if s in self._sessions]

    def _session_lock(self, key: str) -> asyncio.Lock:
        """按 key 串行化（M4：原先 _locks 定义了却从未使用）。

        用于「同一会话 + 同一视频」的并发重活（时间段分析要写同一个 work 父目录），
        避免两个任务同时写盘互相踩。不同 key 之间互不影响。
        """
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        # 顺手清理无人持有的锁，避免长期运行后无限增长
        if len(self._locks) > 512:
            for k in [k for k, v in self._locks.items()
                      if not v.locked() and k != key][:256]:
                self._locks.pop(k, None)
        return lock

    # ══════════════════════════════════════════════════════════════
    #  异步分析任务系统（v1.17.0）
    #
    #  脱钩三重保证：
    #    L1 提交路径：工具只建任务即返回，永不 await 分析体（毫秒级）
    #    L2 计时干扰：分析重活全部离开事件循环（to_thread / executor），
    #                 否则会拖住全局计时器（包括框架的工具超时）
    #    L3 软预算  ：即使工具协程被框架取消，任务依然存活并最终通告
    # ══════════════════════════════════════════════════════════════

    def _rebuild_gates(self):
        """按当前配置重建并发闸（仅用于分析任务；追问不计入）

        用「显式计数 + 事件唤醒」而不是 Semaphore：因为提交时要**同步地**
        判断这次是「立即开跑」还是「排队」，Semaphore 的 acquire 是异步的，
        提交瞬间读到的状态会误导文案（曾导致首次分析被误报成「已排队」）。
        """
        self._chat_running.clear()
        self._global_running = 0
        self._slot_events: dict[str, asyncio.Event] = {}
        if not hasattr(self, "_slot_events") or self._slot_events is None:
            self._slot_events = {}

    def _slot_event(self, sid: str) -> asyncio.Event:
        e = self._slot_events.get(sid)
        if e is None:
            e = asyncio.Event()
            self._slot_events[sid] = e
        return e

    def _try_reserve(self, sid: str) -> bool:
        """同步尝试占一个分析槽（成功返回 True 并已计数）"""
        if self._running_count(sid) >= self.max_parallel_per_chat:
            return False
        if self.max_parallel_global > 0 and self._global_running >= self.max_parallel_global:
            return False
        self._chat_running[sid] = self._running_count(sid) + 1
        self._global_running += 1
        return True

    def _release_slot(self, sid: str):
        if self._running_count(sid) > 0:
            self._chat_running[sid] = self._running_count(sid) - 1
        if self._global_running > 0:
            self._global_running -= 1
        ev = self._slot_events.get(sid)
        if ev is not None:
            ev.set()

    def _new_task_id(self, sid: str) -> str:
        n = int(self._task_seq.get(sid, 0)) + 1
        self._task_seq[sid] = n
        return f"V{n % 10000}"

    def _spawn(self, coro, name: str = "") -> asyncio.Task:
        """创建后台任务并持有强引用（否则可能被 GC 提前回收）。"""
        t = asyncio.create_task(coro, name=name or "vc_bg")
        self._background_tasks.add(t)
        t.add_done_callback(self._background_tasks.discard)
        return t

    def _running_count(self, sid: str) -> int:
        return int(self._chat_running.get(sid, 0))

    def _task_desc_line(self, task: VideoTask) -> str:
        parts = [f"任务号: {task.task_id}"]
        if task.title:
            t = task.title
            parts.append(t if t.startswith("《") else f"《{t}》")
        if task.detail and task.detail != task.title:
            parts.append(task.detail)
        return " | ".join(parts)

    def _prune_tasks(self):
        """清理过期任务记录（只在完成态里挑）"""
        cutoff = time.time() - self.task_keep_minutes * 60
        for tid in [k for k, v in self._tasks.items()
                    if v.state in ("done", "failed", "rejected", "cancelled")
                    and (v.finished_at or v.created_at) < cutoff]:
            self._tasks.pop(tid, None)

    # ── 提交（L1：永不阻塞） ──

    def _submit_task(self, sid: str, kind: str, title: str, detail: str,
                     runner, dup_key: str = "") -> VideoTask:
        """建任务并立刻返回。runner 是真正的执行体（callable → coroutine）。

        并发闸：只对 first / segment 生效；followup 直接执行（你的决策）。
        """
        self._prune_tasks()
        task = VideoTask(self._new_task_id(sid), sid, kind, title, detail, dup_key=dup_key)
        self._tasks[task.task_id] = task

        if kind == "followup":
            task.state = "running"
            task.started_at = time.time()
            task.progress = "正在回答追问"
            task.reserved = False
            task.asyncio_task = self._spawn(self._run_task(task, runner, use_gate=False),
                                            name=f"vc_task_{task.task_id}")
            return task

        # 关键：**同步**决定「立即开跑」还是「排队」，让文案与事实一致
        if self._try_reserve(sid):
            task.state = "running"
            task.started_at = time.time()
            task.progress = "正在处理"
            task.reserved = True
        else:
            task.state = "queued"
            task.progress = "已排队"
            task.reserved = False
        task.asyncio_task = self._spawn(self._run_task(task, runner, use_gate=True),
                                        name=f"vc_task_{task.task_id}")
        # 排队超时看门狗
        if task.state == "queued" and self.queue_timeout_sec > 0:
            task.queue_timer = self._spawn(self._queue_watchdog(task),
                                           name=f"vc_queue_{task.task_id}")
        return task

    async def _queue_watchdog(self, task: VideoTask):
        try:
            await asyncio.sleep(self.queue_timeout_sec)
        except asyncio.CancelledError:
            return
        if task.state != "queued":
            return
        # 排队超时 → 拒绝该任务（不静默丢失）
        task.state = "rejected"
        task.error = (f"本会话已有 {self.max_parallel_per_chat} 个视频在分析"
                      f"（每会话上限 {self.max_parallel_per_chat}），"
                      f"排队等待超过 {self.queue_timeout_sec} 秒仍未轮到")
        task.finished_at = time.time()
        task.progress = "排队超时未执行"
        if task.asyncio_task and not task.asyncio_task.done():
            task.asyncio_task.cancel()
        await self._notify_done(task)

    async def _run_task(self, task: VideoTask, runner, use_gate: bool):
        """任务执行体：等待槽位（若已占则跳过）→ 跑 runner → 释放 → 通告。

        注意：**不在这里 await 工具协程**。真正调用方是 _await_or_handoff()。
        """
        hard_task = None
        try:
            if use_gate and not task.reserved:
                # 排队中：等槽位事件，直到有位置或任务被判超时/取消
                while task.state == "queued" and not task.cancel_requested:
                    ev = self._slot_event(task.sid)
                    ev.clear()
                    try:
                        await asyncio.wait_for(ev.wait(), timeout=0.5)
                    except asyncio.TimeoutError:
                        pass
                    if task.state != "queued":
                        return
                    if self._try_reserve(task.sid):
                        task.reserved = True
                        break
                if task.state != "queued" or not task.reserved:
                    return
                task.state = "running"
                task.started_at = time.time()
                task.progress = "正在处理"

            # 硬上限（默认 0 = 不设）：真正的强制中止。
            # ⚠️ 不能在这里 return —— 那会跳过末尾的 _notify_done，
            #    失败就再也不会告诉用户。用标记位走到统一收尾。
            result = None
            if self.pipeline_hard_timeout_sec > 0:
                hard_task = asyncio.ensure_future(runner(task))
                try:
                    result = await asyncio.wait_for(
                        asyncio.shield(hard_task),
                        timeout=self.pipeline_hard_timeout_sec)
                except asyncio.TimeoutError:
                    hard_task.cancel()
                    try:
                        await hard_task
                    except (asyncio.CancelledError, Exception):
                        pass
                    task.state = "failed"
                    task.error = (f"超过硬上限 {self.pipeline_hard_timeout_sec} 秒仍未完成，已中止"
                                  f"（下载 / 抽帧 / 模型响应某一步卡住）")
                    task.progress = "已中止（超过硬上限）"
                    task.finished_at = time.time()
                    logger.warning("[VC] 任务 %s 超过硬上限 %ds，已强制中止",
                                   task.task_id, self.pipeline_hard_timeout_sec)
            else:
                result = await runner(task)

            if task.state == "running":
                task.state = "done"
                task.result = result or ""
                task.finished_at = time.time()
        except asyncio.CancelledError:
            if hard_task and not hard_task.done():
                hard_task.cancel()
            # ⚠️ 排队看门狗会先把状态标成 rejected 再 cancel 这里 ——
            #    不能无条件覆盖成 cancelled，否则通告会误报成
            #    「任务已取消（插件重载或服务关闭）」
            if task.state not in ("rejected", "failed", "done"):
                task.state = "cancelled"
                task.error = task.error or "任务已取消（插件重载或服务关闭）"
                task.finished_at = task.finished_at or time.time()
            raise
        except Exception as e:
            logger.exception("[VC] 任务 %s 执行异常", task.task_id)
            task.state = "failed"
            task.error = f"{type(e).__name__}: {e}"
            task.finished_at = time.time()
        finally:
            # 统一收尾：状态兜底 + 归还槽位 + 取消排队看门狗
            if task.state == "running":
                task.state = "failed"
                task.error = task.error or "任务未正常结束"
                task.finished_at = time.time()
            if getattr(task, "reserved", False):
                self._release_slot(task.sid)
                task.reserved = False
            task.slot_released = True
            if task.queue_timer and not task.queue_timer.done():
                task.queue_timer.cancel()

        if task.state in ("done", "failed", "rejected"):
            await self._notify_done(task)

    def _task_summary_line(self, task: VideoTask) -> str:
        """头部信息行（通告用）"""
        bits = []
        if task.title:
            bits.append(task.title if task.title.startswith("《") else f"《{task.title}》")
        if task.detail:
            bits.append(task.detail)
        return " | ".join(bits)

    # ── 完成通告（合并窗口，减少 LLM 轮次） ──

    async def _notify_done(self, task: VideoTask):
        """把已完成的任务放进合并缓冲，并在窗口后统一发出。"""
        if task.notified:
            return
        task.notified = True
        self._notice_buffer.setdefault(task.sid, []).append(task)
        if self.notice_coalesce_sec <= 0:
            await self._flush_notices(task.sid)
            return
        self._ensure_flush_task(task.sid)

    def _ensure_flush_task(self, sid: str):
        # 正在 flush 中：它会循环把新到达的任务一并取走，不必再排一个
        if sid in self._flushing:
            return
        name = f"vc_notice_{sid}"
        for t in list(self._notice_tasks):
            if t.done():
                self._notice_tasks.discard(t)
                continue
            if t.get_name() == name:
                return                      # 已有排队的 flush，等它就行
        nt = self._spawn(self._notice_flush_later(sid), name=name)
        self._notice_tasks.add(nt)
        nt.add_done_callback(self._notice_tasks.discard)

    async def _notice_flush_later(self, sid: str):
        try:
            await asyncio.sleep(self.notice_coalesce_sec)
        except asyncio.CancelledError:
            return
        await self._flush_notices(sid)

    async def _flush_notices(self, sid: str):
        """把缓冲里的任务组装成通告并发出去。

        ⚠️ 原先的实现有个竞态：**在 await publish_notice 期间新完成的任务**，
        因为「同名 flush 任务还没 done」而不会再排 flush ⇒ 那条通知就卡在
        buffer 里，直到下一次有任务完成才被带出去（用户表现为「早完成了却没有
        任何消息」）。现在改成：flush 期间循环取走新到达的，收尾再检查一次残留。
        """
        if sid in self._flushing:
            return                          # 已有 flush 在处理这个会话
        self._flushing.add(sid)
        try:
            for _ in range(6):
                tasks = self._notice_buffer.pop(sid, []) or []
                if not tasks:
                    return
                text = self._compose_notice(tasks)
                if not text:
                    continue
                try:
                    await self.ctx.publish_notice(sid, MessageChain([Text(text)]),
                                                  is_mentioned=True)
                    logger.info("[VC] 已回灌分析完成通告（%d 条任务）→ %s", len(tasks), sid)
                except Exception:
                    logger.exception("[VC] 回灌分析通告失败")
                    return
        finally:
            self._flushing.discard(sid)
        # 极端情况下（持续有新任务）还留了残留 → 再排一次，保证不丢
        if self._notice_buffer.get(sid):
            self._ensure_flush_task(sid)

    def _compose_notice(self, tasks: list) -> str:
        """把一批已完成任务组装成一条通告文本"""
        done = [t for t in tasks if t.state == "done"]
        failed = [t for t in tasks if t.state == "failed"]
        rejected = [t for t in tasks if t.state == "rejected"]
        cancelled = [t for t in tasks if t.state == "cancelled"]
        parts = []
        if done:
            parts.append(self._notice_success(done))
        for t in failed:
            parts.append(self._notice_failure(t))
        for t in rejected:
            parts.append(self._notice_rejected(t))
        if cancelled:
            parts.append("【系统通知 · 视频分析已取消】\n"
                         "任务被取消（插件重载或服务关闭）。如果是插件重载导致的，"
                         "请告知用户稍后重新发起即可。")
        return "\n\n".join(p for p in parts if p)

    def _notice_success(self, tasks: list) -> str:
        """成功通告：多条任务合并成一条，避免连开多轮对话。"""
        head = ["【系统通知 · 视频分析完成】"]
        for t in tasks:
            line = f"{t.task_id}: " + (self._task_summary_line(t) or t.kind)
            line += f" | 用时 {int(t.elapsed)} 秒"
            if t.session_id:
                line += f" | session_id={t.session_id}"
            head.append(line)
        body = []
        for t in tasks:
            if len(tasks) == 1:
                body.append(t.result)
            else:
                body.append(f"—— {t.task_id} ——\n{t.result}")
        tail = ("用你自己的语气把结果讲给用户听（可精简、可加点评），"
                "不要提「系统通知/后台任务/任务号」，也不要重发视频。")
        if any(t.session_id for t in tasks):
            tail += "用户可用上面的 session_id 继续追问，不必重发。"
        return "\n".join(head) + "\n━━━\n" + "\n\n".join(body) + "\n━━━\n" + tail

    def _notice_failure(self, task: VideoTask) -> str:
        reason = task.error or "未知原因"
        hint = ""
        low = reason.lower()
        if "超时" in reason or "timeout" in low:
            hint = ("建议: 稍后重试；若反复失败，可能是网络问题，"
                    "或该视频需要登录权限（会员/充电视频需配 B站 Cookie）")
        elif "cookie" in low or "登录" in reason or "权限" in reason:
            hint = "建议: 该视频可能需要登录权限，可在插件设置里配置 B站 Cookie 后重试"
        elif "过大" in reason or "时长" in reason or "上限" in reason:
            hint = "建议: 换更短的视频，或在「安全限制」里放宽对应上限"
        elif "模型" in reason or "api" in low or "401" in reason or "403" in reason:
            hint = "建议: 检查模型组的 API Key / 地址是否正确、余额是否充足"
        else:
            hint = "建议: 稍后重试；若反复失败请查看运行日志"
        text = ("【系统通知 · 视频分析失败】\n"
                f"{task.task_id}: {self._task_summary_line(task)} | 原因: {reason}\n{hint}\n"
                "──────────────────────\n"
                "把原因和建议转述给用户，不要提「系统通知」，也不要立刻反复重试同一个视频。")
        return text

    def _notice_rejected(self, task: VideoTask) -> str:
        return ("【系统通知 · 视频分析未执行】\n"
                f"{task.task_id}: {self._task_summary_line(task)} | 原因: {task.error}\n"
                "建议: 等前面几个完成后再发一次\n"
                "──────────────────────\n"
                "把情况和建议告诉用户。")

    # ── L3：软预算交接 ──

    async def _await_or_handoff(self, task: VideoTask, budget: float):
        """原地最多等 budget 秒。

        返回 ("done", 结果文本) / ("failed", 错误) / ("handoff", None) 三态 ——
        **不能用 None 同时表示「失败」和「转后台」**（曾导致分析失败被误报成
        「还在看，再等一下」，用户永远等不到结果）。

        ⚠️ 排队看门狗在拒绝任务时会 cancel 任务本体，而这里 shield 住的就是它
        ⇒ 等待会抛 CancelledError。**这时不能往上传**（框架会判成工具失败），
        要识别成「任务已被拒绝」并正常返回文案。
        """
        body = task.asyncio_task
        if body is None:
            return ("failed", "任务未启动")
        waiter = asyncio.shield(body)
        try:
            await asyncio.wait_for(waiter, timeout=max(0.1, budget))
        except asyncio.TimeoutError:
            task.budget_exceeded = True
            task.progress = "已超过软预算，转后台继续"
            logger.info("[VC] 任务 %s 超过软预算 %.0fs，转后台继续（不中断）",
                        task.task_id, budget)
            return ("handoff", None)
        except asyncio.CancelledError:
            # 区分两种来源：
            #   ① 任务本体被「排队看门狗」取消 → 这是正常业务结果，正常返回文案
            #   ② 外层（框架的 wait_for）取消我们 → 任务必须继续活着（L3），往上抛
            if task.state in ("rejected", "failed", "cancelled"):
                logger.info("[VC] 任务 %s 已被取消/拒绝，向工具返回明确原因", task.task_id)
                return ("failed", task.error or "任务未执行")
            # 框架的 wait_for 掐断了工具协程 —— 任务必须继续活着（L3）
            task.budget_exceeded = True
            task.progress = "框架工具超时，已转后台继续"
            logger.info("[VC] 任务 %s 的工具调用被框架取消，任务已转后台继续（不影响结果）",
                        task.task_id)
            raise
        if task.state == "done":
            return ("done", task.result)
        if task.state == "rejected":
            return ("failed", task.error or "排队超时未执行")
        return ("failed", task.error or "未知原因")

    # ── 提交文案 ──

    def _ack_submitted(self, task: VideoTask, parallel_now: int) -> str:
        total = self.max_parallel_per_chat
        return (
            f"✅ 已开始分析视频（后台进行）\n"
            f"{self._task_desc_line(task)}\n"
            f"本会话并行: {max(1, parallel_now)}/{total}\n"
            "──────────────────────\n"
            "正在后台处理（下载 → 抽帧 → 语音转写），完成后结果会自动交给你。\n"
            "现在只用一两句话告诉用户「正在看，稍等一下」。"
            "不要猜视频内容、不要重复调用本工具、不要提「后台/系统/任务号」。"
        )

    def _ack_queued(self, task: VideoTask) -> str:
        pos = 1 + sum(1 for t in self._tasks.values()
                      if t.sid == task.sid and t.state == "queued"
                      and t.created_at < task.created_at)
        return (
            f"🕐 已排队（本会话正在分析 {self.max_parallel_per_chat} 个视频，"
            f"达到上限 {self.max_parallel_per_chat}）\n"
            f"{self._task_desc_line(task)} | 排队第 {pos} 位"
            f" | 最长等待 {self.queue_timeout_sec} 秒\n"
            "──────────────────────\n"
            "用一句话告诉用户「前面的还在处理，排到了就立刻开始」，不要重复调用本工具。"
        )

    def _ack_handoff(self, task: VideoTask) -> str:
        return (
            f"⏳ 分析仍在进行（已超过 {self.analysis_budget_sec} 秒），已转后台继续，不会中断。\n"
            f"{self._task_desc_line(task)} | 进度: {task.progress}\n"
            "──────────────────────\n"
            "用一句话告诉用户「这个视频比较大，还在看，再等一下」，不要重复调用本工具。"
        )

    # ── 自动发送B站链接（对标音频条 auto_send_link，0 token） ──

    @on.im_message(priority=Priority.HIGH)
    async def _auto_send_hook(self, event: KiraMessageEvent, *_):
        if not self.enabled or not self.auto_send_link:
            return
        if not self.bili_enabled: return
        if self.auto_send_allowed_sid and event.session.sid not in self.auto_send_allowed_sid:
            return
        if not self._is_qq(event):
            return
        # 防御：bot 自己发的消息（有些实现会上报）绝不能触发自动发送，
        # 否则就会"自己刚发完又一个视频，钩子检测到链接再发一遍" → 重复刷屏
        if self._is_self_message(event):
            return
        sid = event.session.sid or ""
        if not sid:
            return

        bvid = ""

        # 收集整条消息链里所有可能带链接的文本。
        # ⚠️ 不能只看 Text 元素：QQ 小程序卡片（com.tencent.miniapp_01）等
        #    元素不是 Text 类型，但它们的字段里带着 qqdocurl（B站短链）。
        # ⚠️ 这两条在"每条消息"都会走到，所以**只在开诊断开关时才打**，
        #    否则就是刷屏（和之前那个"未发现链接"的日志同一个坑）。
        _diag = self.auto_send_dry_run
        if _diag:
            logger.info("[VCDIAG] 1/6 hook 进入 sid=%s", sid)
        text = _collect_chain_text(event.message.chain)
        if _diag:
            logger.info("[VCDIAG] 2/6 文本收集完成 len=%d", len(text))

        # ⚠️ 只认「**明确的 B站链接**」，**不再把裸 BV 号当触发条件**。
        #    原因：裸的 BV+10位字母数字在日常聊天里出现概率不低（很容易误触发），
        #    而且如果是假号，会先走 B站接口、拿回 code=-400 再报错 —— 用户明确
        #    要求不要这样自动发。要发裸 BV 号请让 bot 主动调 send_video。
        m = re.search(r'https?://(?:www\.|m\.)?bilibili\.com/video/(BV[0-9A-Za-z]{10})',
                      text, re.I)
        if m:
            bvid = m.group(1)

        # b23 短链（也必须是带协议的完整短链）
        if not bvid:
            m2 = re.search(r'https?://b23\.tv/([0-9A-Za-z]+)', text)
            if m2:
                try:
                    bvid = await extract_bvid(m2.group(0), self.dl_timeout)
                except Exception:
                    logger.info("[VC] b23 短链解析失败: %s", m2.group(1))
                if not bvid:
                    logger.info("[VC] b23 短链未解析出 BV 号，跳过自动发送: %s", m2.group(0))

        # 兜底：如果链文本里没有，再从 raw_message 里找一次（有些适配器
        # 把卡片原文放在别处）
        if not bvid:
            try:
                import json as _json
                raw = getattr(event, "raw_message", None)
                if raw is None and hasattr(event, "message"):
                    raw = getattr(event.message, "raw_message", None)
                if raw is None and hasattr(event, "message") and hasattr(event.message, "source_message"):
                    raw = getattr(event.message, "source_message", None)
                if isinstance(raw, dict):
                    raw = _json.dumps(raw, ensure_ascii=False)
                if isinstance(raw, str) and raw:
                    m3 = re.search(r'https?://b23\.tv/([0-9A-Za-z]+)', raw)
                    if m3:
                        bvid = await extract_bvid(m3.group(0), self.dl_timeout)
                    if not bvid:
                        m4 = re.search(r'https?://(?:www\.|m\.)?bilibili\.com/video/(BV[0-9A-Za-z]{10})', raw, re.I)
                        if m4:
                            bvid = m4.group(1)
            except Exception:
                pass

        if not bvid:
            # 没有明确的B站链接是**常态**（群里绝大多数消息都没链接），静默跳过。
            # 注意：这里千万不要打日志 —— 每条消息都会走到这，会疯狂刷屏，
            # 而且"没触发"不代表检测有问题。真正需要关注的是「看到链接但解析失败」
            # （上面 b23 那两条）和下方"检测到B站视频"。
            return
        # 记录"用来在上下文里定位这条消息"的匹配键。
        # ⚠️ 不能只用 bvid：消息里写的可能是 b23 短链（V5Xhy88 这种），
        #    或藏在 QQ 小程序卡片的 qqdocurl 里，用 BV 号是匹配不上的。
        match_keys = [bvid]
        m2 = re.search(r'b23\.tv/([0-9A-Za-z]+)', text)
        if m2:
            match_keys.append(m2.group(1))
        mid = self._msg_id(event)
        logger.info("[VCDIAG] 3/6 检测到B站视频 bvid=%s keys=%s", bvid, match_keys)
        logger.info("[VC] auto_send 检测到B站视频: %s（匹配键=%s）", bvid, match_keys)
        if self.auto_send_dry_run:
            logger.info("[VCDIAG] dry_run=开 → 只检测不发送，到此为止")
            return
        # N4：后台发送（最长 180 秒）期间，用户若刚好 @bot 问这个视频，
        #     _pending 里没有条目 → bot 会答「当前无视频」。这里**先占位**，
        #     发送完成（成功走 _auto_sent 标注、失败则清掉）再交接。
        if sid not in self._pending:
            self._pending[sid] = {"url": f"https://www.bilibili.com/video/{bvid}",
                                  "source": "auto_send", "ts": time.time(),
                                  "bvid": bvid}
        self._spawn(self._auto_send_do(bvid, event.adapter.name, sid,
                                       match_keys=match_keys, message_id=mid),
                    name="vc_auto_send")
        logger.info("[VCDIAG] 4/6 后台任务已创建（hook 返回，不再占用消息处理）")

    async def _auto_send_do(self, bvid: str, adapter_name: str, sid: str,
                            match_keys: list | None = None, message_id: str = ""):
        """异步后台发送，成功后记录 auto_sent 用于 LLM 上下文标注"""
        try:
            logger.info("[VCDIAG] 5/6 后台任务开始 bvid=%s sid=%s", bvid, sid)
            reply = await self._send_video_by_bvid(None, bvid, sid=sid, adapter_name=adapter_name)
            logger.info("[VCDIAG] 6/6 发送函数返回: %s", str(reply)[:80])
            if reply and reply.startswith("✅"):
                # 成功 → 记录 auto_sent，不 discard，消息继续自然流转
                title = bvid
                for line in reply.split("\n"):
                    if "已发送：" in line:
                        title = line.split("已发送：")[-1].strip()
                self._auto_sent[sid] = {
                    "bvid": bvid,
                    "title": title,
                    "file_path": reply.split("本地路径:")[-1].strip() if "本地路径:" in reply else "",
                    # 用于在 LLM 上下文里定位原始消息（短链/卡片消息不能靠 bvid 匹配）
                    "match_keys": list(match_keys or [bvid]),
                    "message_id": str(message_id or ""),
                    "ts": time.time(),
                }
            elif reply:
                # 失败 → **只记日志，不发到会话里**（自动钩子是后台行为，
                # 报错刷屏会打扰群聊；要看就去 cmd / 日志里看）
                logger.warning("[VC] auto_send 未成功: %s", reply)
                # N4：占位的 _pending 要撤掉，避免 bot 以为有视频可分析
                pend = self._pending.get(sid) or {}
                if pend.get("source") == "auto_send" and pend.get("bvid") == bvid:
                    self._pending.pop(sid, None)
        except Exception as e:
            # 同上：异常也只落日志，不往会话里发消息
            logger.warning("[VC] auto_send 异常: %s", e)
            pend = self._pending.get(sid) or {}
            if pend.get("source") == "auto_send" and pend.get("bvid") == bvid:
                self._pending.pop(sid, None)

    # ── 批次阶段缓存（避免群里与 bot 无关的视频也被下载+转写） ──

    @on.im_batch_message(priority=Priority.LOW)
    async def _on_batch_cache(self, event, *_):
        """消息合并成批次（确定要送给 bot）后，才按 cache_scope 决定要不要缓存+转写。

        - mentioned：只处理「被 @ / 引用 / 唤醒」的消息里的视频（默认，最省）
        - batch    ：处理批次里所有消息的视频
        - all      ：已在 _detect 阶段逐条缓存，这里不重复做

        优先级 LOW：批次若被更早的插件 stop 掉，本钩子根本不会执行。
        """
        if not self.enabled or not self.auto_cache_video: return
        if self.cache_scope == "all": return
        if not self._ok(event): return
        sid = self._sid(event)
        if not sid: return
        try:
            msgs = list(getattr(event, "messages", None) or [])
        except Exception:
            return
        if self.cache_scope == "mentioned":
            msgs = [m for m in msgs if getattr(m, "is_mentioned", False)]
        for m in msgs:
            try:
                for ele in self._iter_videos(getattr(m, "chain", None)):
                    f = str(getattr(ele, "file", "") or "")
                    if f.startswith(("http://", "https://")):
                        nm = str(getattr(ele, "name", "") or "")
                        self._spawn(self._cache_then_transcribe(sid, f, nm), name="vc_cache")
            except Exception:
                continue

    # ── 已直发的 LLM 上下文标注（对齐音频条 inject_auto_sent_note） ──

    @on.llm_request(priority=Priority.LOW)
    async def _inject_auto_sent_note(self, event: KiraMessageBatchEvent, req: LLMRequest, *_):
        if not self.enabled: return
        sid = getattr(event.session, "sid", None)
        if not sid: return
        # ⚠️ 这里**不能**先 pop：万一这一轮没匹配到（比如消息还没进批次），
        #    记录就被永久丢掉了。匹配成功后再删；超时（5 分钟）才丢弃。
        sent = self._auto_sent.get(sid)
        if not sent: return
        if time.time() - float(sent.get("ts") or 0) > 300:
            self._auto_sent.pop(sid, None)
            return
        note = (
            f"\n[系统提示：该B站视频（《{sent['title']}》"
            f"BV:{sent['bvid']}）已自动发送压缩版视频（{sent.get('file_path','')}）]"
        )
        bvid = sent["bvid"]
        # 匹配键：bvid + b23 短链 id（消息里写的可能是短链或藏在卡片里）
        keys = [k for k in (sent.get("match_keys") or []) if k]
        if bvid not in keys:
            keys.append(bvid)
        want_mid = str(sent.get("message_id") or "")
        # 按顺序遍历 messages ↔ user_prompt，定位原始消息追加 note
        prompt_idx = 0
        for msg in event.messages:
            while prompt_idx < len(req.user_prompt) and not (
                    req.user_prompt[prompt_idx].name == "message"
                    and req.user_prompt[prompt_idx].source == "system"
            ):
                prompt_idx += 1
            if prompt_idx >= len(req.user_prompt):
                break
            p = req.user_prompt[prompt_idx]
            prompt_idx += 1
            # ① 消息 ID 精确匹配（最可靠）
            hit = bool(want_mid) and want_mid == self._msg_id(msg)
            # ② 回退：扫描整条消息链的文本（**不只 Text 元素**，
            #    这样 QQ 小程序卡片里的 qqdocurl 也能被扫到）
            if not hit:
                msg_text = _collect_chain_text(getattr(msg, "chain", None))
                hit = any(k in msg_text for k in keys)
            if hit:
                p.content += note
                self._auto_sent.pop(sid, None)   # 标注成功 → 消费掉
                logger.info("[VC] 已在上下文里标注「已自动发送」: %s", bvid)
                break

    # ── 改写消息里的视频占位（让 bot 拿到可用的路径，而不是"没缓存"） ──

    _VIDEO_PLACEHOLDER_RE = re.compile(
        r"\[Video name:\s*([^\]\(]+?)\s*\(Video size over 10MB, not cached\)\]")

    def _rewrite_video_notes(self, event, req, sid: str) -> None:
        """把框架渲染的 `[Video name: X (Video size over 10MB, not cached)]`
        替换成我们能提供的最佳信息：

          - 已缓存     → [视频已缓存: <相对路径>]
          - 缓存失败   → [视频未缓存: <原因>]
          - 未预缓存   → [视频 X（未预缓存，可用 analyze_video 分析）]

        最后一种很关键：即使因为缓存策略没提前下，也要让 bot 知道
        「工具其实能处理」，而不是像以前那样回答"我看不到内容"。
        """
        cached = self._cached_videos.get(sid) or []
        failed = self._video_failures.get(sid) or {}
        if not cached and not failed and sid not in self._pending:
            return
        repl = {}
        for c in cached:
            n = str(c.get("orig_name") or "").strip().lower()
            if n:
                repl[n] = f"[视频已缓存: {c.get('rel','')}]"
        for n, reason in failed.items():
            repl.setdefault(n.lower(), f"[视频未缓存: {reason}]")

        prompt_idx = 0
        for msg in (getattr(event, "messages", None) or []):
            while prompt_idx < len(req.user_prompt) and not (
                    req.user_prompt[prompt_idx].name == "message"
                    and req.user_prompt[prompt_idx].source == "system"):
                prompt_idx += 1
            if prompt_idx >= len(req.user_prompt):
                break
            p = req.user_prompt[prompt_idx]; prompt_idx += 1
            if not isinstance(p.content, str) or "[Video name:" not in p.content:
                continue

            def _sub(m, _repl=repl):
                name = (m.group(1) or "").strip()
                hit = _repl.get(name.lower())
                if hit:
                    return hit
                return f"[视频 {name}（未预缓存，可用 analyze_video 分析）]"

            p.content = self._VIDEO_PLACEHOLDER_RE.sub(_sub, p.content)

    # ── Prompt ──

    @on.llm_request(priority=Priority.LOW)
    async def _inject(self, event, req: LLMRequest, *_):
        if not self.enabled: return
        sid = self._sid(event)
        if not sid: return
        has_pending = sid in self._pending
        cached_list = self._cached_videos.get(sid) or []
        # pending 有时效：超过 10 分钟没被用掉就不再提示（避免每轮都刷）
        if has_pending:
            if time.time() - self._pending[sid].get("ts", 0) > 600:
                self._pending.pop(sid, None)
                has_pending = False
        # ① 先把消息里框架写的「(Video size over 10MB, not cached)」改写成可用信息
        #    （每条 prompt 都是当轮新渲染的，所以每轮都要改；改写内容会随消息进历史）
        try:
            self._rewrite_video_notes(event, req, sid)
        except Exception as e:
            logger.debug("[VC] 改写视频占位失败: %s", e)

        if (not has_pending and not cached_list
                and not self._sid_sessions.get(sid)
                and not self._video_failures.get(sid)):
            return
        hint = ""
        if cached_list:
            # 已缓存 → 在 system prompt 里给「怎么用」的提示（路径已在消息里）
            first = cached_list[-1]
            hint = (f"\n【视频】已缓存的视频用 analyze_video(local_path=\"{first.get('rel','')}\") "
                    f"分析内容（画面 + 语音转写一起）")
        elif self.video_analysis_enabled and has_pending:
            url = self._pending[sid].get("url", "")
            if BILI_RE.search(url):
                hint = "\n【B站视频】analyze_video / send_video / search_bili_video"
            else:
                hint = "\n【视频】analyze_video 分析内容"
        if hint:
            for p in req.system_prompt:
                if p.name and "tool" in p.name.lower():
                    p.content += hint; break
            else:
                if req.system_prompt: req.system_prompt[-1].content += hint

    # ── 视频检测 ──

    @staticmethod
    def _is_video_ele(ele) -> bool:
        """判断是否为视频元素（真实类名是 Video；兼容子类与包装类）"""
        n = type(ele).__name__
        if n == "Video" or n.endswith("Video"):
            return True
        t = getattr(ele, "type", None)
        return str(getattr(t, "name", t) or "").lower() == "video" and hasattr(ele, "file")

    def _iter_videos(self, chain, depth: int = 0):
        """递归遍历消息链找 Video 元素。

        ⚠️ 只扫顶层是不够的，视频可能藏在：
          - `Reply.chain`（引用消息，单数 MessageChain）
          - `Forward.chains`（合并转发，复数 list[MessageChain]）
        框架对这类视频常因拿不到 file_size 而放弃缓存，所以这里必须递归接管。
        """
        if depth > 4:
            return
        for ele in chain or []:
            try:
                if self._is_video_ele(ele):
                    yield ele
                # 引用消息：单条内层链
                inner = getattr(ele, "chain", None)
                if inner:
                    yield from self._iter_videos(inner, depth + 1)
                # 合并转发：内层链列表
                for c in (getattr(ele, "chains", None) or []):
                    yield from self._iter_videos(c, depth + 1)
            except Exception:
                continue

    @staticmethod
    def _reply_message_ids(chain) -> list:
        """取出「内层链为空」的引用元素 ID。

        内层链有内容说明适配器已完整解析过，本地找过没有就是真没有；
        只有链为空（没解析出来）才值得按 ID 主动拉一次，避免白调接口。
        """
        ids = []
        for ele in chain or []:
            try:
                if type(ele).__name__ == "Reply":
                    if getattr(ele, "chain", None):
                        continue
                    mid = getattr(ele, "message_id", None)
                    if mid:
                        ids.append(str(mid))
            except Exception:
                continue
        return ids

    def _find_cached(self, safe_name: str) -> str:
        """在缓存目录里找同一个原始文件名已缓存过的文件（避免同一视频反复下载）"""
        try:
            suffix = "_" + safe_name
            for fn in os.listdir(self.other_cache_dir):
                if fn.endswith(suffix) or fn == safe_name:
                    p = os.path.join(self.other_cache_dir, fn)
                    if os.path.isfile(p) and os.path.getsize(p) > 0:
                        return p
        except Exception:
            pass
        return ""

    def _remember_cached(self, sid: str, safe_name: str, path: str):
        """登记到该会话的已缓存列表（列表结构，支持一条消息里多个视频）"""
        try:
            rel = os.path.relpath(path, get_data_path()).replace("\\", "/")
        except Exception:
            rel = path
        try:
            size_mb = os.path.getsize(path) / (1024 * 1024)
        except Exception:
            size_mb = 0.0
        lst = self._cached_videos.setdefault(sid, [])
        for c in lst:
            if c.get("path") == path:
                c["ts"] = time.time()
                return
        lst.append({"orig_name": safe_name, "path": path, "rel": rel,
                    "size_mb": size_mb, "ts": time.time()})

    @staticmethod
    def _explain_failure(e: Exception) -> str:
        msg = str(e)
        if "超过上限" in msg or type(e).__name__ == "DownloadTooLarge":
            return "超过单文件大小上限"
        return f"下载失败（{type(e).__name__}）"

    async def _cache_incoming_video(self, sid: str, url: str, name: str = "") -> str:
        """把收到的视频下载到本地缓存目录，供 bot 直接使用（返回本地路径）

        框架在 file_size 缺失时不会缓存视频（且文案会误导成"超过 10MB"），
        这里自己下下来并记住路径，下一轮对话告诉 bot。
        """
        try:
            os.makedirs(self.other_cache_dir, exist_ok=True)
            safe = re.sub(r"[^\w.\-]+", "_", (name or "").strip()) or "video.mp4"
            if not re.search(r"\.(mp4|mov|mkv|webm|avi|flv|m4v|ts|wmv)$", safe, re.I):
                safe += ".mp4"
            # 已有同名缓存 → 直接复用（同一视频常被反复引用，不必重复下载/转写）
            existing = self._find_cached(safe)
            if existing:
                self._remember_cached(sid, safe, existing)
                logger.info("[VC] 视频已在缓存中，复用: %s", os.path.basename(existing))
                return existing
            path = os.path.join(self.other_cache_dir, f"v{int(time.time())}_{safe}")
            max_bytes = int(self.cache_max_file_mb * 1024 * 1024) if self.cache_max_file_mb > 0 else 0
            if not (os.path.exists(path) and os.path.getsize(path) > 0):
                try:
                    await download_video(url, path, timeout=self.dl_timeout, max_bytes=max_bytes)
                except Exception as de:
                    self._video_failures.setdefault(sid, {})[safe] = self._explain_failure(de)
                    raise
            if os.path.exists(path) and os.path.getsize(path) > 0:
                self._remember_cached(sid, safe, path)
                logger.info("[VC] 视频已缓存(%d): %s (%.2fMB)",
                            len(self._cached_videos.get(sid) or []), os.path.basename(path),
                            os.path.getsize(path) / (1024 * 1024))
                return path
            logger.warning("[VC] 视频缓存后文件为空: %s", url)
        except Exception as e:
            logger.warning("[VC] 视频缓存失败: %s", e)
        return ""

    def _any_group_needs_stt(self) -> bool:
        """是否还有模型组需要 ASR 转写（即：不是所有启用的组都自带音视频理解）"""
        if not self._profiles:
            return True
        return any(not p.native_audio for p in self._profiles)

    async def _cache_then_transcribe(self, sid: str, url: str, name: str = ""):
        """缓存视频后立刻并行启动转写（这样 bot 真要分析时通常已算好）

        若当前启用的模型组**全部**自带音视频理解（不需要 STT），就不再跑 ASR。

        ⚠️ 统一挂在「后台任务闸」下（S2）：原先裸 create_task，群里连发多个视频
        会同时开多路下载 + 多路 ffmpeg + 多路 ASR 切片，把 CPU/带宽打满。
        """
        async with self._bg_gate():
            path = await self._cache_incoming_video(sid, url, name)
            if path:
                self._start_transcript_task(path, skip_asr=not self._any_group_needs_stt())

    def _bg_gate(self):
        """后台重活的统一并发闸（缓存 / 转写）。

        与分析任务的闸分开：这两类是被动触发的（收到视频就缓存），
        不能让它们把「分析任务的槽」占满，否则用户主动发起的分析会排队。
        上限取 max(2, 每会话并发数)，避免连发视频时同时开太多路。
        """
        if self._bg_sem is None or self._bg_sem_limit != self._bg_limit():
            self._bg_sem = asyncio.Semaphore(self._bg_limit())
            self._bg_sem_limit = self._bg_limit()
        return self._bg_sem

    def _bg_limit(self) -> int:
        return max(2, min(16, self.max_parallel_per_chat * 2))

    # ── 语音转写（把"声音"变成模型读得到的文字） ──

    def _asr_ready(self) -> bool:
        return bool(self.audio_enabled and self.audio_base_url and self.audio_model)

    def _transcript_key(self, video_path: str) -> str:
        try:
            st = os.stat(video_path)
            raw = f"{os.path.abspath(video_path)}:{st.st_size}:{int(st.st_mtime)}"
        except Exception:
            raw = str(video_path)
        return hashlib.md5(raw.encode()).hexdigest()[:16]

    def _transcript_path(self, key: str) -> str:
        d = os.path.join(self.other_cache_dir, ".transcripts")
        os.makedirs(d, exist_ok=True)
        return os.path.join(d, f"{key}.json")

    def _load_transcript(self, key: str):
        try:
            p = self._transcript_path(key)
            if os.path.isfile(p):
                with open(p, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception:
            pass
        return None

    def _save_transcript(self, key: str, data: dict):
        try:
            with open(self._transcript_path(key), "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
        except Exception as e:
            logger.debug("[VC] 转写缓存写入失败: %s", e)

    async def _segment_transcribe(self, wav: str, speech_ranges, work: str):
        """兜底：ASR 不返回时间戳时，用本地 VAD 切块逐块识别来造时间轴。

        只在「服务只给纯文本」时才会走到这里（例如硅基流动 SenseVoice）。
        """
        blocks = merge_ranges(speech_ranges, gap=self.audio_gap_sec,
                              max_len=self.audio_block_sec,
                              max_count=self.audio_max_blocks)
        if not blocks:
            return []
        sem = asyncio.Semaphore(self.audio_concurrency)

        async def _one(i: int, s: float, e: float):
            async with sem:
                try:
                    seg_path = os.path.join(work, f"blk_{i:03d}.wav")
                    await slice_audio(wav, seg_path, s, e)
                    r = await asr_transcribe(seg_path, self.audio_base_url,
                                             self.audio_api_key, self.audio_model,
                                             timeout=self.audio_timeout,
                                             language=self.audio_language,
                                             use_proxy=self.audio_use_proxy,
                                             extra_headers=self.audio_extra_headers,
                                             extra_body=self.audio_extra_body)
                    txt = (r.get("text") or "").strip()
                    return {"start": s, "end": e, "text": txt} if txt else None
                except Exception as ex:
                    logger.debug("[VC] 切块转写失败 [%.1f-%.1f]: %s", s, e, ex)
                    return None

        results = await asyncio.gather(*(_one(i, s, e) for i, (s, e) in enumerate(blocks)))
        logger.info("[VC] 切块转写：%d 块 → %d 段有文字（ASR 无原生时间戳）",
                    len(blocks), sum(1 for x in results if x))
        return [x for x in results if x]

    async def _build_transcript(self, video_path: str, bvid: str = "", cid: int = 0,
                                skip_asr: bool = False) -> dict:
        """完整转写流程。

        ① B 站视频优先用**官方字幕**（精确时间轴、免费、不用抽音轨）
        ② 没有字幕再走 ASR：抽音轨 → 静音分析 → 转写 → (必要时切块补轴)
        """
        want_sub = bool(self.bili_use_subtitle and bvid and cid)
        if not self.audio_enabled or (not want_sub and not self._asr_ready()):
            return {}
        key = self._transcript_key(video_path)
        cached = self._load_transcript(key)
        if cached:
            return cached
        t0 = time.time()

        # ① B 站官方字幕优先
        if want_sub:
            try:
                segs, lan_doc, n_tracks = await get_bilibili_subtitle(
                    bvid, cid, self.bili_cookie, prefer_lan=self.audio_language,
                    timeout=self.dl_timeout)
                if segs:
                    doc = build_timeline_doc(
                        segs, [], 0,
                        header=(f"【视频字幕（B站 {lan_doc or 'CC'}，共{n_tracks}条轨）"
                                f"· 时间轴与画面帧口径一致】"))
                    data = {"doc": doc, "segments": segs, "speech": [],
                            "total": round(float(segs[-1].get("end") or 0), 3),
                            "native_axis": True, "asr_source": "bilibili_subtitle",
                            "elapsed": round(time.time() - t0, 2)}
                    self._save_transcript(key, data)
                    logger.info("[VC] 用上 B 站官方字幕：%d 条（%s，用时 %.1fs）",
                                len(segs), lan_doc or "?", time.time() - t0)
                    return data
                logger.info("[VC] 该 B 站视频无可用字幕，转音频识别")
            except Exception as e:
                logger.info("[VC] B 站字幕获取失败，转音频识别: %s", e)
        # ② 音频识别（自带音视频理解的模型不需要）
        if skip_asr:
            logger.info("[VC] 该模型组自带音视频理解，跳过语音识别")
            return {}
        if not self._asr_ready():
            return {}
        work = os.path.join(self.other_cache_dir, f".asr_{key}")
        os.makedirs(work, exist_ok=True)
        try:
            wav = os.path.join(work, "audio.wav")
            await extract_audio(video_path, wav)
            speech, total = await detect_speech_ranges(wav, noise_db=self.audio_silence_db)
            if self.audio_max_sec > 0 and total > self.audio_max_sec:
                logger.info("[VC] 音频 %.0fs 超过转写上限 %.0fs，跳过", total, self.audio_max_sec)
                return {}
            result = await asr_transcribe(wav, self.audio_base_url, self.audio_api_key,
                                          self.audio_model, timeout=self.audio_timeout,
                                          language=self.audio_language,
                                          use_proxy=self.audio_use_proxy,
                                          extra_headers=self.audio_extra_headers,
                                          extra_body=self.audio_extra_body)
            segs = result.get("segments") or []
            native = bool(segs)
            if segs:
                # 兜底校正：有些服务给毫秒却用秒的字段名
                segs = normalize_segments_by_duration(segs, total)
            if not segs and (result.get("text") or "").strip() and speech:
                segs = await self._segment_transcribe(wav, speech, work)
            if result.get("silent") and not segs:
                logger.info("[VC] 音频识别完成：这段 %.1fs 音频里没有可转写的语音", total)
            # 即使没有语音，也把「有声但无人声」的片段标出来（L2）
            doc = build_timeline_doc(segs, speech, total)
            data = {
                "doc": doc, "segments": segs,
                "speech": [[round(s, 3), round(e, 3)] for s, e in speech],
                "total": round(total, 3),
                "native_axis": native,
                "asr_source": result.get("source", ""),
                "elapsed": round(time.time() - t0, 2),
            }
            self._save_transcript(key, data)
            logger.info("[VC] 语音转写完成：%.1fs，%d 段%s（用时 %.1fs）",
                        total, len(segs), "，原生时间轴" if native else "，本地切块补轴",
                        time.time() - t0)
            return data
        except ASRError as e:
            logger.warning("[VC] 语音转写失败（不影响视频分析）: %s", e)
        except Exception as e:
            logger.warning("[VC] 语音转写异常（不影响视频分析）: %s", e)
        return {}

    def _start_transcript_task(self, video_path: str, bvid: str = "", cid: int = 0,
                               skip_asr: bool = False):
        """后台启动转写（与视频缓存并行，拿到就缓存好，分析时零等待）"""
        want_sub = bool(self.bili_use_subtitle and bvid and cid)
        if not self.audio_enabled or (not want_sub and not self._asr_ready()):
            return
        if skip_asr and not want_sub:
            return
        if not video_path or not os.path.isfile(video_path):
            return
        key = self._transcript_key(video_path)
        if key in self._asr_tasks and not self._asr_tasks[key].done():
            return
        if self._load_transcript(key):
            return
        task = asyncio.create_task(self._build_transcript(video_path, bvid=bvid, cid=cid,
                                                          skip_asr=skip_asr))
        self._asr_tasks[key] = task

        def _cleanup(_t, k=key):
            if self._asr_tasks.get(k) is _t:
                self._asr_tasks.pop(k, None)
        task.add_done_callback(_cleanup)

    async def _get_transcript(self, video_path: str, wait: float,
                              bvid: str = "", cid: int = 0,
                              skip_asr: bool = False) -> dict:
        """取转写结果：缓存命中→秒用；有进行中任务→最多等 wait 秒；否则现场跑"""
        want_sub = bool(self.bili_use_subtitle and bvid and cid)
        if not self.audio_enabled or (not want_sub and not self._asr_ready()):
            return {}
        if not video_path or not os.path.isfile(video_path):
            return {}
        key = self._transcript_key(video_path)
        cached = self._load_transcript(key)
        if cached:
            return cached
        task = self._asr_tasks.get(key)
        if task is None or task.done():
            self._start_transcript_task(video_path, bvid=bvid, cid=cid, skip_asr=skip_asr)
            task = self._asr_tasks.get(key)
        if task is None:
            return {}
        try:
            return await asyncio.wait_for(asyncio.shield(task), timeout=max(0.1, wait))
        except asyncio.TimeoutError:
            logger.info("[VC] 语音转写未在 %.0fs 内完成，本次分析先不带转写", wait)
            return {}
        except Exception as e:
            logger.warning("[VC] 取转写失败: %s", e)
            return {}

    @on.im_message(priority=Priority.HIGH)
    async def _detect(self, event: KiraMessageEvent, *_):
        if not self.enabled or not self._ok(event):
            return
        # 防御：bot 自己发的视频（如 send_video 发出去的）不必再当"收到的视频"
        # 缓存/转写一遍 —— 省流量、省 ASR 费用
        if self._is_self_message(event):
            return
        sid = self._sid(event)
        url = None
        vname = ""
        chain_top = getattr(event.message, "chain", None)
        saw_video_ele = False      # 链里出现过 Video（即便 file 为空）
        has_video_seg = False      # raw_message 里有 video 段

        # 1) 消息链里的 Video 元素（含引用 chain / 合并转发 chains）
        try:
            for ele in self._iter_videos(chain_top):
                saw_video_ele = True
                f = getattr(ele, "file", "") or ""
                if f:
                    url = str(f)
                    vname = str(getattr(ele, "name", "") or "")
                    break
        except Exception:
            pass

        # 2) raw_message（OneBot 原始结构）里找 video 段
        raw = None
        if not url:
            for a in ("raw_message", "source_message", "original_message"):
                v = getattr(event.message, a, None) or getattr(event, a, None)
                if v:
                    if isinstance(v, str):
                        try: raw = json.loads(v)
                        except: continue
                    elif isinstance(v, (dict, list)): raw = v
                    if raw: break
            segs = None
            if isinstance(raw, dict):
                segs = raw.get("message")
            elif isinstance(raw, list):
                segs = raw
            if isinstance(segs, list):
                for s in segs:
                    if isinstance(s, dict) and s.get("type") == "video":
                        has_video_seg = True
                        url = (s.get("data") or {}).get("url") or ""
                        break

        # 3) 兜底：主动调 OneBot 接口拉（当前消息 → 引用消息）
        #    只在「确实有视频迹象」时才调，避免每条纯文字消息都白跑一次 API
        reply_ids = self._reply_message_ids(chain_top)
        # 主动调 get_msg 是 OneBot 的能力，其他平台只靠消息链/raw_message
        if not url and self._is_qq(event) and (saw_video_ele or has_video_seg or reply_ids):
            try:
                ad = self.ctx.adapter_mgr.get_adapter(event.adapter.name)
                cl = ad.get_client()

                async def _scan_get_msg(mid) -> str:
                    """取一条消息，返回其中的视频 URL"""
                    if not mid:
                        return ""
                    rm = await cl.send_action("get_msg", {"id": mid}, timeout=15)
                    if isinstance(rm, dict):
                        for s in (rm.get("message") or []):
                            if isinstance(s, dict) and s.get("type") == "video":
                                return (s.get("data") or {}).get("url", "") or ""
                    return ""

                # 3a) 当前消息
                mid = getattr(event.message, "message_id", None) or getattr(event, "message_id", None)
                try:
                    url = await _scan_get_msg(mid)
                except Exception:
                    url = ""
                # 3b) 引用消息（适配器若没解析出内层链，这里按被引用消息 ID 主动拉）
                if not url:
                    for rid in reply_ids:
                        try:
                            url = await _scan_get_msg(rid)
                        except Exception:
                            url = ""
                        if url:
                            logger.info("[VC] 通过引用消息 ID 主动拉到视频: %s", rid)
                            break
            except Exception:
                pass
        if url:
            self._pending[sid] = {"url": url, "source": "onebot", "ts": time.time()}
            # 缓存策略（cache_scope）：
            #   all       → 每条消息就缓存（旧行为，最耗）
            #   batch     → 等进入 bot 批次后再缓存
            #   mentioned → 只在被 @ / 引用 / 唤醒时才缓存（默认，最省）
            # 注意：URL 始终记录在 _pending，所以 bot 主动调工具时永远有源可用。
            if self.auto_cache_video:
                if str(url).startswith(("http://", "https://")):
                    if self.cache_scope == "all":
                        self._spawn(self._cache_then_transcribe(sid, url, vname), name="vc_cache")
                elif os.path.isfile(url):
                    self._remember_cached(sid, os.path.basename(url), url)

    # ────────────── 工具1：search_bili_video ──────────────

    @register.tool(
        name="search_bili_video",
        description="搜索B站视频，返回结果列表（含标题/UP主/时长/播放量/简介）。用户要找B站视频时调用。",
        params={
            "type": "object",
            "properties": {"keyword": {"type": "string", "description": "搜索关键词"}},
            "required": ["keyword"],
        },
    )
    async def _tool_search(self, event, keyword: str) -> str:
        if not self.bili_enabled: return "B站功能未启用"
        try: rs = await search_bili(keyword, self.bili_search_n, self.bili_cookie)
        except Exception as e: return f"⚠️ 搜索失败：{e}"
        if not rs: return "未找到相关视频"
        lines = [f"🔍 搜索「{keyword}」结果："]
        for i, r in enumerate(rs, 1):
            d = r.get("duration", 0); desc = r.get("desc", "")
            lines.append(f"{i}. {r.get('title','')}\n   👤 {r.get('author','')} | ⏱ {d//60}:{d%60:02d} | 👁 {r.get('play',0)}\n   BV: {r.get('bvid','')}")
            if desc and self.search_show_desc: lines.append(f"   📝 {desc[:self.search_desc_max_chars]}")
        lines.append("\n→ send_video(bvid=...) 直接发送\n→ analyze_video(bvid=...) 分析")
        return "\n".join(lines)

    # ────────────── 工具2：send_video ──────────────

    @register.tool(
        name="send_video",
        description="下载B站视频并发送到QQ（可压缩），也支持本地视频路径发送。用户要求下载/发B站视频时调用。传关键词返回候选列表。",
        params={
            "type": "object",
            "properties": {
                "target": {"type": "string", "description": "B站链接/BV号/搜索关键词"},
                "bvid": {"type": "string", "description": "已知BV号（优先）"},
                "local_path": {"type": "string", "description": "本地视频路径（绝对路径或相对 data/ 目录的相对路径）"},
                "quality": {"type": "string", "description": "质量: low|medium|original", "default": ""},
            },
        },
    )
    async def _tool_send_video(self, event, target: str = "", bvid: str = "", local_path: str = "", quality: str = "") -> str:
        if not self._is_qq(event): return "当前不是QQ"
        sid = self._sid(event)
        if local_path:
            # 本地文件（绝对路径直用，相对路径基于 get_data_path() 解析）
            lp = local_path.strip()
            if os.path.isabs(lp) and os.path.isfile(lp): pass
            else:
                # 相对路径 → 以 get_data_path() 为基准
                resolved = os.path.join(get_data_path(), lp)
                if os.path.isfile(resolved): lp = resolved
                else: return f"⚠️ 找不到文件: {local_path}（相对路径以 data/ 为基准）"
            q = quality or self.send_video_quality
            # 压缩
            os.makedirs(self.other_cache_dir, exist_ok=True)
            out_path = os.path.join(self.other_cache_dir, f"send_local_{Path(lp).stem}.mp4")
            try:
                if q == "low": await compress_video(lp, out_path, max_width=360, crf=32)
                elif q == "medium": await compress_video(lp, out_path, max_width=720, crf=28)
                elif q == "original": out_path = lp
                else: await compress_video(lp, out_path, max_width=720, crf=28)
            except Exception as e:
                logger.warning("[VC] 本地视频压缩失败，改发原文件: %s", e)
                out_path = lp
            try:
                ad = self.ctx.adapter_mgr.get_adapter(event.adapter.name)
                cl = ad.get_client()
                # 同 _send_video_by_bvid：框架默认 10 秒超时对"发视频"远远不够
                _send_to = 180
                if "gm:" in sid:
                    await cl.send_action("send_group_msg", {"group_id": int(sid.split(":")[-1]), "message": [{"type": "video", "data": {"file": out_path}}]}, timeout=_send_to)
                elif "dm:" in sid:
                    await cl.send_action("send_private_msg", {"user_id": int(sid.split(":")[-1]), "message": [{"type": "video", "data": {"file": out_path}}]}, timeout=_send_to)
                else: return "无法判断群聊/私聊"
            except Exception as e:
                msg = str(e)
                if "超时" in msg or "timeout" in msg.lower():
                    return ("⚠️ 发送超时（视频可能仍在发送中）：协议端上传视频较慢，"
                            "**请不要重发** —— 重发会导致群里出现多条相同视频。\n"
                            f"   原始错误: {msg}")
                return f"⚠️ 发送失败：{msg}"
            return f"✅ 已发送本地视频：{Path(lp).name} | 质量: {q}"
        if bvid:
            bv = bvid.strip()
            if not bv.startswith("BV"): bv = (await extract_bvid(bv)) or ""
            if bv: return await self._send_video_by_bvid(event, bv, quality)
        if target:
            bv = await extract_bvid(target, self.dl_timeout)
            if bv: return await self._send_video_by_bvid(event, bv, quality)
        keyword = (target or "").strip()
        if not keyword: return "请提供B站链接/BV号/搜索关键词"
        try: rs = await search_bili(keyword, self.bili_search_n, self.bili_cookie)
        except Exception as e: return f"⚠️ 搜索失败：{e}"
        if not rs: return f"未找到「{keyword}」相关视频"
        lines = [f"🔍 搜索「{keyword}」结果："]
        for i, r in enumerate(rs, 1):
            d = r.get("duration", 0); desc = r.get("desc", "")
            lines.append(f"{i}. {r.get('title','')}\n   👤 {r.get('author','')} | ⏱ {d//60}:{d%60:02d} | 👁 {r.get('play',0)}")
            if desc and self.search_show_desc: lines.append(f"   📝 {desc[:self.search_desc_max_chars]}")
            lines.append(f"   BV: {r.get('bvid','')}")
        lines.append("\n→ send_video(bvid=BVxxx)")
        return "\n".join(lines)

    async def _send_video_by_bvid(self, event, bvid: str, quality: str = "", sid: str = "", adapter_name: str = "") -> str:
        """发送B站视频到QQ（内置 NapCat 分块上传防断连）
        event 可为 None（auto_send 钩子 discard 后用 adapter_name 参数代替）
        """
        # BV 号规范化：bot 传进来的值可能带空格/换行，或整个链接。
        # 不处理的话会原样发给 B站 → 返回 code=-400（请求错误），报错很难懂。
        logger.info("[VCDIAG] a) send_video 进入 raw=%r", bvid)
        _raw_bvid = str(bvid or "")
        bvid = self._normalize_bvid(_raw_bvid)
        logger.info("[VCDIAG] b) bvid 规范化 -> %s", bvid)
        if not bvid:
            # 可能是 b23.tv 短链 / 带短链的分享文本 → 联网解析一次
            try:
                bvid = await extract_bvid(_raw_bvid, self.dl_timeout)
            except Exception:
                bvid = ""
        if not bvid:
            return ("⚠️ 没有解析出有效的 BV 号。请传 BV 号本身（形如 BV1xx411c7mD），"
                    "或完整的 B站视频链接 / b23.tv 短链。")
        logger.info("[VCDIAG] c) 开始请求B站视频信息（网络）…")
        try: info = await get_bili_info(bvid, self.bili_cookie)
        except Exception as e:
            # 把 B站的 code 翻译成人话，方便用户判断是"视频没了"还是"参数不对"
            msg = str(e)
            if "-400" in msg:
                return (f"⚠️ 获取信息失败：B站返回「请求错误」。"
                        f"通常是 BV 号无效或视频不存在（已删除/私密）。\n"
                        f"   BV: {bvid}\n   原始报错: {msg}")
            return f"⚠️ 获取信息失败：{msg}（BV: {bvid}）"
        logger.info("[VCDIAG] d) 拿到视频信息 title=%r", info.get("title"))
        d = info.get("duration", 0); title = info.get("title", bvid)
        if self.bili_max_dl and d > self.bili_max_dl:
            return f"⏱ 「{title}」时长{d}s超上限，不下发"
        os.makedirs(self.bili_cache_dir, exist_ok=True)
        q = quality or self.bili_download_quality
        try:
            path, _ = await download_bili_video(bvid, self.bili_cache_dir, info=info,
                cookie=self.bili_cookie, timeout=self.dl_timeout, max_seconds=self.bili_max_dl,
                quality=q)
        except Exception as e: return f"⚠️ 下载失败：{e}"
        # 下载后是否再压缩：只在 compress_quality 比下载质量更低时才有意义
        _rank = {"low": 1, "medium": 2, "original": 3}
        cq = self.bili_compress_quality
        if cq and cq != "original" and _rank.get(cq, 3) < _rank.get(q, 3):
            try:
                compressed = os.path.join(self.bili_cache_dir, f"send_{bvid}_compressed.mp4")
                if cq == "low": await compress_video(path, compressed, max_width=360, crf=32)
                elif cq == "medium": await compress_video(path, compressed, max_width=720, crf=28)
                out_path = compressed
            except Exception as e:
                logger.warning("[VC] B站视频压缩失败，改发原文件: %s", e)
                out_path = path
        else:
            out_path = path

        send_sid = sid or (self._sid(event) if event else "")
        if not send_sid: return "⚠️ 无法获取会话ID"
        ad_name = adapter_name or (event.adapter.name if event and hasattr(event, 'adapter') else "")
        if not ad_name: return "⚠️ 无法获取 adapter"
        try:
            ad = self.ctx.adapter_mgr.get_adapter(ad_name)
            cl = ad.get_client()
            is_group = "gm:" in send_sid
            target_id = int(send_sid.split(":")[-1])

            # ── NapCat 分块上传（第三方扩展 action，**默认关闭**，配置可开） ──
            # 直接用本地路径发送是标准做法（AstrBot 等同类软件都这么做），
            # 对 NapCat / SnowLuma 都能工作。这里只在用户显式开启时才尝试。
            file_ref = out_path  # 兜底/默认：直接发本地路径
            try:
                file_size = Path(out_path).stat().st_size
            except Exception:
                file_size = 0
            if self.napcat_stream and file_size > 1024 * 1024 and not self._stream_unsupported:
                try:
                    filename = f"{bvid}.mp4"
                    chunk_size = 512 * 1024
                    total_chunks = max(1, math.ceil(file_size / chunk_size))
                    stream_id = uuid.uuid4().hex
                    digest = hashlib.sha256()
                    with open(out_path, "rb") as f:
                        for chunk in iter(lambda: f.read(1024 * 1024), b""):
                            digest.update(chunk)
                    sha256 = digest.hexdigest()
                    retention = 600000
                    with open(out_path, "rb") as f:
                        for ci in range(total_chunks):
                            chunk = f.read(chunk_size)
                            if not chunk: break
                            _raise_for_stream(await cl.send_action("upload_file_stream", {
                                "stream_id": stream_id, "chunk_index": ci,
                                "total_chunks": total_chunks, "file_size": file_size,
                                "filename": filename, "expected_sha256": sha256,
                                "file_retention": retention,
                                "chunk_data": base64.b64encode(chunk).decode("ascii"),
                            }, timeout=120))
                    resp = await cl.send_action("upload_file_stream", {
                        "stream_id": stream_id, "is_complete": True,
                        "total_chunks": total_chunks, "file_size": file_size,
                        "filename": filename, "expected_sha256": sha256,
                        "file_retention": retention,
                    }, timeout=120)
                    _raise_for_stream(resp)
                    napcat_path = _extract_stream_path(resp)
                    if napcat_path:
                        file_ref = napcat_path
                except Exception as e:
                    msg = str(e).lower()
                    if "不支持" in msg or "unsupported" in msg or "unknown action" in msg or "not found" in msg:
                        self._stream_unsupported = True  # 记住：本次运行不再尝试
                    logger.info("[VC] stream 上传不可用，降级直接发路径: %s", e)

            # 发送视频（file_ref 是 NapCat 引用路径或本地路径）
            # ⚠️ 必须显式给足超时：框架 send_action 默认只有 10 秒，而协议端
            #    发视频要走 highway 上传（把整个视频读进内存再传），10 秒常常
            #    不够 —— 一旦超时，上层的"失败"会让 bot 重发，群里就会出现
            #    多条相同视频；协议端也可能因此状态混乱（曾导致 ws 断开/退出）。
            _send_to = 180
            if is_group:
                await cl.send_action("send_group_msg", {
                    "group_id": target_id,
                    "message": [{"type": "video", "data": {"file": file_ref, "name": f"{bvid}.mp4"}}],
                }, timeout=_send_to)
            else:
                await cl.send_action("send_private_msg", {
                    "user_id": target_id,
                    "message": [{"type": "video", "data": {"file": file_ref, "name": f"{bvid}.mp4"}}],
                }, timeout=_send_to)
        except Exception as e:
            msg = str(e)
            # 超时 ≠ 发送失败：协议端很可能**还在上传**。这时必须明确提示
            # bot 不要重发，否则群里会重复出现同一个视频。
            if "超时" in msg or "timeout" in msg.lower():
                return ("⚠️ 发送超时（视频可能仍在发送中）：协议端上传视频较慢，"
                        "**请不要重发** —— 重发会导致群里出现多条相同视频。\n"
                        f"   原始错误: {msg}")
            return f"⚠️ 发送失败：{msg}"
        return f"✅ 已发送：{title}\nBV: {bvid} | ⏱ {d//60}:{d%60:02d} | 质量: {q}\n📁 本地路径: {out_path}"
    # ────────────── 工具3：analyze_video（分析开关控制） ──────────────

    @register.tool(
        name="analyze_video",
        description=("分析视频内容。支持QQ视频/B站视频/本地路径。首次返回session_id，追问传回。"
                     "只看某一段时间就传 start_sec/end_sec（数字秒）；一次看多段传 segments=[[起,止],...]（最多5段、每段≤300秒）。"
                     "时间段分析会复用已下载的视频，不会重新下载。"
                     "【重要】首次分析是**后台进行**的：本工具会立刻返回一句「已开始分析」，"
                     "你这时只要告诉用户「正在看，稍等一下」即可，**绝对不要猜测或编造视频内容**；"
                     "分析完成后系统会把结果送给你，那时再正式回答用户。"
                     "追问（带 session_id）是同步的，直接拿结果回答即可。"
                     "用户指定了模型（如「用 Agnes 分析」）就传 model=那个名字；"
                     "**同一次对话里后续的每次调用（包括追问）都要继续带上同一个 model**，"
                     "否则会退回默认模型组、可能答非所问。"
                     "不确定有哪些可选就先不传，传错时返回值会列出全部模型组。"
                     "同一个视频不要重复调用本工具——若已在分析中，返回值会告诉你。"),
        params={
            "type": "object",
            "properties": {
                "question": {"type": "string", "description": "留空=完整分析"},
                "bvid": {"type": "string", "description": "B站BV号"},
                "deep_analysis": {"type": "boolean", "description": "深度视觉分析", "default": False},
                "local_path": {"type": "string", "description": "本地视频路径"},
                "session_id": {"type": "string", "description": "追问用session_id"},
                "model": {"type": "string", "description": "指定用哪个模型组（填别名/模型名/组号，如 \"Agnes\"）；不填则按优先级自动选"},
                "start_sec": {"type": "number", "description": "只分析从第几秒开始（数字秒）", "default": 0},
                "end_sec": {"type": "number", "description": "分析到第几秒结束；传 0 = 到视频结尾", "default": 0},
                "segments": {
                    "type": "array",
                    "description": "多段分析：[[起秒,止秒],[起秒,止秒]]，最多5段、每段≤300秒",
                    "items": {"type": "array", "items": {"type": "number"}},
                },
            },
        },
    )
    async def _tool_analyze(self, event, question: str = "", bvid: str = "",
                             deep_analysis: bool = False, local_path: str = "",
                             session_id: str = "",
                             start_sec: float = 0, end_sec: float = 0,
                             segments=None, model: str = "") -> str:
        """提交入口（L1：永不阻塞）。

        这里只做「参数校验 + 立即返回的轻量查询」，任何重活都交给后台任务。
        这样工具调用毫秒级返回，既不在主 LLM 链路上阻塞，也不受框架
        「工具调用超时」限制。
        """
        if not self.video_analysis_enabled:
            return "⚠️ 视频分析功能已关闭，可在 WebUI 启用"

        profile_spec = None
        if model.strip():
            profile_spec = self._find_profile(model)
            if profile_spec is None:
                return (f"⚠️ 找不到名为「{model}」的模型组。"
                        f"当前可用：{self._list_profiles_text()}")
        segs, err = self._parse_segments(start_sec, end_sec, segments)
        if err: return f"⚠️ {err}"

        sid = self._sid(event)
        if not sid: return "无法获取会话ID"

        # ① 已有 session_id：轻量分支（不下载、不抽帧）
        if session_id:
            sess = self._get_by_session_id(session_id)
            if not sess: return f"⚠️ session_id={session_id} 不存在"
            self._register_session(sess, sid)
            if segs:
                # 时间段分析要重新抽帧 → 按首次分析处理（占并发槽、可异步）
                return await self._submit_or_run(
                    sid, "segment", segs=segs, question=question,
                    model_spec=profile_spec, sess=sess,
                    title=sess.title, detail=f"时间段 {self._seg_label(segs)}")
            if question:
                return await self._submit_or_run(
                    sid, "followup", question=question, model_spec=profile_spec,
                    sess=sess, title=sess.title)
            return (f"📌 session_id={session_id}\n🤖 {sess.analysis_model}\n"
                    f"{self._link_line(sess.host_url)}━━━\n{sess.analysis[:500]}\n━━━\n"
                    f"追问用 session_id=\"{session_id}\"")

        # ② 解析来源（只有 b23 短链解析与本地路径检查会 await，都是轻量网络/文件操作）
        source_url = ""; source_type = ""; bvid = bvid or ""

        if bvid:
            bvid = bvid.strip()
            if not bvid.startswith("BV"):
                bvid = (await extract_bvid(bvid)) or ""
            if not bvid:
                return ("⚠️ 没从 bvid 参数里认出有效的 BV 号。"
                        "请传 BV 号（如 BV1xx411c7mD）或完整链接；"
                        "要分析本地文件请用 local_path 参数。")
            source_type = "bilibili"; source_url = f"https://www.bilibili.com/video/{bvid}"
        elif local_path:
            lp = local_path.strip()
            if os.path.isabs(lp) and os.path.isfile(lp): source_url = lp
            else:
                resolved = os.path.join(get_data_path(), lp)
                if os.path.isfile(resolved): source_url = resolved
                else: return f"⚠️ 找不到文件: {local_path}（相对路径以 data/ 为基准）"
            source_type = "local"
        else:
            pend = self._pending.pop(sid, None)
            if pend:
                source_url = pend["url"]; source_type = "onebot"
                m = BVID_RE.search(source_url)
                if m: bvid = m.group(0); source_type = "bilibili"
            else:
                # 兜底：用该会话最近缓存的视频
                # （插件 reload 后 _pending 会被清空，但缓存文件还在）
                cached = self._cached_videos.get(sid) or []
                if cached:
                    latest = cached[-1]
                    if os.path.isfile(latest.get("path", "")):
                        source_url = latest["path"]; source_type = "local"
                if not source_url:
                    olds = self._list_sessions(sid)
                    if olds:
                        cur = olds[0]
                        # 有历史会话但没指视频 → 当作追问（轻量）
                        if question:
                            return await self._submit_or_run(
                                sid, "followup", question=question,
                                model_spec=profile_spec, sess=cur, title=cur.title)
                        return (f"🔁 已有{len(olds)}个历史，最新session_id={cur.session_id}\n"
                                f"🤖 {cur.analysis_model}\n{self._link_line(cur.host_url)}━━━\n"
                                f"{cur.analysis[:300]}\n━━━\n追问用 session_id=\"{cur.session_id}\"")
                    return "当前无视频"

        # ③ 同一视频已有会话
        sess_id = hashlib.md5(source_url.encode()).hexdigest()[:12]
        if sess_id in self._sessions:
            cur = self._sessions[sess_id]
            self._register_session(cur, sid)
            if segs:
                return await self._submit_or_run(
                    sid, "segment", segs=segs, question=question,
                    model_spec=profile_spec, sess=cur,
                    title=cur.title, detail=f"时间段 {self._seg_label(segs)}")
            if question:
                return await self._submit_or_run(
                    sid, "followup", question=question, model_spec=profile_spec,
                    sess=cur, title=cur.title)
            if deep_analysis and cur.analysis_mode == "AI_summary" and not cur.grids_base64:
                return await self._submit_or_run(
                    sid, "first", deep=True, question=question,
                    model_spec=profile_spec, sess_id=sess_id, source_url=source_url,
                    source_type="bilibili", bvid=bvid,
                    title=cur.title, detail="深度补充分析")
            return (f"🔁 已有分析\n📌 session_id={sess_id}\n🤖 {cur.analysis_model}\n"
                    f"{self._link_line(cur.host_url)}━━━\n{cur.analysis[:400]}\n━━━\n"
                    f"追问用 session_id=\"{sess_id}\"")

        # ④ 首次分析（重活）→ 提交后台
        # 标题：B站此刻只有 BV 号（真实标题要等下载后才知道），先给个可读的标识；
        #      通告里会用后台拿到的真实标题覆盖。
        title = ""
        detail = ""
        if source_type == "bilibili":
            title = bvid
            detail = "B站视频"
        else:
            title = os.path.basename(source_url) or "视频"
            # 本地/QQ 视频：尽量用缓存里记录的原始文件名（更可读）
            for c in (self._cached_videos.get(sid) or [])[::-1]:
                if c.get("path") == source_url and c.get("orig_name"):
                    title = str(c["orig_name"]); break
            detail = ""
        return await self._submit_or_run(
            sid, "first", question=question, model_spec=profile_spec,
            sess_id=sess_id, source_url=source_url, source_type=source_type,
            bvid=bvid, deep=deep_analysis, segments=segs,
            title=title, detail=detail)

    @staticmethod
    def _seg_label(segs) -> str:
        if not segs:
            return ""
        if len(segs) == 1:
            return f"{_ts(segs[0][0])}-{_ts(segs[0][1])}"
        return f"{len(segs)} 段"

    async def _submit_or_run(self, sid: str, kind: str, **kw) -> str:
        """决定「同步执行」还是「提交后台」，然后按对应文案返回。

        同步只用于两类轻量场景：
          - 追问（followup）：拼图已在内存，秒级出结果
          - 异步开关关闭时的首次/时间段分析（此时用软预算兜住框架超时）
        """
        sess = kw.pop("sess", None)
        title = kw.pop("title", "") or ""
        detail = kw.pop("detail", "") or ""
        question = kw.pop("question", "") or ""
        # 兼容两种调用方写法（segs= 与 segments=），并确保不残留到 **kw
        segs = kw.pop("segs", None) or kw.pop("segments", None)
        model_spec = kw.pop("model_spec", None)
        # 防御：这些键已由具名参数承接，**绝不能**再随 **kw 传下去
        # （否则会与形参撞名 → TypeError，时间段分析会 100% 失败）
        for _k in ("segs", "segments", "question", "model_spec", "sess", "title", "detail"):
            kw.pop(_k, None)

        run_sync = (kind == "followup" and not self.async_followup) \
            or (kind != "followup" and not self.async_analyze)

        dup_key = self._dup_key_for(kind, kw, sess, segs, question)
        # 重复调用防护：同一会话同一来源（且同一时间段）已有在跑的任务 → 不重复开跑
        dup = self._find_duplicate(sid, dup_key)
        if dup is not None:
            return (f"ℹ️ 该分析已在后台进行中（任务号 {dup.task_id}），"
                    f"完成后会自动把结果交给你。\n"
                    "现在只用一句话告诉用户「已经在看了，稍等一下」，不要重复调用本工具。")

        if run_sync:
            return await self._run_sync(kind, sid, sess=sess, question=question,
                                        segs=segs, model_spec=model_spec, **kw)

        # ── 异步提交 ──
        task = self._submit_task(
            sid, kind, title, detail,
            runner=lambda t: self._execute(t, kind, sess=sess, question=question,
                                           segs=segs, model_spec=model_spec, **kw),
            dup_key=dup_key)
        if task.state == "queued":
            return self._ack_queued(task)
        return self._ack_submitted(task, self._running_count(sid))

    @staticmethod
    def _dup_key_for(kind: str, kw: dict, sess, segs=None, question: str = "") -> str:
        """去重键：同一会话 + 同一来源 + 同一类型（+ 同一时间段 + 同一问题）→ 视为重复调用

        ⚠️ 时间段必须进 key：否则「先看 0~60s、再看 100~160s」会被误判成重复调用，
        第二个请求直接被拦下（用户换个时间段问就永远得不到回答）。
        同理，显式问题也要进 key：「看 0~10s」和「看 0~10s 并回答某问题」是两回事。
        """
        if kind == "followup":
            return ""
        key = kw.get("source_url") or (sess.session_id if sess else "")
        if not key:
            return ""
        parts = [kind, key]
        if kind == "segment":
            seg_tag = ",".join(f"{float(s):.0f}-{float(e):.0f}" for s, e in (segs or []))
            parts.append(seg_tag)
        if question:
            parts.append(question.strip()[:80])
        return ":".join(parts)

    def _find_duplicate(self, sid: str, dup_key: str):
        """同一会话里是否已有等价任务在跑（防止 bot 重复调用导致重复下载/扣费）"""
        if not dup_key:
            return None
        for t in self._tasks.values():
            if t.sid != sid or t.state not in ("queued", "running"):
                continue
            if t._dup_key and t._dup_key == dup_key:
                return t
        return None

    async def _run_sync(self, kind: str, sid: str, sess=None, question: str = "",
                        segs=None, model_spec=None, **kw) -> str:
        """同步执行：用 **本插件自己的软预算** 兜住，与框架工具超时脱钩。

        即使被框架 wait_for 掐断，任务也会转后台并最终发通告（实测有效）。
        """
        task = self._submit_task(
            sid, "followup" if kind == "followup" else kind,
            kw.get("title", "") or (sess.title if sess else ""),
            kw.get("detail", "") or "",
            runner=lambda t: self._execute(t, kind, sess=sess, question=question,
                                           segs=segs, model_spec=model_spec, **kw),
            dup_key="")
        # 同步模式下不让它触发「完成通告」，避免与工具返回重复
        task.notified = True
        state, payload = await self._await_or_handoff(task, self.analysis_budget_sec)
        if state == "handoff":
            # 超预算 / 被框架取消 → 已转后台，恢复通告（结果仍要交回）
            task.notified = False
            if task.state in ("done", "failed", "rejected"):
                await self._notify_done(task)
            return self._ack_handoff(task)
        if state == "done" and payload:
            return payload
        return self._sync_fail_text(payload)

    @staticmethod
    def _sync_fail_text(reason: str) -> str:
        """同步路径失败文案：区分「排队没排上」与「真失败」，别一律说失败"""
        reason = reason or "未知原因"
        if "排队" in reason or "仍未轮到" in reason:
            return (f"🕐 本次没有执行：{reason}\n"
                    "请把情况告诉用户（前面的还在处理），让他稍后再发一次。")
        return f"⚠️ 分析失败：{reason}\n请把原因转述给用户。"

    async def _execute(self, task: VideoTask, kind: str, sess=None, question: str = "",
                       segs=None, model_spec=None, **kw) -> str:
        """真正的执行体（跑在独立 Task 上；重活已在 video_processor 里离开事件循环）。"""
        task.progress = "正在解析视频来源"
        if kind == "followup":
            task.progress = "正在回答追问"
            return await self._followup(sess, question, model_spec)
        if kind == "segment":
            task.progress = "正在按时间段抽帧分析"
            task.session_id = sess.session_id if sess else ""
            return await self._segment_analyze(sess, question, segs, model_spec)

        # first
        sess_id = kw.get("sess_id") or ""
        source_url = kw.get("source_url") or ""
        source_type = kw.get("source_type") or "local"
        bvid = kw.get("bvid") or ""
        deep = bool(kw.get("deep"))

        # 本地已有缓存就直接用（M1：避免同一视频下载两次）
        source_url, source_type, bvid = self._prefer_local(source_url, source_type, bvid,
                                                           task.sid)

        task.progress = "正在获取视频信息"
        if source_type == "bilibili" and self.bili_use_ai and not deep and not segs:
            try:
                info = await get_bili_info(bvid, self.bili_cookie)
                if info.get("title"):
                    task.title = str(info["title"])
                ai = await get_ai_summary(bvid, info["cid"], info.get("up_mid", 0), self.bili_cookie)
                if ai.get("has_summary"):
                    task.session_id = sess_id
                    task.progress = "已完成（B站AI总结）"
                    return self._build_ai_result(sess_id, task.sid, source_url, info, ai)
            except Exception as e:
                logger.info("[VC] B站AI降级: %s", e)
        elif source_type == "bilibili":
            # 即使不走 AI 总结，也先把真实标题拿到，通告里更好读
            try:
                info = await get_bili_info(bvid, self.bili_cookie)
                if info.get("title"):
                    task.title = str(info["title"])
            except Exception:
                pass

        task.progress = "正在下载 / 抽帧 / 转写"
        task.session_id = sess_id
        return await self._vision(sess_id, task.sid, source_type, source_url, bvid,
                                  question or "请完整分析这段视频", segments=segs,
                                  model_spec=model_spec)

    def _prefer_local(self, source_url: str, source_type: str, bvid: str, sid: str):
        """若本地已有该视频缓存，直接复用本地文件（省一次完整下载）。

        M1：原先收视频时缓存到 other_cache_dir，分析时 process_video 又走 URL
        分支下一份 —— 同一个视频下载两次。

        ⚠️ 关键：**只对「非 B站」来源做这个替换**。
        B站来源一旦改成 local，`_vision` 就走不到 `stype == "bilibili"` 分支，
        会连带丢掉 **B站官方字幕（cid 也拿不到）、AI 总结、真实标题**。
        B站的「不重复下载」由 `download_bili_video` 自己处理
        （文件已存在且非空就直接返回，见 bili_dl.py:283），不需要在这里替换。
        """
        try:
            if source_type != "bilibili" and source_url.startswith(("http://", "https://")):
                # 自动缓存过的文件（按原始文件名后缀匹配）
                name = os.path.basename(source_url.split("?")[0]) or ""
                if name:
                    hit = self._find_cached(name)
                    if hit:
                        logger.info("[VC] 复用已缓存的视频: %s", os.path.basename(hit))
                        return hit, "local", bvid
                # 会话最近的缓存里找（同一个视频被反复引用时最有效）
                for c in (self._cached_videos.get(sid) or [])[::-1]:
                    p = c.get("path") or ""
                    if p and os.path.isfile(p) and c.get("orig_name") and \
                            c["orig_name"] in source_url:
                        logger.info("[VC] 复用会话缓存的视频: %s", os.path.basename(p))
                        return p, "local", bvid
        except Exception as e:
            logger.debug("[VC] 本地复用检查失败: %s", e)
        return source_url, source_type, bvid

    # ── 模型组选择（支持按别名/模型名/组号指定） ──

    def _find_profile(self, spec):
        """按「别名 → 模型名 → 组号」找模型组；找不到返回 None。

        让 bot 能听懂「用 Agnes 抽帧分析这个」这类指令。
        spec 既可以是字符串（别名/模型名/组号），也可以已经是 ModelProfile 对象。
        """
        if spec is None:
            return None
        if isinstance(spec, ModelProfile):
            return spec
        s = str(spec).strip()
        if not s:
            return None
        if s.isdigit():
            for p in self._profiles:
                if str(p.group) == s:
                    return p
        low = s.lower()
        for p in self._profiles:                       # 别名精确
            if p.label and p.label.lower() == low:
                return p
        for p in self._profiles:                       # 模型名精确
            if p.name and p.name.lower() == low:
                return p
        for p in self._profiles:                       # 别名包含
            if p.label and low in p.label.lower():
                return p
        for p in self._profiles:                       # 模型名包含
            if p.name and low in p.name.lower():
                return p
        return None

    def _list_profiles_text(self) -> str:
        if not self._profiles:
            return "（没有启用任何模型组）"
        parts = []
        for p in sorted(self._profiles, key=lambda x: x.priority):
            tag = p.label or p.name or f"组{p.group}"
            parts.append(f"{tag}(组{p.group}/{p.mode})")
        return "、".join(parts)

    # ── 时间段参数解析 ──

    def _parse_segments(self, start_sec, end_sec, segments):
        """归一化时间段参数 → ([(s,e), ...] | None, 错误信息)

        segments 优先于 start_sec/end_sec；end_sec=0 表示到视频结尾。
        未指定时返回 (None, "")，由下游按全片处理。
        """
        raw = []
        if segments:
            items = segments
            if isinstance(segments, str):
                # 容错："10-30,100-130" / "10~30；100~130"
                items = []
                for part in re.split(r"[,;，；]", segments):
                    m = re.match(r"\s*([\d.]+)\s*[-~到至]\s*([\d.]+)\s*$", part)
                    if m: items.append((m.group(1), m.group(2)))
            if isinstance(items, (list, tuple)):
                for item in items:
                    try:
                        if isinstance(item, dict):
                            s = item.get("start", item.get("start_sec", item.get("from")))
                            e = item.get("end", item.get("end_sec", item.get("to")))
                        else:
                            s, e = item[0], item[1]
                        raw.append((float(s), float(e)))
                    except Exception:
                        continue
        elif start_sec or end_sec:
            try:
                raw.append((float(start_sec or 0), float(end_sec or 0)))
            except Exception:
                return None, "时间段参数格式不对（应为数字秒）"

        if not raw:
            return None, ""

        norm = []
        for s, e in raw:
            # N1：负数结束时间是笔误，不能悄悄当成「到结尾」
            if e < 0:
                return None, (f"时间段非法：结束时间 {e:.0f} 为负数。"
                              f"要去掉结束限制请传 0，而不是负数")
            s = max(0.0, s)
            if e == 0: e = 1e9        # 到结尾，稍后按视频时长截断
            if e <= s:
                return None, f"时间段非法：{s:.0f}~{e:.0f}（结束必须大于开始）"
            if e < 1e9 and (e - s) > MAX_SEGMENT_SEC:
                return None, (f"单段最长 {MAX_SEGMENT_SEC} 秒，请把 {s:.0f}~{e:.0f} "
                              f"拆成多段传 segments")
            norm.append((s, e))
        if len(norm) > MAX_SEGMENTS:
            return None, f"最多 {MAX_SEGMENTS} 段，当前 {len(norm)} 段"
        return norm, ""

    # ── B站AI总结 ──

    def _build_ai_result(self, sess_id, sid, url, info, ai):
        sess = VideoSession(sess_id, sid, "bilibili", url)
        sess.title = info.get("title", ""); sess.duration = info.get("duration", 0)
        sess.bili_ai_summary = ai; self._register_session(sess, sid)
        s = ai.get("summary", "")
        outline = "\n".join(
            f"  [{_ts(o.get('timestamp',0))}] {o.get('title','')}\n" +
            "\n".join(f"    → [{_ts(p.get('timestamp',0))}] {p.get('content','')}"
                      for p in o.get("part_outline", []))
            for o in ai.get("outline", []))
        result = (f"🎬 {info.get('title','')}\n📌 session_id={sess_id}\n━━━\n"
                  f"⏱ {info.get('duration',0)}s | 🤖 B站AI总结\n━━━\n{s}\n")
        if outline: result += f"\n📑 大纲\n{outline}\n"
        result += f"\n━━━\n💡 追问用 session_id=\"{sess_id}\""
        sess.analysis = result; sess.analysis_model = "B站AI总结"; sess.analysis_mode = "AI_summary"
        return result

    # ── 视觉分析（非B站视频存到 other_cache_dir） ──

    def _reject_hint(self, result: dict) -> str:
        """把「拒绝处理」翻译成带**可操作建议**的文案。

        ⚠️ 这里用上了 `max_duration_auto`：当上限是「自动跟随模型组」时，
        用户看到「时长超上限」会不知道去哪调（他可能压根没设过这个值）。
        明确告诉他「这是自动跟随来的、去哪个模型组调」才有用。
        """
        err = str(result.get("error") or "无法处理")
        hint = ""
        if "时长" in err:
            if self.max_duration_auto:
                tops = sorted({p.max_video_sec for p in self._profiles}, reverse=True)
                hint = (f"\n💡 当前时长上限 {self.max_duration}s 是**自动跟随**"
                        f"（取已启用模型组里最大的 max_video_sec：{tops[:3]}）。"
                        f"想放宽就去「模型组 N → 最大视频时长」调大，"
                        f"或在「安全限制 → 最大视频时长」手填一个更大的值。")
            else:
                hint = (f"\n💡 时长上限 {self.max_duration}s 是你在「安全限制 → "
                        f"最大视频时长」手填的，改那里即可（填 0 = 自动跟随模型组）。")
        elif "过大" in err:
            hint = (f"\n💡 大小上限 {self.max_file_mb}MB 可在"
                    f"「安全限制 → 最大视频文件大小」调整。")
        return f"⚠️ {err}{hint}"

    async def _vision(self, sess_id, sid, stype, surl, bvid, question,
                      segments=None, model_spec=None):
        if not self._profiles: return "未配置模型"

        # 选模型用的时长：指定片段时用片段总长，否则用视频真实时长
        duration_hint = self.max_duration
        if segments:
            duration_hint = sum(e - s for s, e in segments)
        elif stype == "bilibili" and bvid:
            try:
                _info = await get_bili_info(bvid, self.bili_cookie)
                duration_hint = float(_info.get("duration") or 0) or self.max_duration
            except Exception:
                pass
        elif stype == "local" and os.path.isfile(surl):
            try:
                from video_processor import get_video_info_async
                duration_hint = float((await get_video_info_async(surl)).get("duration") or 0) \
                    or self.max_duration
            except Exception:
                pass
        profile = model_spec or select_model(self._profiles, duration_hint, self.default_model)
        if not profile: return "无合适模型"

        # N5：work 目录带会话+随机后缀，避免同会话并发任务（或同一视频被重复分析）
        #     写同一个目录互相踩（原来只用 sess_id 命名）
        _wsuf = uuid.uuid4().hex[:6]
        info = {}          # B 站分支会填上 cid 等（供字幕获取用）
        if stype == "bilibili" and bvid:
            # B站: 源文件存在 bili_cache_dir
            os.makedirs(self.bili_cache_dir, exist_ok=True)
            try:
                # 同一个 BV 号的文件名是固定的（{bvid}.mp4），并发任务若同时下载
                # 会写同一个文件 → 用按 bvid 的锁串行化（不同视频之间仍可并行）
                async with self._session_lock(f"bili:{bvid}"):
                    raw_path, info = await download_bili_video(bvid, self.bili_cache_dir, cookie=self.bili_cookie,
                        timeout=self.dl_timeout, max_seconds=self.max_duration,
                        quality=self.bili_download_quality)
            except Exception as e: return f"⚠️ 下载失败：{e}"
            # 分析工作也放 bili_cache_dir
            work = os.path.join(self.bili_cache_dir, f"analysis_{sess_id}_{_wsuf}")
        else:
            # 非B站（QQ/本地）：存到 other_cache_dir
            os.makedirs(self.other_cache_dir, exist_ok=True)
            raw_path = surl
            work = os.path.join(self.other_cache_dir, f"analysis_{sess_id}_{_wsuf}")
            if stype == "local":
                raw_path = surl

        os.makedirs(work, exist_ok=True)
        # native + 上传模式：小文件不预压缩（保画质）；阈值 0 = 永不压缩（用极大值让所有文件都跳过）
        native_upload = bool(self.upload_enabled and profile.mode == "native")
        under_mb = 0
        if native_upload:
            under_mb = self.upload_compress_over_mb if self.upload_compress_over_mb > 0 else 10 ** 6
        result = await process_video(raw_path, work_dir=work,
            max_file_mb=self.max_file_mb, max_duration_sec=self.max_duration,
            download_timeout=self.dl_timeout,
            target_frames=self.target_frames, scene_threshold=self.scene_threshold,
            max_per_grid=self.max_per_grid, grid_cols=self.grid_cols,
            cell_width=self.cell_width, cell_ratio=self.cell_ratio,
            segments=[[s, e] for s, e in segments] if segments else None,
            skip_compress=bool(segments),
            skip_compress_if_under_mb=under_mb)
        if result["status"] == "rejected":
            return self._reject_hint(result)
        if result["status"] == "error": return f"⚠️ 处理失败：{result['error']}"

        sess = VideoSession(sess_id, sid, stype, surl) if sess_id not in self._sessions else self._sessions[sess_id]
        sess.compressed_path = result.get("compressed_path", "")
        sess.duration = result.get("duration", 0)
        sess.grids_base64 = result.get("grids_base64", [])
        sess.scene_count = result.get("scene_count", 0); sess.total_frames = result.get("total_frames", 0)
        sess.timestamps = result.get("timestamps", [])
        sess.file_size_mb = result.get("file_size_mb", 0); sess.compressed_size_mb = result.get("compressed_size_mb", 0)
        self._register_session(sess, sid)

        real_segs = [(s, e) for s, e in (result.get("segments") or [])]
        # 取转写/字幕（最多等 audio_wait_sec 秒；超时就不带，绝不卡住分析）
        # B 站视频优先用官方字幕，其他视频走音频识别
        tr = await self._get_transcript(raw_path, self.audio_wait_sec,
                                       bvid=bvid if stype == "bilibili" else "",
                                       cid=int((info or {}).get("cid") or 0)
                                       if stype == "bilibili" else 0,
                                       skip_asr=profile.native_audio)
        tdoc = tr.get("doc", "") or ""
        # M5：指定时间段分析时，只带该段字幕（原先会把整片字幕塞进提示词，
        #     既费 token 又容易让模型答非所问）
        if real_segs:
            tdoc = self._clip_transcript(tr, real_segs) or tdoc
        analysis, label, downgrade_note, host_url = await self._analyze_result(
            profile, result, real_segs, question, work, transcript_doc=tdoc,
            bili_bvid=bvid if stype == "bilibili" else "",
            bili_cid=int((info or {}).get("cid") or 0) if stype == "bilibili" else 0)
        sess.analysis = analysis; sess.analysis_model = label; sess.analysis_mode = profile.mode
        sess.model_tag = profile.label or str(profile.group)   # 记住，供追问沿用
        if host_url:
            sess.host_url = host_url
        if tdoc:
            sess.transcript_doc = tdoc

        range_line = self._range_line(real_segs, result.get("duration", 0))
        link_line = self._link_line(host_url)
        return (f"🎬 视频分析完成\n📌 session_id={sess_id}\n━━━\n"
                f"{range_line}"
                f"⏱ {result['duration']:.1f}s | 📐 {result.get('width','?')}×{result.get('height','?')}\n"
                f"📦 {result['file_size_mb']:.1f}MB→{result['compressed_size_mb']:.1f}MB\n"
                f"🖼 {result['total_frames']}帧/{result['grid_count']}张/{result['scene_count']}场景\n"
                f"🤖 {label}\n{link_line}━━━\n{analysis}{downgrade_note}\n━━━\n"
                f"💡 追问用 session_id=\"{sess_id}\"")

    @staticmethod
    def _direct_ttl_text(url: str) -> str:
        """从 B 站直链里解析签名，算出剩余有效期。

        N2：B 站直链的签名参数不止 deadline（还有 w_rid / expired / ts 等变体），
        原先只认 deadline，认不出就笼统说「带签名」，信息量太低。
        这里多认几种常见形式；确实认不出就如实说明「无法判断剩余时间」。
        """
        u = url or ""
        ts = None
        m = re.search(r"[?&]deadline=(\d+)", u)          # 最常见
        if m:
            ts = m.group(1)
        if ts is None:
            m = re.search(r"[?&](?:expired|expires|expire|expire_at|ts)=(\d{9,})", u)
            if m:
                ts = m.group(1)
        if ts is None:
            return "⚠️ 带签名（无法判断剩余时间，建议现取现用）"
        try:
            remain = int(ts) - int(time.time())
        except Exception:
            return "⚠️ 带签名（无法判断剩余时间，建议现取现用）"
        if remain <= 0:
            return "⚠️ 可能已失效"
        if remain >= 3600:
            return f"约 {remain // 3600} 小时后失效"
        if remain >= 60:
            return f"约 {remain // 60} 分钟后失效"
        return f"约 {remain} 秒后失效"

    def _link_line(self, host_url: str) -> str:
        """给 bot 的链接提示。

        - 上传到文件中转的链接：可分享、可长期引用
        - B 站 temporary 直链：**带签名的时效链接**，只能现拉现用；
          访问多少次都无法延长，想要长期分享得改用 send_video（发视频到QQ）。
        """
        if not host_url:
            return ""
        if "bilivideo" in host_url or "deadline=" in host_url:
            return (f"🔗 视频直链(B站临时): {host_url}\n"
                    f"   ⚠️ 带签名，{self._direct_ttl_text(host_url)}；"
                    f"只能现取现用（反复访问**不会**延长有效期）。"
                    f"需要长期分享请改用 send_video 把视频发到QQ，"
                    f"或重新调用 analyze_video 取一条新链\n")
        return f"🔗 视频直链: {host_url}（临时公开链接，可直接分享或后续引用）\n"

    # ── 时间段分析（复用已下载的视频） ──

    def _clip_transcript(self, tr: dict, segs) -> str:
        """只保留指定时间段的转写（时间段分析用，避免整片字幕干扰）"""
        if not tr:
            return ""
        doc = tr.get("doc", "") or ""
        if not doc or not segs:
            return doc
        s0 = min(float(s) for s, e in segs)
        e0 = max(float(e) for s, e in segs)
        subs = [g for g in (tr.get("segments") or [])
                if float(g.get("end") or 0) > s0 and float(g.get("start") or 0) < e0]
        speech = [r for r in (tr.get("speech") or [])
                  if len(r) >= 2 and float(r[1]) > s0 and float(r[0]) < e0]
        if not subs and not speech:
            return ""
        return build_timeline_doc(subs, speech, float(tr.get("total") or 0))

    def _range_line(self, segs, duration: float) -> str:
        """结果头部的分析范围标注"""
        if not segs or (len(segs) == 1 and segs[0][0] <= 0.05 and segs[0][1] >= duration - 0.5):
            return ""
        if len(segs) == 1:
            return f"🔍 分析范围: {_ts(segs[0][0])} - {_ts(segs[0][1])}\n"
        return ("🔍 分析范围: " + str(len(segs)) + " 段 " +
                " ".join(f"[{_ts(s)}-{_ts(e)}]" for s, e in segs) + "\n")

    async def _analyze_result(self, profile, result, segs, question, work,
                              transcript_doc: str = "", bili_bvid: str = "",
                              bili_cid: int = 0):
        """按 模式 + 段数 选择分析路径，返回 (analysis, label, note, host_url)。

        - native + 单段 → 秒切该段片段（含音频）传给模型
        - 其他（frames / native 多段）→ 拼图帧模式
        - native 失败自动降级 frames
        - transcript_doc：语音转写时间轴，两种模式都会带上
        """
        meta = build_meta(result["duration"], result["total_frames"],
                          result["grid_count"], result["scene_count"],
                          result.get("timestamps", []))
        # 转写拼在提示词之后、问题之前：模型既看到画面，也知道"说了什么、什么时候说的"
        ask_prompt = self.default_prompt
        if transcript_doc:
            ask_prompt = f"{self.default_prompt}\n\n{transcript_doc}"
        multi = len(segs) > 1
        src_for_native = result.get("compressed_path") or ""
        # 全片（未指定时间段）还是指定区间？全片直接用压缩后的整段视频，不做秒切
        is_full = (not segs) or (len(segs) == 1
                                 and segs[0][0] <= 0.5
                                 and segs[0][1] >= result.get("duration", 0) - 0.5)

        async def _native_payload():
            """返回 (视频路径, 附加说明)。全片=压缩后的完整视频；单段=秒切片段。"""
            if is_full:
                return src_for_native, ""
            s, e = segs[0]
            clip_path = os.path.join(work, "clip.mp4")
            clip_path, real_dur, actual_start = await clip_video(src_for_native, clip_path, s, e)
            clip_note = (f"\n（秒切对齐关键帧，实际片段约 {_ts(actual_start)} - "
                         f"{_ts(actual_start + real_dur)}）") if abs(actual_start - s) > 0.3 else ""
            return clip_path, clip_note

        async def _upload_or_none(path: str):
            """尝试把视频换成公开链接；未启用/超限/失败都返回 None（回退 base64）"""
            if not self.upload_enabled:
                return None
            try:
                size_mb = os.path.getsize(path) / (1024 * 1024)
                if size_mb > self.upload_max_mb:
                    logger.info("[VC] 文件 %.1fMB 超过上传上限 %dMB，改用 base64",
                                size_mb, self.upload_max_mb)
                    return None
                key = f"{path}:{os.path.getmtime(path):.0f}"
                if key in self._upload_cache:
                    return self._upload_cache[key]
                t0 = time.time()
                url, used_host = await upload_to_any(path, self.upload_hosts,
                                                     timeout=self.upload_timeout,
                                                     keep_name=self.upload_keep_name,
                                                     use_proxy=self.upload_use_proxy)
                self._upload_cache[key] = url
                logger.info("[VC] 视频已上传（%.1fMB, %.1fs, %s）→ %s",
                            size_mb, time.time() - t0, used_host, url)
                return url
            except UploadError as e:
                logger.warning("[VC] 上传失败，回退 base64: %s", e)
                return None
            except Exception as e:
                logger.warning("[VC] 上传异常，回退 base64: %s", e)
                return None

        async def _native_call():
            vpath, clip_note = await _native_payload()
            size_mb = os.path.getsize(vpath) / (1024 * 1024)
            # 超过阈值 → 上传前先压（体积可控）；≤阈值 → 直接传，保住画质
            if self.upload_enabled and self.upload_compress_over_mb > 0 \
                    and size_mb > self.upload_compress_over_mb:
                try:
                    small = os.path.join(work, "upload_small.mp4")
                    t0 = time.time()
                    await compress_video(vpath, small, max_width=720, crf=28)
                    new_mb = os.path.getsize(small) / (1024 * 1024)
                    logger.info("[VC] 上传前压缩 %.1fMB → %.1fMB（%.1fs）",
                                size_mb, new_mb, time.time() - t0)
                    vpath, size_mb = small, new_mb
                except Exception as e:
                    logger.warning("[VC] 上传前压缩失败，按原文件上传: %s", e)
            url = ""
            # ★ B 站视频 + 开启直传 → 直接用 html5 MP4 直链交给模型，省一次上传
            #   仅限全片：时间段分析用的是裁剪片段，没有对应直链
            if self.bili_direct_url and bili_bvid and is_full:
                try:
                    direct, _q, _ms = await get_bili_direct_url(
                        bili_bvid, bili_cid, self.bili_cookie, self.dl_timeout)
                    url = direct
                    logger.info("[VC] B站视频走直链交给模型（免上传）")
                except Exception as e:
                    logger.info("[VC] B站直链不可用，回退上传: %s", e)
            if not url:
                url = await _upload_or_none(vpath)
            if not url and size_mb > NATIVE_MAX_MB:
                # 没上传成功且超过 base64 上限 → 压到能内联
                try:
                    small = os.path.join(work, "native_small.mp4")
                    await compress_video(vpath, small, max_width=720, crf=30)
                    vpath = small
                except Exception as e:
                    logger.warning("[VC] base64 回退压缩失败: %s", e)
            ans = await analyze_native(profile, vpath, question, ask_prompt,
                                       video_url=url)
            is_direct = bool(url) and ("bilivideo" in url or "deadline=" in url)
            tag = "native B站直链" if is_direct else ("native URL" if url else "native base64")
            if not is_full:
                tag += " 片段"
            return ans, clip_note, tag, url

        # native（全片或单段都走；多段走帧模式）
        if profile.mode == "native" and not multi:
            try:
                analysis, clip_note, tag, url = await _native_call()
                return analysis, f"{profile.name} ({tag})", clip_note, url
            except Exception as e:
                logger.warning("[VC] native 模式失败，降级帧模式: %s", e)
                if not result.get("grids_base64"):
                    return f"⚠️ AI分析失败（{type(e).__name__}）", f"{profile.name} (native)", "", None
                try:
                    analysis = await analyze_frames(profile, result["grids_base64"], meta,
                                                    question, ask_prompt)
                    return (analysis, f"{profile.name} (native→frames)",
                            f"\n（原生视频模式失败，已降级为帧模式：{str(e)[:120]}）", None)
                except Exception as e2:
                    logger.error("[VC] LLM失败: %s", e2)
                    return (f"⚠️ AI分析失败（native: {type(e).__name__} / frames: {type(e2).__name__}）",
                            f"{profile.name} (native)", "", None)

        # frames 路径（含 native 多段：一次请求覆盖所有段）
        try:
            analysis = await analyze_frames(profile, result["grids_base64"], meta,
                                            question, ask_prompt)
            label = f"{profile.name} (frames" + (" 多段)" if multi else ")")
            note = "\n（多段分析走帧模式：一次请求覆盖所有段）" if (multi and profile.mode == "native") else ""
            return analysis, label, note, None
        except Exception as e:
            logger.error("[VC] LLM失败: %s", e)
            return f"⚠️ AI分析失败（{type(e).__name__}）", f"{profile.name} (frames)", "", None

    async def _segment_analyze(self, sess, question, segs, model_spec=None):
        """对已有 session 的视频做指定时间段分析（用本地文件，不重新下载）"""
        path = sess.compressed_path
        if not path or not os.path.isfile(path):
            return ("⚠️ 本地视频文件已失效（可能被缓存清理），请重新发送或重新分析该视频")
        if not self._profiles:
            return "未配置模型"

        # 按视频实际时长截断
        real = []
        for s, e in segs:
            e2 = min(e, sess.duration) if sess.duration else e
            if e2 - s >= 0.05:
                real.append((s, e2))
        if not real:
            return (f"⚠️ 时间段超出视频时长（视频共 {sess.duration:.1f}s）")
        total = sum(e - s for s, e in real)
        spec = model_spec or sess.model_tag               # 同样沿用会话的模型
        profile = (self._find_profile(spec) if spec else None) \
            or select_model(self._profiles, total, self.default_model) \
            or self._profiles[0]
        if model_spec:
            sess.model_tag = profile.label or str(profile.group)

        # N5 + M4：work 目录带随机后缀，且同一会话的「同一视频」分析串行化，
        #          避免两个并发时间段分析写同一个目录互相踩。
        work = os.path.join(os.path.dirname(path),
                            f"seg_{int(time.time()*1000) % 10**9}_{uuid.uuid4().hex[:6]}")
        os.makedirs(work, exist_ok=True)
        async with self._session_lock(f"{sess.session_id}:segment"):
            result = await process_video(
                path, work_dir=work,
                max_file_mb=max(self.max_file_mb, 4096),
                max_duration_sec=max(self.max_duration, int(sess.duration) + 1),
                download_timeout=self.dl_timeout,
                target_frames=self.target_frames, scene_threshold=self.scene_threshold,
                max_per_grid=self.max_per_grid, grid_cols=self.grid_cols,
                cell_width=self.cell_width, cell_ratio=self.cell_ratio,
                segments=[[s, e] for s, e in real], skip_compress=True)
        if result["status"] != "ok":
            return f"⚠️ 处理失败：{result.get('error', result['status'])}"

        # 转写：优先用缓存/进行中的任务，超时就不带（不卡住）
        tr = await self._get_transcript(path, self.audio_wait_sec,
                                        skip_asr=profile.native_audio)
        analysis, label, note, host_url = await self._analyze_result(
            profile, result, real, question, work,
            transcript_doc=self._clip_transcript(tr, real))
        sess.add_turn(question or "(时间段分析)", analysis)
        if host_url:
            sess.host_url = host_url
        range_line = self._range_line(real, sess.duration)
        link_line = self._link_line(host_url)
        return (f"🎬 时间段分析完成\n📌 session_id={sess.session_id}\n━━━\n"
                f"{range_line}"
                f"🖼 {result['total_frames']}帧/{result['grid_count']}张\n"
                f"🤖 {label}\n{link_line}━━━\n{analysis}{note}\n━━━\n"
                f"💡 继续追问用 session_id=\"{sess.session_id}\"")

    async def _followup(self, sess, question, model_spec=None):
        if not question:
            return (f"当前 session={sess.session_id}\n{self._link_line(sess.host_url)}"
                    f"{sess.analysis[:300]}\n追问用 session_id=\"{sess.session_id}\"")
        # 模型粘性：追问没指定 model 时，沿用该会话首次分析用的那一组，
        # 而不是退回默认优先级（否则「同一个视频前后换了模型」）
        spec = model_spec or sess.model_tag
        profile = (self._find_profile(spec) if spec else None) \
            or select_model(self._profiles, sess.duration, self.default_model) \
            or (self._profiles[0] if self._profiles else None)
        if not profile: return "无可用模型"
        if model_spec:                                   # 本次显式换了模型 → 更新会话
            sess.model_tag = profile.label or str(profile.group)

        # M3：拼图被 LRU 回收后仍可追问 —— 退化为「基于已有分析文本 + 转写」作答，
        #     而不是直接拒绝（原先会回「只有xx结果，深度分析后可追问画面」）。
        if not sess.grids_base64:
            if not (sess.analysis or sess.transcript_doc):
                return ("⚠️ 该会话的可用资料已过期（拼图与分析都被清理），"
                        "请重新发送视频或重新分析")
            meta = f"【视频文字资料】已有分析结论与语音转写（画面帧已回收，无法再看画面）\n" \
                   f"时长: {sess.duration:.1f}s"
            ctx = (f"之前: {sess.analysis[:1500]}\n\n"
                   f"{sess.transcript_doc[:3000]}\n\n追问: {question}\n\n"
                   f"基于以上文字资料回答；如果问题必须看画面才能答，"
                   f"请明确说明「需要重新分析该视频才能回答」，不要编造画面内容。")
            try:
                ans = await analyze_frames(profile, [], meta, ctx, "")
            except Exception as e:
                ans = f"⚠️ 追问失败: {type(e).__name__}: {e}"
            sess.add_turn(question, ans)
            return (f"🤖 {profile.name} | session={sess.session_id}\n"
                    f"（提示：该会话的画面帧已被回收，本次基于文字资料作答）\n"
                    f"━━━\n{ans}")

        meta = build_meta(sess.duration, sess.total_frames, len(sess.grids_base64),
                          sess.scene_count, sess.timestamps)
        ctx = f"之前: {sess.analysis[:500]}\n\n追问: {question}\n\n基于帧回答指出时间。"
        try:
            ans = await analyze_frames(profile, sess.grids_base64, meta, ctx,
                                       sess.transcript_doc or "")
        except Exception as e: ans = f"⚠️ 追问失败: {type(e).__name__}: {e}"
        sess.add_turn(question, ans)
        return (f"🤖 {profile.name} | session={sess.session_id}\n"
                f"{self._link_line(sess.host_url)}━━━\n{ans}")

    def reload_cfg(self, cfg: dict):
        """热重载配置：只重读配置项，保留会话、缓存任务与已探测状态"""
        try:
            self._load_cfg(cfg)
        except Exception as e:
            logger.error("[VC] 配置热重载失败: %s", e)


def _ts(s: float) -> str:
    return f"{int(s//60):02d}:{s - int(s//60)*60:06.3f}"


# ── NapCat stream 工具函数（模块级，供类内方法调用） ──────────

def _raise_for_stream(resp):
    if not isinstance(resp, dict):
        raise RuntimeError(f"stream response: {resp!r}")
    if resp.get("status") == "ok":
        return
    msg = (str(resp.get("data", {})) if isinstance(resp.get("data"), dict) else str(resp))[:200]
    if "unsupported" in msg.lower() or "not found" in msg.lower() or "unknown action" in msg.lower():
        raise RuntimeError(f"NapCat 不支持 upload_file_stream: {msg}")
    raise RuntimeError(f"stream 上传失败: {msg}")


def _extract_stream_path(resp) -> Optional[str]:
    data = resp.get("data")
    if isinstance(data, dict):
        for k in ("file_path", "path", "file"):
            v = data.get(k)
            if v: return str(v)
    for k in ("file_path", "path", "file"):
        v = resp.get(k)
        if v: return str(v)
    return None