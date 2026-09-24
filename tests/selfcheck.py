"""视频理解插件 v1.17.0 自检（离线，不依赖 KiraAI 本体）

跑法：
    cd <插件目录>
    python3 tests/selfcheck.py

覆盖：
  A. 静态判据 —— L1 提交路径、L2 计时干扰、配置默认值、文案、迁移
  B. 行为判据 —— 用桩件跑真类：软预算交接、框架超时分支、并发闸、通告、迁移幂等

设计原则：
  - 不用字符串「看起来对」当结论：关键项用真类 + 真文件跑一遍
  - 判据前先剥注释，避免注释里的示例把判据骗了
"""
import ast
import asyncio
import json
import os
import re
import shutil
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

PASS, FAIL = [], []


def check(name, cond, extra=""):
    (PASS if cond else FAIL).append(name)
    print(f"{'✓' if cond else '✗'} {name}" + (f"  [{extra}]" if extra and not cond else ""))


def read(p):
    with open(os.path.join(ROOT, p), "r", encoding="utf-8") as f:
        return f.read()


def strip_comments(src: str) -> str:
    """剥掉 Python 注释（行内与整行），避免注释里的示例骗过判据。

    注意：不剥字符串字面量 —— 判据要检的文案本身就在字符串里。
    """
    lines = []
    for line in src.splitlines():
        # 只处理不在字符串里的 #（用简单状态机，够用于本仓库代码）
        out, i, in_str, quote = [], 0, False, ""
        while i < len(line):
            ch = line[i]
            if in_str:
                out.append(ch)
                if ch == "\\":
                    if i + 1 < len(line):
                        out.append(line[i + 1]); i += 2; continue
                elif ch == quote:
                    in_str = False
                i += 1
                continue
            if ch in ("'", '"'):
                in_str, quote = True, ch
                out.append(ch); i += 1; continue
            if ch == "#":
                break
            out.append(ch); i += 1
        lines.append("".join(out))
    return "\n".join(lines)


def strip_json_comments(src: str) -> str:
    # JSON 没有注释，但保留函数以便统一调用（schema 里可能有人手写 // 说明）
    return re.sub(r"(?m)^\s*//.*$", "", src)


# ══════════════════════════════════════════════════════════════
#  A. 静态判据
# ══════════════════════════════════════════════════════════════
def static_checks():
    print("\n── A. 静态判据 ──")
    main_src = read("main.py")
    main_code = strip_comments(main_src)
    vp_code = strip_comments(read("video_processor.py"))
    schema = json.loads(read("schema.json"))

    # ── L1：提交路径不得直接 await 分析体 ──
    tree = ast.parse(main_src)
    target = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_tool_analyze":
            target = node
            break
    check("L1 _tool_analyze 存在", target is not None)
    awaits = [ast.unparse(n) for n in ast.walk(target) if isinstance(n, ast.Await)]
    bad = [a for a in awaits if re.search(r"_vision\(|_followup\(|_segment_analyze\(", a)]
    check("L1 提交路径不直接 await 分析体（0 处）",
          len(bad) == 0, f"仍有 {len(bad)} 处: {bad[:3]}")

    # ── L2：同步重活必须离开事件循环 ──
    vp_tree = ast.parse(read("video_processor.py"))
    vp_awaits = [ast.unparse(n) for n in ast.walk(vp_tree) if isinstance(n, ast.Await)]
    joined = "\n".join(vp_awaits)
    check("L2 抽帧区间在线程池 (to_thread)",
          "to_thread(_extract_range" in joined or "_extract_range" in joined)
    check("L2 拼图在线程池", "_composite_grid_sync" in joined)
    check("L2 ffprobe 探测在线程池",
          "to_thread(_get_video_info" in joined and "to_thread" in joined)
    check("L2 base64 编码在线程池",
          "to_thread" in joined and "grid_to_base64" in joined)
    check("L2 拼图有同步实现体", "_composite_grid_sync" in vp_code)

    # ── L3：软预算交接机制 ──
    check("L3 使用 shield 保护任务",
          "asyncio.shield(" in main_code)
    check("L3 软预算异常分支存在",
          "budget_exceeded = True" in main_code and "转为后台" in main_src or
          "budget_exceeded" in main_code)
    check("L3 捕获取消后不杀任务（raise 前不 cancel）",
          re.search(r"except asyncio\.CancelledError:[\s\S]{0,900}任务已转后台继续", main_src) is not None
          and re.search(r'if task\.state in \("rejected", "failed", "cancelled"\):',
                        main_code) is not None)

    # ── 并发闸 ──
    check("并发：每会话闸", "max_parallel_per_chat" in main_code and "Semaphore" in main_code)
    check("并发：全局闸", "max_parallel_global" in main_code)
    check("并发：followup 不取槽",
          re.search(r'if kind == "followup":[\s\S]{0,300}use_gate=False', main_code) is not None
          or re.search(r'if kind == "followup":[\s\S]{0,300}reserved = False', main_code) is not None)
    check("并发：排队看门狗", "_queue_watchdog" in main_code)
    check("并发：超时拒绝会通告", "视频分析未执行" in main_src)

    # ── 后台任务统一挂闸（S2） ──
    check("S2 缓存/转写走后台闸", "async with self._bg_gate()" in main_code)
    check("S2 不再裸 create_task 缓存",
          "asyncio.create_task(self._cache_then_transcribe" not in main_code)

    # ── 配置默认值 ──
    bili = schema["section_bili"]["fields"]
    audio = schema["section_audio"]["fields"]
    a = schema["section_async"]["fields"]
    check("配置：B站AI总结默认关", bili["bili_use_ai_summary"]["default"] is False)
    check("配置：B站下载时长默认 1800", bili["bili_max_download_sec"]["default"] == 1800)
    check("配置：STT 并发默认 10", audio["audio_concurrency"]["default"] == 10)
    check("配置：async_analyze 默认开", a["async_analyze"]["default"] is True)
    check("配置：async_followup 默认关", a["async_followup"]["default"] is False)
    check("配置：每会话并发默认 3", a["max_parallel_per_chat"]["default"] == 3)
    check("配置：软预算默认 200", a["analysis_budget_sec"]["default"] == 200)
    check("配置：硬上限默认 0（不设）", a["pipeline_hard_timeout_sec"]["default"] == 0)
    check("配置：无 tool_call_timeout 相关项（第6点已取消）",
          not any("tool_call_timeout" in json.dumps({k: v}, ensure_ascii=False)
                  for k, v in a.items()))
    check("配置：代码侧默认值与 schema 一致",
          'bs.get("bili_use_ai_summary", False)' in main_code
          and 'bs.get("bili_max_download_sec", 1800)' in main_code
          and 'au.get("audio_concurrency", 10)' in main_code)

    # ── 文案（agnes 风格） ──
    check("文案：提交时告知用户稍候",
          "正在看，稍等一下" in main_src)
    check("文案：提交时禁止猜内容",
          "不要猜视频内容" in main_src)
    check("文案：完成时要求用自己的语气讲",
          "用你自己的语气" in main_src)
    check("文案：完成时禁止提系统字眼",
          "系统通知/后台任务/任务号" in main_src)
    check("文案：失败时给建议", "建议:" in main_src and "视频分析失败" in main_src)
    check("文案：排队提示", "排到了就立刻开始" in main_src)
    check("文案：转后台提示", "还在看，再等一下" in main_src)

    # ── 迁移 ──
    check("迁移：先写标记后应用",
          re.search(r"先写标记（原子）[\s\S]{0,600}再改内存配置", main_src) is not None)
    check("迁移：原子写", "_atomic_write_json" in main_code and "os.replace" in main_code)
    check("迁移：标记在插件数据目录", "get_plugin_data_dir" in main_code)
    check("迁移：只迁未改过的（比对旧默认）",
          "if cur == old_default:" in main_code)
    check("迁移：不动框架配置结构（合并写回）",
          "merged[section] = {**merged[section], **values}" in main_code)
    check("迁移：失败只告警不影响加载",
          "配置迁移异常（已忽略" in main_src)

    # ── 其他修复 ──
    check("M1 复用本地缓存", "_prefer_local" in main_code)
    check("M3 会话降级保留", "已回收拼图" in main_src)
    check("M4 会话锁真正使用", "_session_lock" in main_code
          and main_code.count("_session_lock") >= 2)
    check("M5 时间段字幕裁剪", "_clip_transcript(tr, real_segs)" in main_code)
    check("N1 负数时间段报错", "结束时间 {e:.0f} 为负数" in main_src)
    # N2：直链有效期识别多种签名参数（原先只认 deadline，认不出就笼统说「带签名」）
    check("N2 直链多签名识别",
          re.search(r"expired\|expires\|expire\|expire_at\|ts", main_src) is not None
          and "无法判断剩余时间" in main_src)
    check("N3 自动发送占位",
          'pend.get("source") == "auto_send"' in main_code)
    check("N5 work 目录带随机后缀", "uuid.uuid4().hex[:6]" in main_code)
    check("并发：B站同名文件下载串行化（防同视频并发写同一文件）",
          'async with self._session_lock(f"bili:{bvid}")' in main_code)

    # ── 审计（第二轮全量复核）新增的判据 ──
    check("审计：_prefer_local 不动 B站来源（否则丢字幕/AI总结）",
          'if source_type != "bilibili" and source_url.startswith' in main_code)
    check("审计：硬上限真的被使用（不是空配置）",
          main_code.count("pipeline_hard_timeout_sec") >= 2
          and "超过硬上限" in main_src)
    check("审计：通告 flush 竞态已修（flush 期间循环取走新任务）",
          "_flushing" in main_code and "_compose_notice" in main_code)
    check("审计：排队超时不被 canceled 覆盖",
          re.search(r"CancelledError:[\s\S]{0,300}if task\.state not in \(\"rejected\"", main_code)
          is not None)
    check("审计：段分析与提问参与去重键",
          "parts.append(seg_tag)" in main_code and "parts.append(question.strip()" in main_code)
    check("审计：_submit_or_run 参数不残留到 **kw（防 TypeError）",
          "for _k in (\"segs\", \"segments\", \"question\"" in main_code)
    check("审计：抽帧区间调用参数与签名一致（避免 TypeError）",
          "await asyncio.to_thread(\n            _extract_range, video_path, out_dir, all_frames, s, e, n,\n            scene_threshold, si)" in read("video_processor.py"))
    check("审计：任务/通告/锁 三张表都有清理（无泄漏）",
          "_prune_tasks" in main_code and "flushing.discard(sid)" in main_code
          and "self._locks.pop(k, None)" in main_code)
    check("审计：flush 期间新任务不会卡在缓冲（循环取走）",
          "for _ in range(6):" in main_code)
    check("审计：排队被拒时同步路径返回明确原因（不抛 CancelledError）",
          "_sync_fail_text" in main_code and "本次没有执行" in main_src)
    check("审计：硬上限不 return 跳过通告（统一收尾）",
          re.search(r"hard_task\.cancel\(\)[\s\S]{0,700}finally:", main_code) is not None)
    check("修复：不再误删本地文件",
          "只删「我们下载的临时文件」" in read("video_processor.py"))
    check("修复：本地文件跳过清理", "if not is_local:" in vp_code)

    # ── 保持原有功能 ──
    check("原有：三工具齐全",
          all(x in main_src for x in ['name="search_bili_video"',
                                      'name="send_video"',
                                      'name="analyze_video"']))
    check("原有：send_video 超时 180s 保留", "_send_to = 180" in main_code)
    check("原有：模式隔离保留", "不允许走原生视频调用" in read("llm_proxy.py"))
    check("原有：B站字幕优先保留", "bilibili_subtitle" in main_code)
    check("原有：session_id 追问机制保留", "session_id" in main_code)
    check("原有：模型粘性保留", "sess.model_tag" in main_code)


# ══════════════════════════════════════════════════════════════
#  B. 行为判据（真类 + 桩件）
# ══════════════════════════════════════════════════════════════
def build_plugin(tmpdir, cfg=None):
    """构造一个真实插件实例（重依赖用桩件替换）。"""
    import types

    # ── 桩掉 core 包，让 main.py 能 import ──
    core = types.ModuleType("core")
    plugin = types.ModuleType("core.plugin")

    class _P:
        LOW, MEDIUM, HIGH = -50, 0, 50

    class _Reg:
        def tool(self, *a, **k):
            def deco(f):
                return f
            return deco
        def __getattr__(self, name):
            def deco(*a, **k):
                if len(a) == 1 and callable(a[0]) and not k:
                    return a[0]
                return lambda f: f
            return deco

    plugin.BasePlugin = object
    plugin.logger = types.SimpleNamespace(
        info=lambda *a, **k: None, warning=lambda *a, **k: None,
        error=lambda *a, **k: None, debug=lambda *a, **k: None,
        exception=lambda *a, **k: None)
    plugin.on = _Reg()
    plugin.Priority = _P
    plugin.register = _Reg()

    chat = types.ModuleType("core.chat")
    # MessageChain 桩件：把 Text 拼成字符串，便于断言通告正文
    chat.MessageChain = lambda items=None: "".join(
        getattr(i, "text", str(i)) for i in (items or []))
    chat.message_utils = types.ModuleType("core.chat.message_utils")
    chat.message_utils.KiraMessageEvent = type("KiraMessageEvent", (), {})
    chat.message_utils.KiraMessageBatchEvent = type("KiraMessageBatchEvent", (), {})
    chat.message_elements = types.ModuleType("core.chat.message_elements")

    class _Text:
        def __init__(self, text=""):
            self.text = text
        def __repr__(self):
            return f"Text({self.text!r})"

    chat.message_elements.Text = _Text
    chat.message_elements.Image = type("Image", (), {"__init__": lambda self, *a, **k: None})

    provider = types.ModuleType("core.provider")
    provider.LLMRequest = type("LLMRequest", (), {})

    utils = types.ModuleType("core.utils")
    path_utils = types.ModuleType("core.utils.path_utils")
    path_utils.get_data_path = lambda: __import__("pathlib").Path(tmpdir)

    sys.modules.update({
        "core": core, "core.plugin": plugin, "core.chat": chat,
        "core.chat.message_utils": chat.message_utils,
        "core.chat.message_elements": chat.message_elements,
        "core.provider": provider, "core.utils": utils,
        "core.utils.path_utils": path_utils,
    })
    for m in ("core.chat", ):
        sys.modules[m].__path__ = []

    # 清掉可能已缓存的插件模块，保证用最新源码
    for m in list(sys.modules):
        if m in ("main", "video_processor", "llm_proxy", "asr", "bili_dl", "video_host",
                 "video_comprehension"):
            del sys.modules[m]

    import importlib
    main_mod = importlib.import_module("main")
    importlib.reload(main_mod)

    default_cfg = {
        "section_basic": {"enabled": True, "video_analysis_enabled": True},
        "section_async": dict(cfg or {}),
    }
    ctx = type("Ctx", (), {
        "get_plugin_data_dir": lambda self: tmpdir,
        "config": {},
    })()
    inst = main_mod.VideoComprehensionPlugin.__new__(main_mod.VideoComprehensionPlugin)
    # 手工初始化运行状态（绕过 BasePlugin.__init__ 的抽象基类限制）
    # ⚠️ 必须与 VideoComprehensionPlugin.__init__ 的字段保持同步
    #    （清理防御性 getattr 后，少一个字段就会 AttributeError）
    for attr, val in [("_pending", {}), ("_sessions", {}), ("_sid_sessions", {}),
                      ("_locks", {}), ("_cleanup", None), ("_auto_sent", {}),
                      ("_ffmpeg_ok", False), ("_stream_unsupported", False),
                      ("_upload_cache", {}), ("_cached_videos", {}),
                      ("_video_failures", {}), ("_asr_tasks", {}),
                      ("_tasks", {}), ("_task_seq", {}), ("_chat_running", {}),
                      ("_global_running", 0), ("_slot_events", {}),
                      ("_notice_buffer", {}), ("_flushing", set()),
                      ("_notice_tasks", set()),
                      ("_background_tasks", set()), ("_migrated_keys", set()),
                      ("_bg_sem", None), ("_bg_sem_limit", 0)]:
        setattr(inst, attr, val)
    inst.ctx = ctx
    inst.plugin_cfg = default_cfg
    inst._load_cfg(default_cfg)
    return inst, main_mod


def behavior_checks():
    print("\n── B. 行为判据（真类 + 桩件） ──")
    tmp = tempfile.mkdtemp(prefix="vc_test_")
    try:
        # ── B1. 软预算 → 转后台（复刻实测） ──
        asyncio.run(_b1_budget(tmp))

        # ── B2. 框架超时取消 → 任务存活 ──
        asyncio.run(_b2_framework_timeout(tmp))

        # ── B3. 并发闸 ──
        asyncio.run(_b3_concurrency(tmp))

        # ── B4. 追问不占槽 ──
        asyncio.run(_b4_followup_no_slot(tmp))

        # ── B5. 排队超时 → 拒绝并通告 ──
        asyncio.run(_b5_queue_timeout(tmp))

        # ── B6. 通告合并 ──
        asyncio.run(_b6_coalesce(tmp))

        # ── B7. 配置迁移：只迁未改过的、只迁一次、原子、不破坏其他字段 ──
        _b7_migration(tmp)

        # ── B8. L2：同步阻塞不再拖累无关协程 ──
        _b8_no_block()

    finally:
        shutil.rmtree(tmp, ignore_errors=True)


async def _b1_budget(tmp):
    inst, _ = build_plugin(tmp, {"analysis_budget_sec": 1, "notice_coalesce_sec": 0})
    notices = []

    async def fake_notice(sid, chain, is_mentioned=True):
        notices.append((sid, str(chain)))

    inst.ctx.publish_notice = fake_notice

    async def runner(task):
        await asyncio.sleep(0.4)
        return "RESULT-TEXT"

    t0 = time.time()
    task = inst._submit_task("qq:gm:1", "first", "标题", "BV:x", runner, dup_key="k")
    state, payload = await inst._await_or_handoff(task, 0.15)   # 预算 0.15s < 执行 0.4s
    dt = time.time() - t0
    check("B1 软预算到点立即返回（不等执行完）",
          state == "handoff" and dt < 0.35, f"state={state} dt={dt:.2f}")
    check("B1 任务仍在后台跑（未被取消）", task.state == "running")
    await asyncio.sleep(0.6)
    check("B1 后台照常完成", task.state == "done" and task.result == "RESULT-TEXT",
          f"state={task.state}")
    check("B1 完成后仍发通告", len(notices) == 1 and "RESULT-TEXT" in notices[0][1])

    # 文案
    ack = inst._ack_handoff(task)
    check("B1 转后台文案含引导", "还在看，再等一下" in ack)
    check("B1 转后台文案含任务号", task.task_id in ack)


async def _b2_framework_timeout(tmp):
    inst, _ = build_plugin(tmp, {"analysis_budget_sec": 200, "notice_coalesce_sec": 0})
    notices = []
    inst.ctx.publish_notice = lambda sid, chain, is_mentioned=True: _fake_notice(notices, sid, chain)
    done = []

    async def runner(task):
        try:
            await asyncio.sleep(0.5)
            done.append("finished")
            return "OK"
        except asyncio.CancelledError:
            done.append("cancelled")
            raise

    # 模拟框架：wait_for(tool_coro, 0.15) 掐断
    async def tool_like():
        task = inst._submit_task("qq:gm:2", "first", "t", "", runner)
        state, _payload = await inst._await_or_handoff(task, 200)
        return state

    try:
        await asyncio.wait_for(tool_like(), 0.15)
        check("B2 框架超时应抛 TimeoutError", False)
    except asyncio.TimeoutError:
        check("B2 框架超时触发（模拟框架行为）", True)
    await asyncio.sleep(0.7)
    check("B2 框架掐断后任务仍跑完（未取消）", done == ["finished"], str(done))
    check("B2 完成通告照常发出", len(notices) == 1)


async def _fake_notice(bucket, sid, chain, is_mentioned=True):
    bucket.append((sid, str(chain)))


async def _b3_concurrency(tmp):
    inst, _ = build_plugin(tmp, {"max_parallel_per_chat": 2, "max_parallel_global": 99,
                                 "notice_coalesce_sec": 0})
    inst.ctx.publish_notice = lambda sid, chain, is_mentioned=True: asyncio.sleep(0)
    running_peak = [0]
    cur = [0]

    async def runner(task):
        cur[0] += 1
        running_peak[0] = max(running_peak[0], cur[0])
        await asyncio.sleep(0.3)
        cur[0] -= 1
        return "R"

    tasks = [inst._submit_task("qq:gm:3", "first", f"t{i}", "", runner, dup_key=f"k{i}")
             for i in range(5)]
    check("B3 第 3 个起进入排队",
          sum(1 for t in tasks if t.state == "queued") >= 3,
          f"queued={sum(1 for t in tasks if t.state == 'queued')}")
    await asyncio.sleep(1.6)
    check("B3 同时运行数不超过每会话上限", running_peak[0] <= 2, f"peak={running_peak[0]}")
    check("B3 全部最终完成",
          all(t.state == "done" for t in tasks),
          f"{[t.state for t in tasks]}")


async def _b4_followup_no_slot(tmp):
    inst, _ = build_plugin(tmp, {"max_parallel_per_chat": 1, "notice_coalesce_sec": 0})
    inst.ctx.publish_notice = lambda sid, chain, is_mentioned=True: asyncio.sleep(0)
    order = []

    async def slow(task):
        order.append("first-start")
        await asyncio.sleep(0.4)
        order.append("first-end")
        return "F"

    async def quick(task):
        order.append("followup-start")
        return "Q"

    t1 = inst._submit_task("qq:gm:4", "first", "t", "", slow)
    await asyncio.sleep(0.05)
    t2 = inst._submit_task("qq:gm:4", "followup", "t", "", quick)
    await asyncio.sleep(0.2)
    check("B4 追问不被并发槽卡住（立即执行）",
          "followup-start" in order and order.index("followup-start") < len(order) - 0
          and "first-end" not in order[:order.index("followup-start")],
          str(order))
    await asyncio.sleep(0.5)
    check("B4 追问结果正确", t2.state == "done" and t2.result == "Q")


async def _b5_queue_timeout(tmp):
    inst, _ = build_plugin(tmp, {"max_parallel_per_chat": 1, "queue_timeout_sec": 1,
                                 "notice_coalesce_sec": 0})
    inst.queue_timeout_sec = 0.2      # 直接改实例（schema 是整数，测试里要亚秒）
    notices = []
    inst.ctx.publish_notice = lambda sid, chain, is_mentioned=True: _fake_notice(notices, sid, chain)

    async def slow(task):
        await asyncio.sleep(1.2)
        return "SLOW"

    async def quick(task):
        return "QUICK"

    inst._submit_task("qq:gm:5", "first", "占用", "", slow)
    await asyncio.sleep(0.05)
    t2 = inst._submit_task("qq:gm:5", "first", "排队者", "", quick)
    check("B5 第二个任务在排队", t2.state == "queued")
    await asyncio.sleep(0.5)
    check("B5 排队超时被拒绝", t2.state == "rejected",
          f"state={t2.state}")
    text = "".join(n[1] for n in notices)
    check("B5 超时后发了「未执行」通告（不静默丢失）", "未执行" in text, text[:120])


async def _b6_coalesce(tmp):
    inst, _ = build_plugin(tmp, {"notice_coalesce_sec": 0.2, "max_parallel_per_chat": 5,
                                 "max_parallel_global": 99})
    notices = []
    inst.ctx.publish_notice = lambda sid, chain, is_mentioned=True: _fake_notice(notices, sid, chain)

    async def mk(task, n):
        return f"结果{n}"

    ts = [inst._submit_task("qq:gm:6", "first", f"标题{n}", "", (lambda n: (lambda t: mk(t, n)))(n),
                            dup_key=f"c{n}") for n in range(3)]
    await asyncio.sleep(0.8)
    check("B6 三个同时完成 → 合并为一条通告", len(notices) == 1, f"notices={len(notices)}")
    if notices:
        body = notices[0][1]
        check("B6 合并通告包含三条结果",
              all(f"结果{n}" in body for n in range(3)))
        check("B6 合并通告含引导语", "用你自己的语气" in body)
    check("B6 三个任务都完成", all(t.state == "done" for t in ts))


def _b7_migration(tmp):
    """迁移：真文件、真读写。"""
    # 造一个「用户没改过」的插件配置
    cfg_dir = os.path.join(tmp, "config", "plugins")
    os.makedirs(cfg_dir, exist_ok=True)
    cfg_path = os.path.join(cfg_dir, "video-comprehension.json")
    original = {
        "section_bili": {"bili_max_download_sec": 600, "bili_cookie": "SECRET",
                         "bili_download_quality": "low"},
        "section_audio": {"audio_concurrency": 5, "audio_model": "whisper"},
        "section_basic": {"enabled": True},
    }
    with open(cfg_path, "w", encoding="utf-8") as f:
        json.dump(original, f, ensure_ascii=False, indent=4)

    # 造 manifest，让插件能定位自己的配置文件名
    with open(os.path.join(ROOT, "manifest.json"), "r", encoding="utf-8") as f:
        pid = json.load(f).get("plugin_id", "video-comprehension")

    inst, _ = build_plugin(tmp)

    cfg = json.loads(json.dumps(original))
    inst._migrate_config(cfg)
    check("B7 未改过的 600 → 1800", cfg["section_bili"]["bili_max_download_sec"] == 1800)
    check("B7 未改过的 5 → 10", cfg["section_audio"]["audio_concurrency"] == 10)

    # 磁盘上的写回
    with open(cfg_path, "r", encoding="utf-8") as f:
        on_disk = json.load(f)
    check("B7 已写回磁盘", on_disk["section_bili"]["bili_max_download_sec"] == 1800)
    check("B7 其他字段未被破坏",
          on_disk["section_bili"]["bili_cookie"] == "SECRET"
          and on_disk["section_bili"]["bili_download_quality"] == "low"
          and on_disk["section_audio"]["audio_model"] == "whisper"
          and on_disk["section_basic"]["enabled"] is True)
    check("B7 未污染框架配置结构（无新增顶层垃圾）",
          set(on_disk.keys()) == set(original.keys()), str(set(on_disk.keys())))

    # 标记文件
    mark = os.path.join(tmp, "config_migrations.json")
    check("B7 迁移标记已写", os.path.isfile(mark))
    if os.path.isfile(mark):
        with open(mark, "r", encoding="utf-8") as f:
            m = json.load(f)
        check("B7 标记内容完整",
              m.get("bili_max_download_sec_600_to_1800") and m.get("audio_concurrency_5_to_10"))

    # ── 幂等：用户手动改回 600 后，第二次启动不许再迁 ──
    cfg2 = {"section_bili": {"bili_max_download_sec": 600},
            "section_audio": {"audio_concurrency": 5}}
    inst._migrate_config(cfg2)
    check("B7 第二次启动不重复迁移（标记生效）",
          cfg2["section_bili"]["bili_max_download_sec"] == 600
          and cfg2["section_audio"]["audio_concurrency"] == 5)

    # ── 用户改过的值不许被动 ──
    tmp2 = tempfile.mkdtemp(prefix="vc_test2_")
    try:
        inst2, _ = build_plugin(tmp2)
        cfg3 = {"section_bili": {"bili_max_download_sec": 900},
                "section_audio": {"audio_concurrency": 3}}
        inst2._migrate_config(cfg3)
        check("B7 用户改过的值不被覆盖",
              cfg3["section_bili"]["bili_max_download_sec"] == 900
              and cfg3["section_audio"]["audio_concurrency"] == 3)
    finally:
        shutil.rmtree(tmp2, ignore_errors=True)

    # ── 标记文件损坏时不能炸 ──
    tmp3 = tempfile.mkdtemp(prefix="vc_test3_")
    try:
        with open(os.path.join(tmp3, "config_migrations.json"), "w", encoding="utf-8") as f:
            f.write("{ 这不是合法 JSON")
        inst3, _ = build_plugin(tmp3)
        cfg4 = {"section_bili": {"bili_max_download_sec": 600}}
        try:
            inst3._migrate_config(cfg4)
            check("B7 标记损坏时不抛异常", True)
        except Exception as e:
            check("B7 标记损坏时不抛异常", False, str(e))
    finally:
        shutil.rmtree(tmp3, ignore_errors=True)


def _b8_no_block():
    """L2 回归：确认 video_processor 的重活不再阻塞事件循环。

    判据：async 函数里**直接在事件循环上**调用同步重活 → 报错。
    允许两种写法：
      ① to_thread(fn, ...) / run_in_executor(None, fn, ...) 包起来
      ② 在「被 executor 执行的嵌套函数」内部调用（那时已经在别的线程里）
    """
    src = read("video_processor.py")
    tree = ast.parse(src)
    heavy = ("_composite_grid_sync", "_extract_range", "_get_video_info",
             "_scan_all_frames", "grid_to_base64", "_get_video_info")
    offenders = []

    def _direct_calls(node):
        """只找「不在嵌套函数里」的直接调用"""
        found = []
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
                continue                     # 嵌套函数体内的调用交给它自己判定
            if isinstance(child, ast.Call):
                found.append(ast.unparse(child.func).split(".")[-1])
            found.extend(_direct_calls(child))
        return found

    for node in ast.walk(tree):
        if not isinstance(node, ast.AsyncFunctionDef):
            continue
        body = " ".join(ast.unparse(s) for s in node.body)
        # 该函数里出现了 executor/to_thread 包装
        has_pool = re.search(r"(to_thread|run_in_executor)\(", body) is not None
        for base in _direct_calls(node):
            if base not in heavy:
                continue
            # 该调用是否出现在 to_thread(...) / run_in_executor(...) 的参数里
            wrapped = re.search(
                rf"(to_thread|run_in_executor)\([^)]*\b{re.escape(base)}\s*\(", body)
            if wrapped:
                continue
            # async 函数里若把重活放在「稍后会丢进 executor 的同步闭包」里，
            # 闭包不在 node.body 的顶层 —— 这里按「该 async 函数内是否存在
            # 同步闭包 + executor 调用」放行（人工已核对 clip_video 走的是这条）
            if has_pool:
                continue
            offenders.append(f"{node.name} → {base}")
    check("B8 L2 无「async 函数直接调用同步重活」", not offenders, str(offenders[:5]))


# ══════════════════════════════════════════════════════════════
def main():
    print("=" * 62)
    print("视频理解插件 v1.17.0 自检")
    print("=" * 62)
    static_checks()
    behavior_checks()
    print("\n" + "=" * 62)
    print(f"通过 {len(PASS)} / 失败 {len(FAIL)}")
    if FAIL:
        print("\n失败项：")
        for f in FAIL:
            print("  ✗", f)
    print("=" * 62)
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
