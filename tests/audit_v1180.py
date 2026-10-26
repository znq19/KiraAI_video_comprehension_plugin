"""v1.18.0 可靠性回归：任务、缓存、通知、Prompt、字幕、并发与清理。"""
import asyncio
import io
import os
from pathlib import Path
import shutil
import sys
import tempfile
import time
import zipfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import selfcheck as sc

PASS, FAIL = [], []
def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(("✓" if cond else "✗"), name, ("" if cond else f" <-- {detail}"))

async def task_and_notice(root):
    inst, mm = sc.build_plugin(str(root), {"notice_coalesce_sec": 0, "max_parallel_global": 10})
    notices = []
    inst.ctx.publish_notice = lambda sid, chain, is_mentioned=True: sc._fake_notice(notices, sid, chain)
    gate = asyncio.Event()
    async def runner(task):
        await gate.wait()
        return task.sid
    a = inst._submit_task("qq:gm:A", "first", "A", "", runner, dup_key="same")
    inst._submit_task("qq:gm:B", "first", "B", "", runner, dup_key="same")
    check("跨会话同短号仍保留两个独立任务", len(inst._tasks) == 2)
    check("跨会话任务去重记录不被覆盖", inst._find_duplicate(a.sid, "same") is a)
    gate.set()
    await asyncio.sleep(0.4)
    async def quick_many(task):
        await asyncio.sleep(0.02)
        return task.sid
    many = [inst._submit_task(f"qq:gm:S{i}", "first", f"t{j}", "", quick_many,
                              dup_key=f"{i}:{j}")
            for i in range(20) for j in range(5)]
    await asyncio.sleep(2)
    check("20 会话 × 5 任务无覆盖且全部完成", len(many) == 100
          and all(t.state == "done" for t in many) and inst._global_running == 0,
          f"states={ {s: sum(t.state == s for t in many) for s in {t.state for t in many}} } global={inst._global_running}")

    # 同步外层取消：必须恢复结果通告。
    async def fake_execute(task, *args, **kwargs):
        await asyncio.sleep(0.2)
        return "SYNC-RESULT"
    inst._execute = fake_execute
    try:
        await asyncio.wait_for(inst._run_sync("first", "qq:gm:sync", sess_id="x",
                                             source_url="x", source_type="local"), 0.03)
    except asyncio.TimeoutError:
        pass
    task = next(t for t in inst._tasks.values() if t.sid == "qq:gm:sync")
    await asyncio.sleep(0.5)
    sync_notices = [n for n in notices if n[0] == "qq:gm:sync"]
    check("同步取消后 notified 恢复", not task.notified or sync_notices)
    check("同步取消后结果仍交付", task.state == "done" and len(sync_notices) == 1
          and "SYNC-RESULT" in sync_notices[0][1])

    # 发布失败：保留 → 重试成功，不重复交付。
    inst2, _ = sc.build_plugin(str(root / "retry"), {"notice_coalesce_sec": 0})
    calls, delivered = [], []
    async def flaky(sid, chain, is_mentioned=True):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("temporary")
        delivered.append(str(chain))
    inst2.ctx.publish_notice = flaky
    async def quick(task):
        return "IMPORTANT"
    inst2._submit_task("q", "first", "t", "", quick)
    await asyncio.sleep(0.2)
    check("发布失败结果回到缓冲", len(inst2._notice_buffer.get("q") or []) == 1)
    check("发布失败已安排退避重试", any(not x.done() and "retry" in x.get_name()
                                      for x in inst2._notice_tasks),
          f"retries={inst2._notice_retries} tasks={[(x.get_name(), x.done()) for x in inst2._notice_tasks]}")
    await inst2._flush_notices("q")
    check("重试成功且只交付一次", len(delivered) == 1 and "IMPORTANT" in delivered[0])
    check("重试成功后缓冲与计数清理", not inst2._notice_buffer.get("q") and "q" not in inst2._notice_retries)
    await inst2.terminate()
    await inst.terminate()

async def business_and_cache(root):
    inst, mm = sc.build_plugin(str(root), {"notice_coalesce_sec": 0})
    notices = []
    inst.ctx.publish_notice = lambda sid, chain, is_mentioned=True: sc._fake_notice(notices, sid, chain)
    inst._profiles = []
    async def runner(task):
        return await inst._execute(task, "first", sess_id="empty", source_url="/not/read",
                                   source_type="local")
    task = inst._submit_task("q", "first", "t", "", runner)
    await asyncio.sleep(0.3)
    body = "".join(n[1] for n in notices)
    check("未配置模型进入 failed", task.state == "failed")
    check("业务失败不显示分析完成", "视频分析失败" in body and "视频分析完成" not in body)
    check("失败不留下空视频会话", "empty" not in inst._sessions)
    profile = type("P", (), {"mode": "frames", "name": "fake", "label": "fake"})()
    try:
        await inst._analyze_result(profile, {
            "duration": 1, "total_frames": 0, "grid_count": 0,
            "scene_count": 0, "timestamps": [], "grids_base64": [],
        }, [], "q", str(root))
        no_frame_failed = False
    except mm.VideoAnalysisError:
        no_frame_failed = True
    check("零有效画面不会调用模型假分析", no_frame_failed)
    native_profile = type("P", (), {"mode": "native", "name": "native", "label": "native"})()
    async def fail_native(*args, **kwargs):
        raise RuntimeError("native unsupported")
    async def ok_frames(*args, **kwargs):
        return "FALLBACK-OK"
    mm.analyze_native = fail_native
    mm.analyze_frames = ok_frames
    inst.upload_enabled = False
    inst.bili_direct_url = False
    analysis, label, note, _ = await inst._analyze_result(native_profile, {
        "duration": 1, "total_frames": 1, "grid_count": 1,
        "scene_count": 0, "timestamps": [0], "grids_base64": ["data:x"],
        "compressed_path": "missing.mp4",
    }, [], "q", str(root))
    check("native 失败仍按原设计降级 frames", analysis == "FALLBACK-OK"
          and "native→frames" in label and "原生视频模式失败" in note)
    await inst.terminate()

    inst2, mm = sc.build_plugin(str(root / "cache"), {})
    cache = root / "cache" / "store"
    cache.mkdir(parents=True, exist_ok=True)
    inst2.other_cache_dir = str(cache)
    (cache / "v1_video.mp4").write_bytes(b"VIDEO-A-LEGACY")
    calls = []
    async def fake_download(url, dest, **kw):
        calls.append(url)
        await asyncio.sleep(0.05)
        Path(dest).parent.mkdir(parents=True, exist_ok=True)
        Path(dest).write_bytes(url.encode())
        return dest
    mm.download_video = fake_download
    u1 = "https://cdn.example/a/video.mp4?sig=1"
    u2 = "https://cdn.example/b/video.mp4?sig=2"
    p1, p1_dup, p2 = await asyncio.gather(
        inst2._cache_incoming_video("s", u1, "video.mp4"),
        inst2._cache_incoming_video("s", u1, "video.mp4"),
        inst2._cache_incoming_video("s", u2, "video.mp4"),
    )
    check("同名不同 URL 不再错用旧缓存", p1 and p2 and p1 != p2
          and Path(p1).read_bytes() == u1.encode() and Path(p2).read_bytes() == u2.encode())
    check("相同 URL 并发只下载一次", calls.count(u1) == 1 and calls.count(u2) == 1)
    again = await inst2._cache_incoming_video("s", u1, "video.mp4")
    check("相同 URL 后续仍命中缓存", again == p1 and calls.count(u1) == 1)
    await inst2.terminate()

async def prompt_subtitle_gate(root):
    inst, mm = sc.build_plugin(str(root), {"max_parallel_per_chat": 1})
    sid = "qq:gm:prompt"
    event = type("E", (), {"session": type("S", (), {"sid": sid})(), "messages": []})()
    class Req:
        def __init__(self):
            self.system_prompt = [mm.Prompt("ROLE", name="role"), mm.Prompt("TOOLS", name="tools")]
            self.user_prompt = [mm.Prompt("MESSAGE", name="message")]
    inst._cached_videos[sid] = [{"orig_name": "a.mp4", "path": "/a", "rel": "files/a.mp4", "ts": 1}]
    req = Req()
    before = [p.content for p in req.system_prompt]
    await inst._inject(event, req)
    dynamic = [p for p in req.user_prompt if p.name == "video_comprehension"]
    check("动态提示不改变系统前缀", [p.content for p in req.system_prompt] == before)
    check("动态提示在 user 尾部且不持久化", len(dynamic) == 1 and not dynamic[0].persist
          and "files/a.mp4" in dynamic[0].content)
    check("同一请求不重复注入动态提示", sum(p.name == "video_comprehension" for p in req.user_prompt) == 1)
    from asr import build_timeline_doc
    tr = {"segments": [{"start": 5, "end": 6, "text": "FIRST"},
                       {"start": 50, "end": 51, "text": "MIDDLE"},
                       {"start": 105, "end": 106, "text": "LAST"}],
          "speech": [[0, 120]], "total": 120,
          "doc": "【B站官方字幕】\n[00:05.000] FIRST\n[00:50.000] MIDDLE\n[01:45.000] LAST"}
    clipped = inst._clip_transcript(tr, [(0, 10), (100, 110)])
    check("多段字幕不含段间内容", "FIRST" in clipped and "LAST" in clipped and "MIDDLE" not in clipped)
    check("裁剪保留字幕来源标题", clipped.startswith("【B站官方字幕】"))
    tr2 = {"segments": [{"start": 50, "end": 51, "text": "OUTSIDE"}],
           "speech": [], "total": 60,
           "doc": build_timeline_doc([{"start": 50, "end": 51, "text": "OUTSIDE"}], [], 60)}
    check("目标段无字幕时保持为空", inst._clip_transcript(tr2, [(0, 10)]) == "")
    segs, err = inst._parse_segments(0, 0, "not-a-range")
    check("非法 segments 明确报错而不退化全片", segs is None and "无法解析" in err)
    check("合法 segments 行为不变", inst._parse_segments(0, 0, [[0, 10], [100, 110]])[0] == [(0, 10), (100, 110)])

    inst.audio_enabled = True
    inst.audio_base_url = "https://asr.invalid/v1"
    inst.audio_model = "fake"
    inst.bili_use_subtitle = False
    paths = []
    for i in range(6):
        p = root / f"gate{i}.mp4"
        p.write_bytes(str(i).encode())
        paths.append(str(p))
    async def fake_cache(sid, url, name=""):
        return url
    inst._cache_incoming_video = fake_cache
    release = asyncio.Event()
    active = peak = 0
    async def fake_build(*args, **kwargs):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        try:
            await release.wait()
            return {}
        finally:
            active -= 1
    inst._build_transcript = fake_build
    outer = [inst._spawn(inst._cache_then_transcribe("s", p, "video.mp4")) for p in paths]
    await asyncio.sleep(0.3)
    check("后台 ASR 并发不超过闸上限", peak <= inst._bg_limit(), f"peak={peak}")
    check("转写未完成时缓存任务仍持闸", sum(t.done() for t in outer) < len(outer))
    release.set()
    await asyncio.gather(*outer, return_exceptions=True)
    await inst.terminate()

async def cleanup_and_low(root):
    inst, mm = sc.build_plugin(str(root), {})
    inst.other_cache_dir = str(root / "other")
    Path(inst.other_cache_dir).mkdir()
    video = root / "source.mp4"
    video.write_bytes(b"fake")
    inst.audio_enabled = True
    inst.audio_base_url = "https://asr.invalid/v1"
    inst.audio_model = "fake"
    inst.bili_use_subtitle = False
    async def fake_extract(src, dst, **kw):
        Path(dst).write_bytes(b"wav")
        return dst
    async def fake_detect(*args, **kwargs):
        return [(0.0, 1.0)], 1.0
    async def fake_asr(*args, **kwargs):
        return {"text": "ok", "segments": [{"start": 0, "end": 1, "text": "ok"}], "source": "segments"}
    mm.extract_audio, mm.detect_speech_ranges, mm.asr_transcribe = fake_extract, fake_detect, fake_asr
    data = await inst._build_transcript(str(video))
    key = inst._transcript_key(str(video))
    work = Path(inst.other_cache_dir) / f".asr_{key}"
    check("转写成功保留 JSON 且删除中间目录", bool(data.get("doc")) and not work.exists()
          and (Path(inst.other_cache_dir) / ".transcripts" / f"{key}.json").is_file())
    # 崩溃遗留与逐文件缓存扫描。
    old = time.time() - 3 * 86400
    (Path(inst.other_cache_dir) / ".asr_dead").mkdir()
    (Path(inst.other_cache_dir) / ".asr_dead" / "audio.wav").write_bytes(b"x")
    (Path(inst.other_cache_dir) / ".ffmpeg").mkdir()
    (Path(inst.other_cache_dir) / ".ffmpeg" / "ffmpeg").write_bytes(b"x")
    for p in [Path(inst.other_cache_dir) / ".asr_dead", Path(inst.other_cache_dir) / ".transcripts"]:
        os.utime(p, (old, old))
    os.utime(Path(inst.other_cache_dir) / ".transcripts" / f"{key}.json", (old, old))
    doomed = inst._scan_cache(inst.other_cache_dir, 1, 30, "其他", ttl_hours=1, max_total_mb=0)
    check("遗留 .asr 目录纳入清理", ".asr_dead" in doomed)
    check("转写 JSON 按单文件纳入清理", os.path.join(".transcripts", f"{key}.json") in doomed)
    check(".ffmpeg 始终豁免", ".ffmpeg" not in doomed)
    inst._asr_tasks["dead"] = asyncio.create_task(asyncio.sleep(10))
    protected = inst._active_cache_entries()
    check("活动 ASR 中间目录受保护", ".asr_dead" in protected)
    check("活动 ASR JSON 受保护", os.path.join(".transcripts", "dead.json") in protected)
    protected_scan = inst._scan_cache(inst.other_cache_dir, 1, 30, "其他",
                                      ttl_hours=1, max_total_mb=0,
                                      protected_names=protected)
    check("活动任务不会进入实际删除候选", ".asr_dead" not in protected_scan
          and os.path.join(".transcripts", "dead.json") not in protected_scan)
    inst._asr_tasks["dead"].cancel()
    try:
        await inst._asr_tasks["dead"]
    except asyncio.CancelledError:
        pass
    inst._asr_tasks.pop("dead", None)
    await inst._do_cleanup(inst.other_cache_dir, 1, 30, "其他",
                           ttl_hours=1, max_total_mb=0)
    check("清理真实删除遗留 ASR 与过期 JSON", not (Path(inst.other_cache_dir) / ".asr_dead").exists()
          and not (Path(inst.other_cache_dir) / ".transcripts" / f"{key}.json").exists())
    check("清理保留 .ffmpeg 与 transcripts 容器", (Path(inst.other_cache_dir) / ".ffmpeg").is_dir()
          and (Path(inst.other_cache_dir) / ".transcripts").is_dir())
    # B站条数语义不变。
    bili = root / "bili"
    bili.mkdir()
    for i in range(101):
        p = bili / f"BV{i:012d}.mp4"
        p.write_bytes(b"x")
        os.utime(p, (old + i, old + i))
    check("B站仍只按条数删最旧", inst._scan_cache(str(bili), 100, 20, "B站") == ["BV000000000000.mp4"])
    # 历史只限内存，不改模型输入。
    sess = mm.VideoSession("s", "q", "local", "x")
    for i in range(50):
        sess.add_turn(f"q{i}", f"a{i}")
    check("会话历史限制为最近 40 轮", len(sess.history) == 80 and sess.history[0]["text"] == "q10")
    await inst.terminate()

async def compression_and_ffmpeg(root):
    inst, mm = sc.build_plugin(str(root), {})
    src = root / "src.mp4"
    src.write_bytes(b"source")
    p1 = inst._compression_cache_path(str(root), "send", str(src), 720, 28)
    p2 = inst._compression_cache_path(str(root), "send", str(src), 720, 28)
    p3 = inst._compression_cache_path(str(root), "send", str(src), 360, 32)
    check("同源同参数压缩键稳定", p1 == p2)
    check("不同压缩参数不混用", p1 != p3)
    calls = []
    async def fake_info(path):
        return {"duration": 1}
    async def fake_compress(src, out, **kw):
        calls.append(1)
        await asyncio.sleep(0.05)
        Path(out).write_bytes(b"compressed")
        return out
    mm.get_video_info_async = fake_info
    mm.compress_video = fake_compress
    a, b = await asyncio.gather(inst._compress_cached(str(src), p1, 720, 28),
                                inst._compress_cached(str(src), p1, 720, 28))
    again = await inst._compress_cached(str(src), p1, 720, 28)
    check("同键并发只压缩一次且后续复用", a == b == again == p1 and len(calls) == 1)
    # 静态包只提取两个可执行文件，且不响应路径穿越项。
    blob = io.BytesIO()
    with zipfile.ZipFile(blob, "w") as z:
        z.writestr("nested/ffmpeg.exe", b"ffmpeg")
        z.writestr("nested/ffprobe.exe", b"ffprobe")
        z.writestr("../../evil.txt", b"evil")
    out = root / "static"
    out.mkdir()
    ok = mm._extract_static_ffmpeg_windows(blob.getvalue(), str(out))
    check("静态包双组件完整才成功", ok and (out / "ffmpeg.exe").is_file() and (out / "ffprobe.exe").is_file())
    check("静态包不写包内路径", not (root / "evil.txt").exists() and not (out / "nested").exists())
    # 只有 ffmpeg、没有 ffprobe 时不得提前报成功。
    import httpx
    original_which = mm.shutil.which
    original_client = httpx.AsyncClient
    class FailClient:
        def __init__(self, *args, **kwargs):
            pass
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            return None
        async def get(self, *args, **kwargs):
            return type("R", (), {"status_code": 500, "content": b""})()
    mm.shutil.which = lambda name: "/usr/bin/ffmpeg" if name == "ffmpeg" else None
    httpx.AsyncClient = FailClient
    inst._ffmpeg_ok = False
    inst.bili_cache_dir = str(root / "ffmpeg_missing")
    try:
        available = await inst._ensure_ffmpeg_async()
    finally:
        mm.shutil.which = original_which
        httpx.AsyncClient = original_client
    check("缺 ffprobe 不再假报安装成功", not available and not inst._ffmpeg_ok)
    await inst.terminate()

async def main():
    root = Path(tempfile.mkdtemp(prefix="vc_v1180_"))
    try:
        for name in ["tasks", "retry", "business", "cache", "prompt", "cleanup", "compress"]:
            (root / name).mkdir(exist_ok=True)
        await task_and_notice(root / "tasks")
        await business_and_cache(root / "business")
        await prompt_subtitle_gate(root / "prompt")
        await cleanup_and_low(root / "cleanup")
        await compression_and_ffmpeg(root / "compress")
    finally:
        shutil.rmtree(root, ignore_errors=True)
    print("\n" + "=" * 60)
    print(f"通过 {len(PASS)} / 失败 {len(FAIL)}")
    for name in FAIL:
        print("  ✗", name)
    return 1 if FAIL else 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
