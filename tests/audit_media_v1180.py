"""v1.18.0 真实媒体、HTTP、进程与 B 站回归（不访问外网）。"""
import asyncio
import http.server
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import types

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from process_utils import run_subprocess
import video_processor as vp
import bili_dl
import asr
import video_host
import llm_proxy

PASS, FAIL = [], []
def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(("✓" if cond else "✗"), name, ("" if cond else f" <-- {detail}"))


def make_video(path: Path, seconds=2):
    subprocess.run([
        "ffmpeg", "-y", "-v", "error",
        "-f", "lavfi", "-i", "testsrc=size=96x96:rate=6",
        "-f", "lavfi", "-i", "sine=frequency=880:sample_rate=16000",
        "-t", str(seconds), "-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac", str(path),
    ], check=True, timeout=30)
    return path


def pid_alive(pid):
    if os.name != "posix":
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


async def process_checks(root):
    r = await run_subprocess([sys.executable, "-c", "print('ok')"], timeout=5)
    check("异步子进程保留 stdout/returncode", r.returncode == 0 and r.stdout.strip() == "ok")
    for mode in ("cancel", "timeout"):
        pid_file = root / f"{mode}.pid"
        code = f"import os,time;open(r'{pid_file}','w').write(str(os.getpid()));time.sleep(30)"
        task = asyncio.create_task(run_subprocess([sys.executable, "-c", code], timeout=30 if mode == "cancel" else 0.2))
        for _ in range(100):
            if pid_file.exists():
                break
            await asyncio.sleep(0.02)
        check(f"{mode} 测试进程已启动", pid_file.exists())
        started = time.monotonic()
        if mode == "cancel":
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            outcome = task.cancelled()
        else:
            try:
                await task
                outcome = False
            except asyncio.TimeoutError:
                outcome = True
        elapsed = time.monotonic() - started
        pid = int(pid_file.read_text())
        for _ in range(50):
            if not pid_alive(pid):
                break
            await asyncio.sleep(0.02)
        check(f"{mode} 快速结束且非僵尸", outcome and elapsed < 3 and not pid_alive(pid),
              f"elapsed={elapsed:.2f} alive={pid_alive(pid)}")


async def media_checks(root):
    src = make_video(root / "input.mp4")
    tmp = root / "tmp"
    tmp.mkdir()
    original = tempfile.mkdtemp
    def isolated(prefix=""):
        return original(prefix=prefix, dir=str(tmp))
    vp.tempfile.mkdtemp = isolated
    heartbeat_stop = False
    delays = []
    async def heartbeat():
        loop = asyncio.get_running_loop()
        last = loop.time()
        while not heartbeat_stop:
            await asyncio.sleep(0.02)
            now = loop.time()
            delays.append(now - last)
            last = now
    hb = asyncio.create_task(heartbeat())
    try:
        result = await vp.process_video(
            str(src), work_dir=str(root / "work"), max_file_mb=20, max_duration_sec=60,
            target_frames=8, max_per_grid=8, grid_cols=4, cell_width=96,
            skip_compress=True,
        )
    finally:
        heartbeat_stop = True
        await hb
        vp.tempfile.mkdtemp = original
    check("真实媒体处理不长时间阻塞事件循环", max(delays or [0]) < 0.25,
          f"heartbeat_max={max(delays or [0]):.3f}s")
    print(f"  实测 heartbeat_max={max(delays or [0]):.3f}s")
    check("真实视频流水线成功", result.get("status") == "ok" and result.get("total_frames") == 6,
          json.dumps(result, ensure_ascii=False)[:300])
    check("成功后 vcf 临时目录为 0", not list(tmp.glob("vcf_*")))
    check("真实拼图内容保留", len(result.get("grids_base64") or []) == 1)
    original_scan = vp._scan_all_frames
    async def empty_scan(path):
        return []
    vp._scan_all_frames = empty_scan
    try:
        fallback = await vp.extract_frames(str(src), target_frames=6,
                                           out_dir=str(root / "fallback_frames"))
    finally:
        vp._scan_all_frames = original_scan
    check("ffprobe 扫描失败也抽真实帧而非灰色占位", len(fallback) > 0
          and all(f.get("path") and Path(f["path"]).is_file() for f in fallback))
    clip = await vp.clip_video(str(src), str(root / "clip.mp4"), 0.2, 1.2)
    check("异步裁剪返回有效片段", Path(clip[0]).is_file() and clip[1] > 0 and clip[2] >= 0)
    wav = await vp.extract_audio(str(src), str(root / "audio.wav"))
    speech, total = await vp.detect_speech_ranges(wav)
    check("音轨与静音检测有效", total > 0 and isinstance(speech, list))
    block = await vp.slice_audio(wav, str(root / "block.wav"), 0, min(0.5, total))
    check("音频切块有效", Path(block).is_file() and Path(block).stat().st_size > 0)
    compressed = await vp.compress_video(str(src), str(root / "compressed.mp4"), max_width=64, crf=30)
    check("压缩原子输出有效且无 .part", Path(compressed).is_file()
          and not Path(compressed + ".part.mp4").exists())
    check("有效缓存通过 ffprobe 校验", await bili_dl._valid_cached_video(compressed))
    partial = root / "partial.mp4"
    partial.write_bytes(b"partial")
    check("半截缓存不能通过完整性校验", not await bili_dl._valid_cached_video(partial))


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass
    def _send(self, status, body, content_type="application/octet-stream"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def do_GET(self):
        if self.path == "/ok":
            self._send(200, b"abc" * 32768)
        elif self.path == "/large":
            self._send(200, b"x" * 65536)
        elif self.path == "/slow":
            self.send_response(200)
            self.end_headers()
            try:
                for _ in range(30):
                    self.wfile.write(b"x" * 1024)
                    self.wfile.flush()
                    time.sleep(0.05)
            except (BrokenPipeError, ConnectionResetError):
                pass
        else:
            self._send(500, b"error")
    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length)
        if self.path == "/v1/audio/transcriptions":
            assert b"file" in body and b"model" in body
            self._send(200, json.dumps({"text": "hello", "segments": [
                {"start": 0, "end": 1, "text": "hello"}]}).encode(), "application/json")
        elif self.path == "/":
            self._send(200, b"https://cdn.test/video.mp4", "text/plain")
        else:
            self._send(500, b"error")


async def http_checks(root):
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        dest = root / "download.mp4"
        await vp.download_video(base + "/ok", str(dest), timeout=5)
        check("HTTP 下载成功且原子落盘", dest.stat().st_size == 32768 * 3 and not Path(str(dest) + ".part").exists())
        dest.write_bytes(b"GOOD")
        try:
            await vp.download_video(base + "/large", str(dest), timeout=5, max_bytes=10)
            too_large = False
        except vp.DownloadTooLarge:
            too_large = True
        check("超限只删 .part 且保留旧好文件", too_large and dest.read_bytes() == b"GOOD"
              and not Path(str(dest) + ".part").exists())
        try:
            await vp.download_video(base + "/slow", str(dest), timeout=0.2)
            timed_out = False
        except asyncio.TimeoutError:
            timed_out = True
        check("HTTP 总超时关闭连接且不留半截", timed_out and dest.read_bytes() == b"GOOD"
              and not Path(str(dest) + ".part").exists())
        wav = root / "asr.wav"
        wav.write_bytes(b"wav")
        tr = await asr.transcribe(str(wav), base + "/v1", "key", "model", use_proxy=True)
        check("ASR 异步 multipart 与时间轴兼容", tr.get("text") == "hello" and len(tr.get("segments")) == 1)
        url = await video_host.upload_file(str(wav), base, use_proxy=True)
        check("上传异步 multipart 返回 URL", url == "https://cdn.test/video.mp4")
        req = bili_dl._Requester()
        bdest = root / "bili.mp4"
        await req.stream(base + "/ok", bdest)
        check("B站流式下载原子落盘", bdest.stat().st_size == 32768 * 3 and not Path(str(bdest) + ".part").exists())
        bdest.write_bytes(b"OLD")
        try:
            await req.stream(base + "/error", bdest)
            failed = False
        except bili_dl.BiliError:
            failed = True
        check("B站失败保留旧文件且无 .part", failed and bdest.read_bytes() == b"OLD"
              and not Path(str(bdest) + ".part").exists())
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


async def bili_checks(root):
    src = make_video(root / "bili_valid.mp4", seconds=1)
    class Api:
        async def get_json(self, url, params=None):
            return {"code": 0, "data": {"dash": {"video": [
                {"height": 1080, "baseUrl": "HIGH", "bandwidth": 100},
                {"height": 720, "baseUrl": "MEDIUM", "bandwidth": 80},
                {"height": 360, "baseUrl": "LOW", "bandwidth": 50}], "audio": []}}}
    high = await bili_dl._get_video_urls(Api(), "BVx", 1, quality_hint=0)
    mid = await bili_dl._get_video_urls(Api(), "BVx", 1, quality_hint=720)
    low = await bili_dl._get_video_urls(Api(), "BVx", 1, quality_hint=360)
    check("DASH original/medium/low 各选正确档", high["video_url"] == "HIGH"
          and mid["video_url"] == "MEDIUM" and low["video_url"] == "LOW")

    calls = []
    class FakeRequester:
        def __init__(self, *args, **kwargs):
            pass
        async def get_json(self, url, params=None):
            return await Api().get_json(url, params)
        async def stream(self, url, fpath):
            calls.append(url)
            await asyncio.to_thread(shutil.copyfile, src, fpath)
    original_requester = bili_dl._Requester
    original_direct = bili_dl.get_bili_direct_url
    bili_dl._Requester = FakeRequester
    try:
        for quality, expected_url, filename in [
            ("original", "HIGH", "BVx_original.mp4"),
            ("medium", "MEDIUM", "BVx_medium.mp4"),
        ]:
            out = root / quality
            out.mkdir()
            path, _ = await bili_dl.download_bili_video(
                "BVx", out, info={"duration": 1, "cid": 1, "title": "x"},
                quality=quality, max_seconds=60)
            check(f"{quality} 使用独立缓存与正确档", Path(path).name == filename
                  and expected_url in calls and await bili_dl._valid_cached_video(path))
        direct_calls = []
        async def fake_direct(*args, **kwargs):
            direct_calls.append(1)
            return "LOW_HTML5", 32, 0
        bili_dl.get_bili_direct_url = fake_direct
        out = root / "low"
        out.mkdir()
        path, _ = await bili_dl.download_bili_video(
            "BVx", out, info={"duration": 1, "cid": 1, "title": "x"},
            quality="low", max_seconds=60)
        check("low 保留 html5 免合并优先与旧文件名", direct_calls and Path(path).name == "BVx.mp4"
              and "LOW_HTML5" in calls)
    finally:
        bili_dl._Requester = original_requester
        bili_dl.get_bili_direct_url = original_direct


async def client_checks():
    instances = []
    class FakeOpenAI:
        def __init__(self, *args, **kwargs):
            self.closed = 0
            self.chat = types.SimpleNamespace(
                completions=types.SimpleNamespace(create=self._create))
            instances.append(self)
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            self.closed += 1
        async def _create(self, **kwargs):
            return types.SimpleNamespace(choices=[types.SimpleNamespace(
                message=types.SimpleNamespace(content="OK"))])
    original = llm_proxy.AsyncOpenAI
    llm_proxy.AsyncOpenAI = FakeOpenAI
    try:
        frames = llm_proxy.ModelProfile(1, True, "m", "https://x", "k", "frames", 1, 600)
        native = llm_proxy.ModelProfile(1, True, "m", "https://x", "k", "native", 1, 600)
        a = await llm_proxy.analyze_frames(frames, [], "meta", "q", "")
        b = await llm_proxy.analyze_native(native, "unused", "q", "", video_url="https://v")
        check("frames/native 调用结果不变", a == b == "OK")
        check("两个 OpenAI 客户端均显式关闭", len(instances) == 2 and all(x.closed == 1 for x in instances))
        try:
            await llm_proxy.analyze_native(frames, "unused", "q", "", video_url="https://v")
            blocked = False
        except RuntimeError:
            blocked = True
        check("frames 模式硬隔离仍生效", blocked)
    finally:
        llm_proxy.AsyncOpenAI = original


async def main():
    root = Path(tempfile.mkdtemp(prefix="vc_media_v1180_"))
    try:
        await process_checks(root)
        await media_checks(root)
        await http_checks(root)
        await bili_checks(root)
        await client_checks()
    finally:
        shutil.rmtree(root, ignore_errors=True)
    print("\n" + "=" * 60)
    print(f"通过 {len(PASS)} / 失败 {len(FAIL)}")
    for name in FAIL:
        print("  ✗", name)
    return 1 if FAIL else 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
