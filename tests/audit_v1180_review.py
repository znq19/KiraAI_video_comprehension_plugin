"""第二轮独立复审：上一轮未覆盖的边界与取消竞态。"""
import asyncio
import os
from pathlib import Path
import shutil
import sys
import tempfile
import types

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import selfcheck as sc
from process_utils import run_subprocess
import video_processor as vp

PASS, FAIL = [], []
def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(("✓" if cond else "✗"), name, ("" if cond else f" <-- {detail}"))


def pid_alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


async def time_and_locks(root):
    inst, mm = sc.build_plugin(str(root), {})
    video = root / "time.mp4"
    video.write_bytes(b"AA")
    sec = 1_700_000_000_000_000_000
    os.utime(video, ns=(sec, sec))
    first = inst._transcript_key(str(video))
    os.utime(video, ns=(sec, sec + 500_000_000))
    second = inst._transcript_key(str(video))
    check("同秒不同 mtime_ns 不复用错误转写", first != second, f"{first} == {second}")
    legacy = inst._legacy_transcript_key(str(video))
    inst._save_transcript(legacy, {"doc": "OLD-CACHE", "segments": []})
    migrated = inst._load_video_transcript(str(video), second)
    check("旧秒级转写缓存命中并迁移而非重跑", migrated.get("doc") == "OLD-CACHE"
          and os.path.isfile(inst._transcript_path(second)))

    lock = inst._session_lock("target")
    for i in range(511):
        inst._session_lock(f"k{i}")
    await lock.acquire()
    entered = asyncio.Event()
    async def waiter():
        async with lock:
            entered.set()
    pending = asyncio.create_task(waiter())
    await asyncio.sleep(0.05)
    lock.release()
    inst._session_lock("new")  # 超过 512，触发锁表清理
    same = inst._session_lock("target")
    check("锁表清理不删除仍有等待者的锁", same is lock,
          f"same={same is lock} locks={len(inst._locks)}")
    await asyncio.wait_for(pending, 1)
    check("等待者最终仍能进入原锁", entered.is_set())
    await inst.terminate()


async def multi_video(root):
    inst, mm = sc.build_plugin(str(root), {})
    inst.cache_scope = "all"
    inst.auto_cache_video = True
    inst.enabled = True
    inst.audio_enabled = False
    calls = []
    async def fake_cache(sid, url, name=""):
        calls.append((url, name))
    inst._cache_then_transcribe = fake_cache
    class Video:
        def __init__(self, file, name):
            self.file, self.name = file, name
    chain = [Video("https://cdn/1.mp4", "1.mp4"), Video("https://cdn/2.mp4", "2.mp4")]
    message = types.SimpleNamespace(
        chain=chain, self_id="bot", sender=types.SimpleNamespace(user_id="user"),
        raw_message={}, message_id="m1")
    event = types.SimpleNamespace(
        session=types.SimpleNamespace(sid="qq:gm:multi"),
        adapter=types.SimpleNamespace(name="qq", platform="QQ"), message=message)
    await inst._detect(event)
    await asyncio.sleep(0.1)
    check("cache_scope=all 缓存一条消息里的全部视频", calls == [
        ("https://cdn/1.mp4", "1.mp4"), ("https://cdn/2.mp4", "2.mp4")],
        f"calls={calls}")
    calls.clear()
    message.chain = []
    message.raw_message = {"message": [
        {"type": "video", "data": {"url": "https://cdn/3.mp4", "name": "3.mp4"}},
        {"type": "video", "data": {"url": "https://cdn/4.mp4", "name": "4.mp4"}},
    ]}
    await inst._detect(event)
    await asyncio.sleep(0.1)
    check("raw_message 多视频段也全部缓存", calls == [
        ("https://cdn/3.mp4", "3.mp4"), ("https://cdn/4.mp4", "4.mp4")],
        f"calls={calls}")
    await inst.terminate()


async def followup_session(root):
    inst, mm = sc.build_plugin(str(root), {})
    sess = mm.VideoSession("sess1", "q", "local", "x")
    release = asyncio.Event()
    async def fake_followup(*args, **kwargs):
        await release.wait()
        return "OK"
    inst._followup = fake_followup
    task = mm.VideoTask("V1", "q", "followup")
    running = asyncio.create_task(inst._execute(task, "followup", sess=sess, question="q"))
    await asyncio.sleep(0.05)
    check("活动追问 session_id 被记录以防过期清理", task.session_id == "sess1",
          f"session_id={task.session_id!r}")
    release.set()
    await running
    await inst.terminate()


async def clip_cancel(root):
    dst = root / "clip.mp4"
    dst.write_bytes(b"OLD")
    original = vp.run_subprocess
    async def fake_run(cmd, timeout):
        Path(cmd[-1]).write_bytes(b"PARTIAL")
        await asyncio.Event().wait()
    vp.run_subprocess = fake_run
    task = asyncio.create_task(vp.clip_video("source", str(dst), 0, 1))
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    finally:
        vp.run_subprocess = original
    check("裁剪取消不覆盖旧文件且删除临时输出", dst.read_bytes() == b"OLD"
          and not Path(str(dst) + ".part.mp4").exists(),
          f"dst={dst.read_bytes()!r}")


async def hard_timeout(root):
    inst, mm = sc.build_plugin(str(root), {"pipeline_hard_timeout_sec": 1,
                                           "notice_coalesce_sec": 0})
    notices = []
    inst.ctx.publish_notice = lambda sid, chain, is_mentioned=True: sc._fake_notice(notices, sid, chain)
    pid_file = root / "hard.pid"
    code = f"import os,time;open(r'{pid_file}','w').write(str(os.getpid()));time.sleep(30)"
    async def runner(task):
        return await run_subprocess([sys.executable, "-c", code], timeout=30)
    task = inst._submit_task("q", "first", "t", "", runner)
    for _ in range(100):
        if pid_file.exists():
            break
        await asyncio.sleep(0.02)
    await asyncio.sleep(2.5)
    pid = int(pid_file.read_text())
    check("完整硬超时任务进入 failed", task.state == "failed", task.state)
    check("完整硬超时杀死底层进程", not pid_alive(pid), f"pid={pid} alive={pid_alive(pid)}")
    check("硬超时发送失败通知而非完成", any("视频分析失败" in n[1] for n in notices))
    await inst.terminate()


async def main():
    root = Path(tempfile.mkdtemp(prefix="vc_second_review_"))
    try:
        await time_and_locks(root)
        await multi_video(root)
        await followup_session(root)
        await clip_cancel(root)
        await hard_timeout(root)
    finally:
        shutil.rmtree(root, ignore_errors=True)
    print("\n" + "=" * 60)
    print(f"通过 {len(PASS)} / 失败 {len(FAIL)}")
    for name in FAIL:
        print("  ✗", name)
    return 1 if FAIL else 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
