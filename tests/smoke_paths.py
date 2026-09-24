"""全参数组合冒烟：确保每条调用路径都不会因参数问题抛异常"""
import asyncio
import os
import sys
import tempfile
import traceback

import os as _os
sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
import selfcheck as sc

FAILED = []


class E:
    def __init__(self, sid="qq:gm:1"):
        self.session = type("S", (), {"sid": sid})()
        self.adapter = type("A", (), {"name": "qq", "platform": "QQ"})()
        self.message = type("M", (), {"message_id": "m", "chain": []})()


async def main():
    tmp = tempfile.mkdtemp(prefix="smoke_")
    inst, mm = sc.build_plugin(tmp, {"max_parallel_per_chat": 3, "notice_coalesce_sec": 0,
                                     "async_analyze": True, "async_followup": False})
    inst.ctx.publish_notice = lambda sid, chain, is_mentioned=True: asyncio.sleep(0)
    inst._profiles = [type("P", (), {"group": 1, "label": "T", "name": "t", "mode": "frames",
                                     "priority": 1, "max_video_sec": 600,
                                     "native_audio": False})()]
    inst.bili_use_ai = False
    inst.bili_cache_dir = os.path.join(tmp, "bili")
    inst.other_cache_dir = os.path.join(tmp, "other")
    os.makedirs(inst.bili_cache_dir, exist_ok=True)
    os.makedirs(inst.other_cache_dir, exist_ok=True)

    async def fake_vision(sess_id, sid, stype, surl, bvid, question, segments=None,
                          model_spec=None):
        await asyncio.sleep(0.05)
        sess = mm.VideoSession(sess_id, sid, stype, surl)
        sess.analysis = "结果"
        sess.analysis_model = "T"
        sess.duration = 100.0
        inst._register_session(sess, sid)
        return f"🎬 ok\n📌 session_id={sess_id}\n━━━\n结果\n━━━\n💡 追问"

    async def fake_seg(sess, question, segs, model_spec=None):
        await asyncio.sleep(0.05)
        sess.add_turn(question or "", "段结果")
        return f"🎬 时间段分析完成\n📌 session_id={sess.session_id}\n━━━\n段结果"

    async def fake_follow(sess, question, model_spec=None):
        await asyncio.sleep(0.05)
        sess.add_turn(question or "", "追问结果")
        return f"🤖 T | session={sess.session_id}\n━━━\n追问结果"

    inst._vision = fake_vision
    inst._segment_analyze = fake_seg
    inst._followup = fake_follow

    ev = E("qq:gm:X")
    local = os.path.join(tmp, "v.mp4")
    open(local, "wb").write(b"x" * 100)

    cases = [
        ("首次: local_path 异步", dict(local_path=local)),
        ("首次: bvid 异步", dict(bvid="BV1GJ411x7h7")),
        ("首次: bvid + question", dict(bvid="BV1GJ411x7h7", question="讲了啥")),
        ("bvid 无效", dict(bvid="not-a-bv")),
        ("local_path 不存在", dict(local_path="/tmp/xx_missing.mp4")),
        ("end_sec 负数", dict(local_path=local, start_sec=0, end_sec=-5)),
        ("单段超长", dict(local_path=local, start_sec=0, end_sec=999)),
        ("段数超限", dict(local_path=local,
                        segments=[[0, 10], [20, 30], [40, 50], [60, 70], [80, 90], [100, 110]])),
        ("model 不存在", dict(local_path=local, model="不存在模型")),
        ("session_id 不存在", dict(session_id="deadbeef")),
    ]

    segs = [None, [(0.0, 10.0)], [(0.0, 10.0), (20.0, 30.0)]]

    for name, kw in cases:
        try:
            r = await asyncio.wait_for(inst._tool_analyze(ev, **kw), timeout=3)
            ok = isinstance(r, str) and len(r) > 0
            print(("✓" if ok else "✗"), name, "->", repr(r[:70]))
            if not ok:
                FAILED.append(name)
        except Exception as e:
            print("✗ EXC", name, "->", type(e).__name__, e)
            traceback.print_exc()
            FAILED.append(f"{name} ({type(e).__name__}: {e})")

    await asyncio.sleep(0.5)

    # 有 session 后的各种组合
    sess_ids = list(inst._sessions.keys())
    print("\n已有 session:", sess_ids)
    if sess_ids:
        sid0 = sess_ids[0]
        inst._sessions[sid0].grids_base64 = ["data:x"]
        for name, kw in [
            ("追问 同步", dict(session_id=sid0, question="再讲讲")),
            ("追问 无 question", dict(session_id=sid0)),
            ("时间段 单段", dict(session_id=sid0, start_sec=0, end_sec=10)),
            ("时间段 多段", dict(session_id=sid0, segments=[[0, 10], [20, 30]])),
            ("时间段+追问", dict(session_id=sid0, question="这段", start_sec=0, end_sec=10)),
        ]:
            try:
                r = await asyncio.wait_for(inst._tool_analyze(ev, **kw), timeout=5)
                ok = isinstance(r, str) and len(r) > 0
                print(("✓" if ok else "✗"), name, "->", repr(r[:70]))
                if not ok:
                    FAILED.append(name)
            except Exception as e:
                print("✗ EXC", name, "->", type(e).__name__, e)
                traceback.print_exc()
                FAILED.append(f"{name} ({type(e).__name__}: {e})")

    # 同步模式（关掉异步）
    print("\n--- async_analyze=False（同步路径 + 软预算兜底）---")
    inst.async_analyze = False
    inst.analysis_budget_sec = 1
    try:
        r = await asyncio.wait_for(inst._tool_analyze(ev, local_path=local), timeout=5)
        print("✓ 同步首次 ->", repr(r[:70]))
    except Exception as e:
        print("✗ EXC 同步首次 ->", type(e).__name__, e)
        traceback.print_exc()
        FAILED.append(f"同步首次 ({type(e).__name__}: {e})")

    await asyncio.sleep(0.3)
    sc.shutil.rmtree(tmp, ignore_errors=True)
    print("\n" + "=" * 55)
    if FAILED:
        print(f"失败 {len(FAILED)} 项：")
        for f in FAILED:
            print("  ✗", f)
    else:
        print("全部通过")
    print("=" * 55)


asyncio.run(main())
