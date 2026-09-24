"""端到端模拟：验证「提交 → 后台分析 → 通告回灌」全链路（不打网络）

用一个假的 ctx + 假的 _vision，检查：
  1. analyze_video 是**毫秒级**返回（L1 真实计时）
  2. 返回文案符合 agnes 风格（告知用户稍候）
  3. 完成后 publish_notice 被调用，且通告里带完整结果 + 引导语
  4. 追问走同步、几秒内出结果、不发通告
  5. 同一视频重复调用 → 不重复开跑
"""
import asyncio
import os
import sys
import time
import tempfile

import os as _os
sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
import selfcheck as sc


class FakeEvent:
    """最小可用的消息事件（_sid 只读 session.sid）"""
    def __init__(self, sid="qq:gm:123456"):
        self.session = type("S", (), {"sid": sid})()
        self.adapter = type("A", (), {"name": "qq", "platform": "QQ"})()
        self.message = type("M", (), {"message_id": "m1", "chain": []})()


async def main():
    tmp = tempfile.mkdtemp(prefix="e2e_")
    inst, main_mod = sc.build_plugin(tmp, {
        "async_analyze": True, "async_followup": False,
        "analysis_budget_sec": 200, "notice_coalesce_sec": 0.1,
        "max_parallel_per_chat": 3,
    })
    notices = []
    inst.ctx.publish_notice = lambda sid, chain, is_mentioned=True: sc._fake_notice(notices, sid, chain)
    # 让插件有「模型组」，否则 _vision 会返回「未配置模型」
    inst._profiles = [type("P", (), {"group": 1, "label": "TestModel", "name": "m",
                                     "mode": "frames", "priority": 1,
                                     "max_video_sec": 600, "native_audio": False})()]

    # 假 _vision：模拟「下载 + 抽帧 + 转写 + 模型」共 0.5 秒
    async def fake_vision(sess_id, sid, stype, surl, bvid, question, segments=None,
                          model_spec=None):
        await asyncio.sleep(0.5)
        inst._sessions[sess_id] = main_mod.VideoSession(sess_id, sid, stype, surl)
        inst._sessions[sess_id].analysis = "【分析正文】开场是新地图实机，05:10 公布新角色"
        inst._sessions[sess_id].analysis_model = "TestModel (frames)"
        return (f"🎬 视频分析完成\n📌 session_id={sess_id}\n━━━\n"
                f"【分析正文】开场是新地图实机，05:10 公布新角色\n━━━\n"
                f"💡 追问用 session_id=\"{sess_id}\"")
    inst._vision = fake_vision

    ev = FakeEvent()

    print("── ① 首次分析：提交延迟 ──")
    t0 = time.time()
    ret = await inst._tool_analyze(ev, bvid="BV1xx411c7mD")
    dt = time.time() - t0
    print(f"  返回耗时 {dt*1000:.1f} ms")
    print("  ---- 返回值 ----")
    print("  " + ret.replace("\n", "\n  "))
    print()
    sc.check("E2E 提交毫秒级返回（<300ms）", dt < 0.3, f"{dt*1000:.1f}ms")
    sc.check("E2E 文案含「正在看，稍等一下」", "正在看，稍等一下" in ret)
    sc.check("E2E 文案禁用猜内容", "不要猜视频内容" in ret)
    sc.check("E2E 文案不含分析正文", "分析正文" not in ret)
    sc.check("E2E 文案含本会话并行数", "本会话并行" in ret)

    print("\n── ② 等待完成 + 通告 ──")
    await asyncio.sleep(1.0)
    sc.check("E2E 完成后发了通告", len(notices) == 1, f"n={len(notices)}")
    body = notices[0][1] if notices else ""
    print("  ---- 通告 ----")
    print("  " + body.replace("\n", "\n  "))
    sc.check("E2E 通告含系统通知头", "【系统通知 · 视频分析完成" in body)
    sc.check("E2E 通告含 session_id", "session_id=" in body)
    sc.check("E2E 通告含完整结果", "分析正文" in body)
    sc.check("E2E 通告含「用你自己的语气」引导", "用你自己的语气" in body)
    sc.check("E2E 通告禁止提系统字眼", "系统通知/后台任务/任务号" in body)

    print("\n── ③ 追问：同步 ──")
    sess_id = list(inst._sessions.keys())[0] if inst._sessions else ""
    # 给会话塞拼图，走「有画面」的追问路径
    if sess_id:
        inst._sessions[sess_id].grids_base64 = ["data:image/jpeg;base64,AAAA"]
        inst._sessions[sess_id].duration = 754.0

    async def fake_frames(profile, grids, meta, ctx, doc):
        await asyncio.sleep(0.05)
        return "那段是在讲 4.3 的卡池安排……"
    import llm_proxy
    llm_proxy.analyze_frames = fake_frames
    inst.__class__.__module__  # noqa
    main_mod.analyze_frames = fake_frames

    t0 = time.time()
    ret2 = await inst._tool_analyze(ev, session_id=sess_id, question="5:10 那段再讲讲")
    dt2 = time.time() - t0
    print(f"  返回耗时 {dt2*1000:.1f} ms")
    print("  ---- 返回值 ----")
    print("  " + ret2.replace("\n", "\n  "))
    sc.check("E2E 追问同步返回（<1s）", dt2 < 1.0, f"{dt2*1000:.1f}ms")
    sc.check("E2E 追问结果直接给出", "卡池安排" in ret2)
    sc.check("E2E 追问不发通告", len(notices) == 1, f"n={len(notices)}")

    print("\n── ④ 重复调用防护 ──")
    ret3 = await inst._tool_analyze(ev, bvid="BV1xx411c7mD")
    print("  " + ret3.replace("\n", "\n  ")[:200])
    sc.check("E2E 已分析过的视频不重复开跑（返回已有分析）",
             "已有分析" in ret3 or "已经在看" in ret3)

    print("\n── ⑤ 排队文案 ──")
    # 占满 3 个槽
    async def slow(task):
        await asyncio.sleep(1.0)
        return "S"
    for i in range(3):
        inst._submit_task("qq:gm:777", "first", f"t{i}", "", slow, dup_key=f"s{i}")
    await asyncio.sleep(0.05)
    ev2 = FakeEvent("qq:gm:777")
    ret4 = await inst._tool_analyze(ev2, local_path="/tmp/definitely_missing_file.mp4")
    # 上面应为「找不到文件」；换一个真实存在的路径来触发排队
    real_file = os.path.join(tmp, "dummy.mp4")
    open(real_file, "wb").write(b"x")
    ret4 = await inst._tool_analyze(ev2, local_path=real_file)
    print("  " + ret4.replace("\n", "\n  "))
    sc.check("E2E 超并发时返回排队文案", "已排队" in ret4 and "排到了就立刻开始" in ret4)

    print("\n── ⑥ 找不到文件 ──")
    ret5 = await inst._tool_analyze(ev, local_path="/tmp/no_such_video_xyz.mp4")
    print("  " + ret5)
    sc.check("E2E 文件不存在时明确报错",
             "找不到文件" in ret5 and "data/ 为基准" in ret5)

    await asyncio.sleep(1.5)
    sc.shutil.rmtree(tmp, ignore_errors=True)
    print(f"\n通过 {len(sc.PASS)} / 失败 {len(sc.FAIL)}")
    for f in sc.FAIL:
        print("  ✗", f)
    return 1 if sc.FAIL else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
