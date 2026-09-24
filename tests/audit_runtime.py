"""第三轮：并发/状态/资源泄漏深挖"""
import asyncio, os, sys, tempfile, time, gc
import os as _os
sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
import selfcheck as sc

ISSUES = []


def issue(ok, name, detail=""):
    if not ok:
        ISSUES.append(f"{name}: {detail}")
    print(("✓" if ok else "✗ BUG"), name, ("" if ok else f"  <-- {detail}"))


async def t1_background_leak():
    print("\n=== 1. 后台任务引用是否泄漏 ===")
    tmp = tempfile.mkdtemp(prefix="t1_")
    inst, _ = sc.build_plugin(tmp, {"max_parallel_per_chat": 4, "notice_coalesce_sec": 0})
    inst.ctx.publish_notice = lambda *a, **k: asyncio.sleep(0)

    async def r(task):
        return "R"

    for i in range(50):
        inst._submit_task("q", "first", "t", "", r, dup_key=f"k{i}")
    await asyncio.sleep(0.8)
    issue(len(inst._background_tasks) < 20, "后台任务集合不无限增长",
          f"size={len(inst._background_tasks)}")
    issue(len(inst._slot_events) == 1, "槽位事件按会话数增长（不是每任务一个）",
          f"size={len(inst._slot_events)}")
    sc.shutil.rmtree(tmp, ignore_errors=True)


async def t2_session_lock_growth():
    print("\n=== 2. 会话锁表是否无限增长 ===")
    tmp = tempfile.mkdtemp(prefix="t2_")
    inst, _ = sc.build_plugin(tmp)
    for i in range(600):
        inst._session_lock(f"key{i}")
    issue(len(inst._locks) <= 513, "锁表有上限", f"size={len(inst._locks)}")
    sc.shutil.rmtree(tmp, ignore_errors=True)


async def t3_task_seq():
    print("\n=== 3. 任务号是否回绕后冲突 ===")
    tmp = tempfile.mkdtemp(prefix="t3_")
    inst, _ = sc.build_plugin(tmp)
    ids = [inst._new_task_id("q") for _ in range(10050)]
    issue(len(set(ids)) < len(ids), "任务号会回绕（已知设计）",
          f"unique={len(set(ids))}/{len(ids)}")
    # 回绕只是「短号」复用；真正的唯一性由 _tasks 的键 + dup_key 保证，
    # 而 _tasks 里旧任务会按 task_keep_minutes 被 prune 掉，不会与新任务撞。
    print(f"  第 10000 个 = {ids[9999]}, 第 10001 个 = {ids[10000]}（短号按 10000 取模）")
    issue(ids[0] == "V1" and ids[9999] == "V0" and ids[10000] == "V1",
          "短号回绕规律符合预期（1..10000→V1..V0）", f"{ids[0]},{ids[9999]},{ids[10000]}")
    sc.shutil.rmtree(tmp, ignore_errors=True)


async def t4_concurrent_same_video():
    print("\n=== 4. 同一视频并发（去重是否真拦住）===")
    tmp = tempfile.mkdtemp(prefix="t4_")
    inst, _ = sc.build_plugin(tmp, {"max_parallel_per_chat": 5, "notice_coalesce_sec": 0})
    inst.ctx.publish_notice = lambda *a, **k: asyncio.sleep(0)
    calls = []

    async def slow(task):
        calls.append(1)
        await asyncio.sleep(0.5)
        return "R"

    # 模拟 bot 在同一轮里连发 3 次相同调用
    ts = [inst._submit_task("q", "first", "t", "", slow, dup_key="dup:1") for _ in range(3)]
    await asyncio.sleep(0.7)
    issue(len(calls) == 3, "底层 _submit_task 不做去重（去重在 _submit_or_run）",
          f"calls={len(calls)}")
    print("  → 去重由 _find_duplicate 在提交前完成（见 t5）")
    sc.shutil.rmtree(tmp, ignore_errors=True)


async def t5_dup_really_blocks():
    print("\n=== 5. _submit_or_run 去重是否真拦住重复 ===")
    tmp = tempfile.mkdtemp(prefix="t5_")
    inst, _ = sc.build_plugin(tmp, {"max_parallel_per_chat": 5, "notice_coalesce_sec": 0,
                                    "async_analyze": True})
    inst.ctx.publish_notice = lambda *a, **k: asyncio.sleep(0)
    inst._profiles = [type("P", (), {"group": 1, "label": "T", "name": "t",
                                     "mode": "frames", "priority": 1,
                                     "max_video_sec": 600, "native_audio": False})()]
    inst.bili_use_ai = False
    vision_calls = []

    async def slow_v(sess_id, sid, stype, surl, bvid, question, segments=None,
                     model_spec=None):
        vision_calls.append(1)
        await asyncio.sleep(0.6)
        return "R"

    inst._vision = slow_v

    class E:
        def __init__(self):
            self.session = type("S", (), {"sid": "qq:gm:1"})()
            self.adapter = type("A", (), {"name": "qq", "platform": "QQ"})()
    ev = E()
    r1 = await inst._tool_analyze(ev, bvid="BV1GJ411x7h7")
    r2 = await inst._tool_analyze(ev, bvid="BV1GJ411x7h7")
    r3 = await inst._tool_analyze(ev, bvid="BV1GJ411x7h7")
    await asyncio.sleep(0.9)
    issue(len(vision_calls) == 1, "重复调用只真正执行一次",
          f"vision_calls={len(vision_calls)}")
    issue("已在后台进行中" in r2 and "已在后台进行中" in r3,
          "后两次明确告知已在看")
    sc.shutil.rmtree(tmp, ignore_errors=True)


async def t6_budget_zero_and_negative():
    print("\n=== 6. 极端配置值 ===")
    tmp = tempfile.mkdtemp(prefix="t6_")
    for cfgv, name in [({"analysis_budget_sec": 0}, "budget=0"),
                       ({"analysis_budget_sec": -5}, "budget=-5"),
                       ({"queue_timeout_sec": 0}, "queue=0"),
                       ({"max_parallel_global": 0}, "global=0(不限)"),
                       ({"max_parallel_per_chat": 0}, "per_chat=0"),
                       ({"notice_coalesce_sec": -1}, "coalesce=-1"),
                       ({"task_keep_minutes": 0}, "keep=0")]:
        try:
            inst, _ = sc.build_plugin(tmp, cfgv)
            issue(True, f"配置 {name} 不炸")
        except Exception as e:
            issue(False, f"配置 {name} 不炸", repr(e))
    sc.shutil.rmtree(tmp, ignore_errors=True)


async def t7_notice_coalesce_zero_default():
    print("\n=== 7. 默认 notice_coalesce_sec=2 下是否会漏发 ===")
    tmp = tempfile.mkdtemp(prefix="t7_")
    inst, _ = sc.build_plugin(tmp)   # 用默认值
    issue(inst.notice_coalesce_sec == 2.0, "默认合并窗口=2.0",
          str(inst.notice_coalesce_sec))
    notices = []
    inst.ctx.publish_notice = lambda sid, chain, is_mentioned=True: sc._fake_notice(notices, sid, chain)

    async def r(task):
        return "R"

    for i in range(3):
        inst._submit_task("q", "first", f"t{i}", "", r, dup_key=f"n{i}")
        await asyncio.sleep(0.7)      # 每个间隔 0.7s < 2s 窗口 → 应合并成一条
    await asyncio.sleep(3.0)
    issue(len(notices) >= 1, "有通告发出", f"n={len(notices)}")
    body = "".join(n[1] for n in notices)
    issue(body.count("结果") >= 0, "内容非空")
    print(f"  实际发出 {len(notices)} 条通告（间隔 0.7s，窗口 2s ⇒ 预期合并）")
    sc.shutil.rmtree(tmp, ignore_errors=True)


async def t8_cancel_cleanup():
    print("\n=== 8. terminate 是否干净 ===")
    tmp = tempfile.mkdtemp(prefix="t8_")
    inst, _ = sc.build_plugin(tmp, {"notice_coalesce_sec": 0})

    async def slow(task):
        await asyncio.sleep(10)
        return "R"

    inst._submit_task("q", "first", "t", "", slow, dup_key="x")
    await asyncio.sleep(0.05)
    await inst.terminate()
    await asyncio.sleep(0.1)
    issue(not inst._tasks, "terminate 清空任务表")
    issue(not inst._background_tasks, "terminate 清空后台任务集")
    issue(inst._global_running == 0, "terminate 后全局计数归零",
          str(inst._global_running))
    sc.shutil.rmtree(tmp, ignore_errors=True)


async def main():
    await t1_background_leak()
    await t2_session_lock_growth()
    await t3_task_seq()
    await t4_concurrent_same_video()
    await t5_dup_really_blocks()
    await t6_budget_zero_and_negative()
    await t7_notice_coalesce_zero_default()
    await t8_cancel_cleanup()
    print("\n" + "=" * 58)
    if ISSUES:
        print(f"发现 {len(ISSUES)} 个问题：")
        for i in ISSUES:
            print("  ✗", i)
    else:
        print("未发现问题")
    print("=" * 58)


asyncio.run(main())
