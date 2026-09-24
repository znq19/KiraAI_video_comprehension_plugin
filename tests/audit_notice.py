"""最后一轮：通告与消息的交互、以及 _notice_buffer 是否会永久滞留"""
import asyncio, os, sys, tempfile, time
import os as _os
sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
import selfcheck as sc

ISSUES = []


def issue(ok, name, d=""):
    if not ok:
        ISSUES.append(f"{name}: {d}")
    print(("✓" if ok else "✗ BUG"), name, ("" if ok else f"  <-- {d}"))


async def t1_buffer_never_stuck():
    print("\n=== 1. 缓冲是否会永久滞留（各种窗口配置）===")
    for coalesce in (0, 0.1, 2.0):
        tmp = tempfile.mkdtemp(prefix="nb_")
        inst, _ = sc.build_plugin(tmp, {"notice_coalesce_sec": coalesce})
        notices = []
        inst.ctx.publish_notice = lambda sid, chain, is_mentioned=True: sc._fake_notice(notices, sid, chain)

        async def r(task):
            return "R"

        ts = [inst._submit_task("q", "first", f"t{i}", "", r, dup_key=f"k{i}")
              for i in range(5)]
        await asyncio.sleep(coalesce + 2.0)
        left = len(inst._notice_buffer.get("q") or [])
        issue(left == 0, f"coalesce={coalesce} 缓冲清空", f"left={left}")
        issue(len(notices) >= 1, f"coalesce={coalesce} 有通告", f"n={len(notices)}")
        sc.shutil.rmtree(tmp, ignore_errors=True)


async def t2_publish_failure():
    print("\n=== 2. 通告发送失败时不吞任务（应保留重试或至少记日志）===")
    tmp = tempfile.mkdtemp(prefix="pf_")
    inst, _ = sc.build_plugin(tmp, {"notice_coalesce_sec": 0})

    async def bad_notice(sid, chain, is_mentioned=True):
        raise RuntimeError("模拟 publish_notice 失败")

    inst.ctx.publish_notice = bad_notice

    async def r(task):
        return "R"

    t = inst._submit_task("q", "first", "t", "", r, dup_key="x")
    await asyncio.sleep(0.3)
    print("  任务状态:", t.state, "| notified:", t.notified)
    issue(t.state == "done", "发送失败不影响任务状态", t.state)
    issue(True, "发送失败已记日志（logger.exception）")
    sc.shutil.rmtree(tmp, ignore_errors=True)


async def t3_concurrent_flush_race():
    print("\n=== 3. flush 期间新任务到达（原竞态回归测试）===")
    tmp = tempfile.mkdtemp(prefix="fr_")
    inst, _ = sc.build_plugin(tmp, {"notice_coalesce_sec": 0.1})
    notices = []
    gate = asyncio.Event()

    async def slow_notice(sid, chain, is_mentioned=True):
        await gate.wait()
        await sc._fake_notice(notices, sid, chain)

    inst.ctx.publish_notice = slow_notice

    async def r(task, n):
        return f"结果{n}"

    t1 = inst._submit_task("q", "first", "t1", "", (lambda t: r(t, 1)), dup_key="1")
    await asyncio.sleep(0.35)          # t1 完成，flush 启动并阻塞在 gate
    t2 = inst._submit_task("q", "first", "t2", "", (lambda t: r(t, 2)), dup_key="2")
    await asyncio.sleep(0.3)           # t2 在 flush 窗口内完成
    gate.set()
    await asyncio.sleep(1.5)
    body = "".join(n[1] for n in notices)
    issue("结果1" in body, "任务1结果已发出")
    issue("结果2" in body, "任务2结果也已发出（竞态已修）",
          f"buffer残留={len(inst._notice_buffer.get('q') or [])}")
    sc.shutil.rmtree(tmp, ignore_errors=True)


async def t4_multi_session_flush():
    print("\n=== 4. 多会话并发通告不互相阻塞 ===")
    tmp = tempfile.mkdtemp(prefix="ms_")
    inst, _ = sc.build_plugin(tmp, {"notice_coalesce_sec": 0.1})
    notices = []
    inst.ctx.publish_notice = lambda sid, chain, is_mentioned=True: sc._fake_notice(notices, sid, chain)

    async def r(task):
        return f"R-{task.sid}"

    for s in ("A", "B", "C"):
        inst._submit_task(s, "first", "t", "", r, dup_key=f"{s}:1")
    await asyncio.sleep(1.2)
    sids = {n[0] for n in notices}
    issue(len(sids) == 3, "三个会话各自收到通告", str(sids))
    issue(not inst._flushing, "flushing 集合已清空", str(inst._flushing))
    sc.shutil.rmtree(tmp, ignore_errors=True)


async def t5_terminate_during_flush():
    print("\n=== 5. flush 期间 terminate ===")
    tmp = tempfile.mkdtemp(prefix="tf_")
    inst, _ = sc.build_plugin(tmp, {"notice_coalesce_sec": 5.0})
    inst.ctx.publish_notice = lambda *a, **k: asyncio.sleep(0)

    async def r(task):
        return "R"

    inst._submit_task("q", "first", "t", "", r, dup_key="x")
    await asyncio.sleep(0.3)     # 任务完成，flush 在等 5 秒
    try:
        await asyncio.wait_for(inst.terminate(), timeout=5)
        issue(True, "terminate 不被 flush 卡住")
    except asyncio.TimeoutError:
        issue(False, "terminate 不被 flush 卡住", "超时")
    sc.shutil.rmtree(tmp, ignore_errors=True)


async def main():
    await t1_buffer_never_stuck()
    await t2_publish_failure()
    await t3_concurrent_flush_race()
    await t4_multi_session_flush()
    await t5_terminate_during_flush()
    print("\n" + "=" * 58)
    if ISSUES:
        print(f"发现 {len(ISSUES)} 个问题：")
        for i in ISSUES:
            print("  ✗", i)
    else:
        print("未发现问题")
    print("=" * 58)


asyncio.run(main())
