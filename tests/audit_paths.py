"""审计测试：找矛盾与边界 bug（不打网络）"""
import asyncio
import os
import sys
import tempfile
import time

import os as _os
sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
import selfcheck as sc

ISSUES = []


def issue(ok, name, detail=""):
    if not ok:
        ISSUES.append(f"{name}: {detail}")
    print(("✓" if ok else "✗ BUG"), name, ("" if ok else f"  <-- {detail}"))


class FakeEvent:
    def __init__(self, sid="qq:gm:1"):
        self.session = type("S", (), {"sid": sid})()
        self.adapter = type("A", (), {"name": "qq", "platform": "QQ"})()
        self.message = type("M", (), {"message_id": "m1", "chain": []})()


async def audit_concurrency_and_state():
    print("\n=== 1. 并发与状态机 ===")
    tmp = tempfile.mkdtemp(prefix="audit1_")
    inst, _ = sc.build_plugin(tmp, {"max_parallel_per_chat": 2, "max_parallel_global": 3,
                                    "notice_coalesce_sec": 0, "analysis_budget_sec": 200})
    inst.ctx.publish_notice = lambda sid, chain, is_mentioned=True: asyncio.sleep(0)
    peak = [0]
    cur = [0]

    async def runner(task):
        cur[0] += 1
        peak[0] = max(peak[0], cur[0])
        await asyncio.sleep(0.25)
        cur[0] -= 1
        return "R"

    # 两个会话各 3 个任务，全局上限 3
    tasks = []
    for s in ("qq:gm:A", "qq:gm:B"):
        for i in range(3):
            tasks.append(inst._submit_task(s, "first", f"t{i}", "", runner, dup_key=f"{s}:{i}"))
    await asyncio.sleep(2.5)
    issue(peak[0] <= 3, "全局并发不超过上限", f"peak={peak[0]} (limit=3)")
    issue(all(t.state == "done" for t in tasks), "所有任务最终完成",
          str([t.state for t in tasks]))

    # 计数是否归还
    issue(inst._global_running == 0, "_global_running 归零（槽位已归还）",
          f"_global_running={inst._global_running}")
    issue(all(v == 0 for v in inst._chat_running.values()), "每会话计数归零",
          str(inst._chat_running))

    # 任务表是否被 prune 清理
    await asyncio.sleep(0.1)
    issue(len(inst._tasks) <= 6, "任务表大小合理", f"len={len(inst._tasks)}")
    sc.shutil.rmtree(tmp, ignore_errors=True)


async def audit_failure_paths():
    print("\n=== 2. 失败路径 ===")
    tmp = tempfile.mkdtemp(prefix="audit2_")
    inst, _ = sc.build_plugin(tmp, {"notice_coalesce_sec": 0})
    notices = []
    inst.ctx.publish_notice = lambda sid, chain, is_mentioned=True: sc._fake_notice(notices, sid, chain)

    # 2a. runner 抛异常 → 通告失败（不是"还在看"）
    async def boom(task):
        raise RuntimeError("模拟下载失败")

    t = inst._submit_task("qq:gm:A", "first", "标题", "", boom)
    await asyncio.sleep(0.3)
    issue(t.state == "failed", "异常→failed", t.state)
    body = "".join(n[1] for n in notices)
    issue("视频分析失败" in body, "失败通告含「失败」字样", body[:100])
    issue("还在看" not in body, "失败通告不含「还在看」（不误报）", body[:100])
    notices.clear()

    # 2b. runner 返回空字符串
    async def empty(task):
        return ""

    t2 = inst._submit_task("qq:gm:A", "first", "标题2", "", empty, dup_key="e")
    await asyncio.sleep(0.2)
    issue(t2.state == "done", "空结果→done", t2.state)
    body2 = "".join(n[1] for n in notices)
    issue("视频分析完成" in body2, "空结果也发完成通告", body2[:80])
    notices.clear()

    # 2c. 同步路径下 runner 抛异常
    async def boom2(task):
        raise ValueError("同步失败")

    ev = FakeEvent()
    inst._profiles = [type("P", (), {"group": 1, "label": "T", "name": "t", "mode": "frames",
                                     "priority": 1, "max_video_sec": 600,
                                     "native_audio": False})()]
    inst.async_followup = False
    sess = type("S", (), {"session_id": "abc", "title": "t", "analysis": "x",
                          "grids_base64": ["d"], "duration": 10.0, "model_tag": "",
                          "analysis_model": "m", "host_url": "", "transcript_doc": "",
                          "total_frames": 1, "scene_count": 0, "timestamps": []})()
    inst._sessions["abc"] = sess
    inst._sid_sessions["qq:gm:1"] = ["abc"]
    inst._followup = boom2
    ret = await inst._tool_analyze(ev, session_id="abc", question="问")
    issue("分析失败" in ret or "失败" in ret, "同步路径失败有明确错误返回", repr(ret[:120]))
    sc.shutil.rmtree(tmp, ignore_errors=True)


async def audit_migration_edge():
    print("\n=== 3. 迁移边界 ===")
    tmp = tempfile.mkdtemp(prefix="audit3_")
    inst, _ = sc.build_plugin(tmp)

    # 3a. cfg 缺 section
    cfg = {}
    inst._migrate_config(cfg)
    issue(True, "空配置不抛异常")

    # 3b. 值为字符串
    cfg2 = {"section_bili": {"bili_max_download_sec": "600"}}
    inst._migrate_config(cfg2)
    issue(cfg2["section_bili"]["bili_max_download_sec"] in ("600", 1800),
          "字符串值不炸", str(cfg2))

    # 3c. 值为 None
    cfg3 = {"section_bili": {"bili_max_download_sec": None}}
    try:
        inst._migrate_config(cfg3)
        issue(True, "None 值不炸")
    except Exception as e:
        issue(False, "None 值不炸", repr(e))

    # 3d. section 不是 dict
    cfg4 = {"section_bili": "oops"}
    try:
        inst._migrate_config(cfg4)
        issue(True, "section 非 dict 不炸")
    except Exception as e:
        issue(False, "section 非 dict 不炸", repr(e))

    # 3e. 标记写不进去（父路径是个文件）→ 不许应用迁移
    #     ⚠️ 不能用 chmod，因为测试常以 root 运行（root 无视只读位）
    tmp_ro = tempfile.mkdtemp(prefix="audit3ro_")
    try:
        blocker = os.path.join(tmp_ro, "blocker")
        with open(blocker, "w") as f:
            f.write("x")
        inst2, _ = sc.build_plugin(tmp_ro)
        inst2._migration_marker_path = lambda: os.path.join(
            blocker, "sub", "config_migrations.json")
        cfg5 = {"section_bili": {"bili_max_download_sec": 600}}
        inst2._migrate_config(cfg5)
        issue(cfg5["section_bili"]["bili_max_download_sec"] == 600,
              "标记写失败时不应用迁移（宁可不变）",
              str(cfg5["section_bili"]["bili_max_download_sec"]))
        # 3f. 插件配置写回失败时，本次运行仍应生效（不因写不回就退回旧值）
        inst3, _ = sc.build_plugin(tmp_ro)
        inst3._plugin_cfg_path = lambda: os.path.join(blocker, "sub2", "vc.json")
        cfg6 = {"section_bili": {"bili_max_download_sec": 600}}
        inst3._migrate_config(cfg6)
        issue(cfg6["section_bili"]["bili_max_download_sec"] == 1800,
              "写回失败时本次运行仍生效", str(cfg6["section_bili"]["bili_max_download_sec"]))
    finally:
        sc.shutil.rmtree(tmp_ro, ignore_errors=True)

    sc.shutil.rmtree(tmp, ignore_errors=True)


async def audit_budget_semantics():
    print("\n=== 4. 软预算语义 ===")
    tmp = tempfile.mkdtemp(prefix="audit4_")
    inst, _ = sc.build_plugin(tmp, {"notice_coalesce_sec": 0, "analysis_budget_sec": 200})
    notices = []
    inst.ctx.publish_notice = lambda sid, chain, is_mentioned=True: sc._fake_notice(notices, sid, chain)

    # 4a. 预算足够 → 直接拿结果，不发通告
    async def quick(task):
        await asyncio.sleep(0.05)
        return "QUICK-RESULT"

    ev = FakeEvent()
    sess = type("S", (), {"session_id": "s1", "title": "t", "analysis": "x",
                          "grids_base64": ["d"], "duration": 10.0, "model_tag": "",
                          "analysis_model": "m", "host_url": "", "transcript_doc": "",
                          "total_frames": 1, "scene_count": 0, "timestamps": []})()
    inst._sessions["s1"] = sess
    inst._sid_sessions["qq:gm:1"] = ["s1"]
    inst.async_followup = False
    inst._followup = lambda s, q, m=None: quick(None)
    ret = await inst._tool_analyze(ev, session_id="s1", question="问")
    issue("QUICK-RESULT" in ret, "同步足够预算→直接返回结果", repr(ret[:80]))
    issue(len(notices) == 0, "同步成功不发通告（避免重复）", f"n={len(notices)}")

    # 4b. 预算不足 → 转后台 + 通告（结果仍交回）
    async def slow(task):
        await asyncio.sleep(1.0)
        return "SLOW-RESULT"

    inst.analysis_budget_sec = 1
    inst._followup = lambda s, q, m=None: slow(None)
    inst.analysis_budget_sec = 0          # 立即超预算
    inst.analysis_budget_sec = 1
    t0 = time.time()
    ret2 = await inst._tool_analyze(ev, session_id="s1", question="问2")
    dt = time.time() - t0
    issue("转后台" in ret2 or "还在看" in ret2, "超预算返回转后台文案", repr(ret2[:100]))
    await asyncio.sleep(1.4)
    body = "".join(n[1] for n in notices)
    issue("SLOW-RESULT" in body, "转后台后结果仍通过通告交回", body[:150])

    sc.shutil.rmtree(tmp, ignore_errors=True)


async def audit_dup_guard():
    print("\n=== 5. 重复调用防护 ===")
    tmp = tempfile.mkdtemp(prefix="audit5_")
    inst, _ = sc.build_plugin(tmp, {"max_parallel_per_chat": 3, "notice_coalesce_sec": 0})
    inst.ctx.publish_notice = lambda sid, chain, is_mentioned=True: asyncio.sleep(0)
    inst._profiles = [type("P", (), {"group": 1, "label": "T", "name": "t", "mode": "frames",
                                     "priority": 1, "max_video_sec": 600,
                                     "native_audio": False})()]
    inst.bili_use_ai = False

    async def slow_vision(sess_id, sid, stype, surl, bvid, question, segments=None,
                          model_spec=None):
        await asyncio.sleep(1.0)
        return f"结果-{bvid}"

    inst._vision = slow_vision
    ev = FakeEvent("qq:gm:1")

    r1 = await inst._tool_analyze(ev, bvid="BV1GJ411x7h7")
    r2 = await inst._tool_analyze(ev, bvid="BV1GJ411x7h7")
    issue("已在看" in r2 or "已在后台进行中" in r2, "同一视频重复调用被拦下", repr(r2[:120]))
    await asyncio.sleep(1.4)

    # 完成后应能重新分析（不误拦）
    inst._sessions.clear()
    inst._sid_sessions.clear()
    r3 = await inst._tool_analyze(ev, bvid="BV1GJ411x7h7")
    issue("已开始分析" in r3 or "已排队" in r3, "完成后可重新分析（不误拦）", repr(r3[:100]))
    await asyncio.sleep(1.4)
    sc.shutil.rmtree(tmp, ignore_errors=True)


def audit_static_consistency():
    print("\n=== 6. 静态一致性 ===")
    main_src = sc.read("main.py")
    schema = __import__("json").loads(sc.read("schema.json"))
    a = schema["section_async"]["fields"]

    # 6a. schema 里每个 async 配置都在代码里被读取
    for k in a:
        ok = f'"{k}"' in main_src
        issue(ok, f"schema.{k} 在代码中被使用", k)

    # 6b. 代码里读的 async 键都在 schema 里
    import re
    keys = set(re.findall(r'ac\.get\("(\w+)"', main_src))
    for k in keys:
        issue(k in a, f"代码读取的 {k} 存在于 schema", sorted(set(keys) - set(a)))

    # 6c. 并发闸上限与 schema maximum 一致
    issue(a["max_parallel_per_chat"]["maximum"] == 10 and
          "min(10, int(ac.get" in main_src, "每会话并发上下限一致")
    issue(a["max_parallel_global"]["maximum"] == 32 and
          "min(32, int(ac.get" in main_src, "全局并发上下限一致")

    # 6d. 工具描述与实际行为一致
    td = main_src[main_src.find('name="analyze_video"'):main_src.find('name="analyze_video"') + 1200]
    issue("后台" in td and "稍等一下" in td, "工具描述说明了异步行为")


async def audit_l2_and_threads():
    print("\n=== 7. L2 与线程池 ===")
    src = sc.read("video_processor.py")
    import ast
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef):
            body = ast.unparse(node)
            for heavy, why in [("_scan_all_frames", "目录扫描"),
                               ("_extract_range", "逐帧抽帧"),
                               ("grid_to_base64", "JPEG 编码")]:
                if re.search(rf"(?<!\w){heavy}\s*\(", body) and "to_thread" not in body \
                        and "run_in_executor" not in body:
                    issue(False, f"{node.name} 直接调用 {heavy}", why)


async def main():
    await audit_concurrency_and_state()
    await audit_failure_paths()
    await audit_migration_edge()
    await audit_budget_semantics()
    await audit_dup_guard()
    audit_static_consistency()
    await audit_l2_and_threads()
    print("\n" + "=" * 60)
    if ISSUES:
        print(f"发现 {len(ISSUES)} 个问题：")
        for i in ISSUES:
            print("  ✗", i)
    else:
        print("未发现问题")
    print("=" * 60)


import re  # noqa
asyncio.run(main())
