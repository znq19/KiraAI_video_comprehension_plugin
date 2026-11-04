"""自动发送「已接管/已发送」标注 + 防重发回归测试（离线，不依赖 KiraAI 本体）。

覆盖 v1.18.1 修复的四个点：
  A. im_message hook 不再联网阻塞（短链解析挪到后台）
  B. 标注记录在「检测到链接时」就写好 → 当轮 LLM 一定能看到
  C. 原始消息不在本轮 batch 时，退化为 tail persist=False 提示
  D. 防重发窗口：pending 拦、sent 180s 内拦、failed 放行

跑法：cd <插件目录> && python3 tests/audit_auto_sent_note.py
"""
import asyncio
import os
import sys
import tempfile
import time
import types

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import selfcheck as sc

PASS, FAIL = [], []


def check(name, cond, extra=""):
    (PASS if cond else FAIL).append(name)
    print(f"{'✓' if cond else '✗'} {name}" + (f"  [{extra}]" if extra and not cond else ""))


def make_event(mod, text, mid="m1", sid="qq:gm:g1"):
    Text = sys.modules["core.chat.message_elements"].Text
    msg = types.SimpleNamespace(message_id=mid, self_id="10000", raw_message=None,
                                chain=[Text(text)])
    return types.SimpleNamespace(
        session=types.SimpleNamespace(sid=sid),
        adapter=types.SimpleNamespace(name="QQ", platform="QQ"),
        message=msg, raw_message=None)


def make_batch(mod, msgs, sid="qq:gm:g1"):
    return types.SimpleNamespace(session=types.SimpleNamespace(sid=sid), messages=msgs)


def make_req(mod, *texts):
    Prompt = sys.modules["core.prompt_manager"].Prompt
    return types.SimpleNamespace(
        user_prompt=[Prompt(t, name="message", source="system", render_template=False)
                     for t in texts])


async def t_hook_nonblocking():
    tmp = tempfile.mkdtemp(prefix="vc_note_")
    inst, mod = sc.build_plugin(tmp)
    inst.auto_send_link = True
    inst.bili_enabled = True
    inst.auto_send_allowed_sid = []
    inst.auto_send_dry_run = False
    inst.dl_timeout = 5
    inst.send_dedup_enabled = True          # 默认关闭；这里显式开启以验证窗口逻辑
    inst.send_dedup_window_sec = 20
    calls = []

    async def slow_send(_ev, bvid, quality="", sid="", adapter_name=""):
        calls.append(bvid)
        await asyncio.sleep(3)
        return "✅ 已发送：T\nBV: BV1xx411c7mD | ⏱ 1:00 | 质量: medium\n📁 本地路径: /tmp/v.mp4"

    async def slow_extract(url, *a, **k):
        await asyncio.sleep(5)
        return "BV1xx411c7mD"

    inst._send_video_by_bvid = slow_send
    mod.extract_bvid = slow_extract
    ev = make_event(mod, "看看 https://b23.tv/V5Xhy88")
    t0 = time.time()
    await inst._auto_send_hook(ev)
    cost = time.time() - t0
    check("A1 hook 不阻塞（<200ms，短链解析在后台）", cost < 0.2, f"{cost*1000:.0f}ms")
    rec = inst._auto_sent.get("qq:gm:g1") or {}
    check("B1 检测时即写入 pending 记录", rec.get("state") == "pending", str(rec))
    check("B2 匹配键含短链 id", "V5Xhy88" in (rec.get("match_keys") or []), str(rec))
    check("D1 pending 期间查重命中", inst._recent_auto_hit("qq:gm:g1", "", "https://b23.tv/V5Xhy88") is not None)

    # 当轮 LLM 请求（1.5s 之后，下载还没完）
    await asyncio.sleep(1.5)
    other = types.SimpleNamespace(chain=[sys.modules["core.chat.message_elements"].Text("别的话")])
    link = types.SimpleNamespace(chain=[sys.modules["core.chat.message_elements"].Text("看看 https://b23.tv/V5Xhy88")])
    req = make_req(mod, "别的话", "看看 https://b23.tv/V5Xhy88")
    await inst._inject_auto_sent_note(make_batch(mod, [other, link]), req)
    check("B3 标注落在原始链接消息上", "系统提示" in req.user_prompt[1].content, req.user_prompt[1].content)
    check("B4 未污染其他消息", "系统提示" not in req.user_prompt[0].content)
    check("B5 文案为「已由插件自动下载并发送」", "已由插件自动下载并发送" in req.user_prompt[1].content
          and "正在" not in req.user_prompt[1].content)
    check("B6 记录保留待落定（consumed）", (inst._auto_sent.get("qq:gm:g1") or {}).get("consumed") is True)

    for t in list(inst._background_tasks):
        if not t.done():
            await t
    rec = inst._auto_sent.get("qq:gm:g1") or {}
    check("B7 发送完成 → state=sent", rec.get("state") == "sent", str(rec))
    # 成功后再来一轮：静默收尾，不重复标注
    req2 = make_req(mod, "新消息")
    new_msg = types.SimpleNamespace(chain=[sys.modules["core.chat.message_elements"].Text("新消息")])
    await inst._inject_auto_sent_note(make_batch(mod, [new_msg]), req2)
    check("B8 成功不重复刷标注", all("系统提示" not in p.content for p in req2.user_prompt))
    check("B9 记录已消费干净", "qq:gm:g1" not in inst._auto_sent)
    check("D2 sent 窗口内查重命中", inst._recent_auto_hit("qq:gm:g1", "BV1xx411c7mD") is not None)
    _k = "BV1XX411C7MD"
    _recs = inst._recent_auto.get("qq:gm:g1") or {}
    if _k in _recs:
        _recs[_k] = (_recs[_k][0] - 21, _recs[_k][1], _recs[_k][2])   # 模拟 21 秒前
    check("D2b 超过 20s 窗口不再拦", inst._recent_auto_hit("qq:gm:g1", "BV1xx411c7mD") is None)
    check("D3 发送次数=1", len(calls) == 1, str(calls))
    inst.send_dedup_enabled = False
    check("D4 默认关闭时完全不拦", inst._recent_auto_hit("qq:gm:g1", "BV1xx411c7mD") is None)
    sc.shutil.rmtree(tmp, ignore_errors=True)


async def t_card_message():
    """QQ 小程序卡片（非 Text 元素）也要能定位到那条消息并标注。"""
    tmp = tempfile.mkdtemp(prefix="vc_note_")
    inst, mod = sc.build_plugin(tmp)
    inst.auto_send_link = True
    inst.bili_enabled = True
    inst.auto_send_allowed_sid = []
    inst.auto_send_dry_run = False
    inst.dl_timeout = 5

    class Card:
        def __init__(self, url):
            self.data = {"app": "com.tencent.miniapp_01",
                         "meta": {"detail_1": {"qqdocurl": url, "title": "卡片"}}}

        def __repr__(self):
            return "Card"

    msg = types.SimpleNamespace(message_id="m-card", self_id="10000", raw_message=None,
                                chain=[Card("https://b23.tv/V5Xhy88")])
    ev = types.SimpleNamespace(session=types.SimpleNamespace(sid="qq:gm:g1"),
                               adapter=types.SimpleNamespace(name="QQ", platform="QQ"),
                               message=msg, raw_message=None)
    ok = False

    async def fake_send(*a, **k):
        return "✅ 已发送：T\n📁 本地路径: /tmp/v.mp4"

    async def fake_extract(url, *a, **k):
        return "BV1xx411c7mD"

    inst._send_video_by_bvid = fake_send
    mod.extract_bvid = fake_extract
    await inst._auto_send_hook(ev)
    ok = "qq:gm:g1" in inst._auto_sent
    # 当轮就注入（与真实时序一致：debounce 1.5s 时下载还没完）
    req = make_req(mod, "<card>")
    batch_msg = types.SimpleNamespace(chain=[Card("https://b23.tv/V5Xhy88")])
    await inst._inject_auto_sent_note(make_batch(mod, [batch_msg]), req)
    for t in list(inst._background_tasks):
        if not t.done():
            await t
    check("E1 卡片消息能检测到链接", ok)
    check("E2 卡片消息也能标注", "已由插件自动下载并发送" in req.user_prompt[0].content,
          req.user_prompt[0].content)
    sc.shutil.rmtree(tmp, ignore_errors=True)


async def t_fail_and_fallback():
    tmp = tempfile.mkdtemp(prefix="vc_note_")
    inst, mod = sc.build_plugin(tmp)
    inst.auto_send_link = True
    inst.bili_enabled = True
    inst.auto_send_allowed_sid = []
    inst.auto_send_dry_run = False
    inst.dl_timeout = 5
    inst.send_dedup_enabled = True
    inst.send_dedup_window_sec = 20

    async def fail_send(*a, **k):
        return "⚠️ 下载失败：网络错误"

    inst._send_video_by_bvid = fail_send
    eval_ = make_event(mod, "https://b23.tv/V5Xhy88")
    await inst._auto_send_hook(eval_)
    req = make_req(mod, "看看 https://b23.tv/V5Xhy88")
    msg = types.SimpleNamespace(chain=[sys.modules["core.chat.message_elements"].Text("看看 https://b23.tv/V5Xhy88")])
    await inst._inject_auto_sent_note(make_batch(mod, [msg]), req)
    check("F1 pending 先标注", "系统提示" in req.user_prompt[0].content)
    for t in list(inst._background_tasks):
        if not t.done():
            await t
    check("F2 失败 → state=failed", (inst._auto_sent.get("qq:gm:g1") or {}).get("state") == "failed")
    check("D5 失败放行重试", inst._recent_auto_hit("qq:gm:g1", "", "https://b23.tv/V5Xhy88") is None)
    req2 = make_req(mod, "新消息")
    new_msg = types.SimpleNamespace(chain=[sys.modules["core.chat.message_elements"].Text("新消息")])
    await inst._inject_auto_sent_note(make_batch(mod, [new_msg]), req2)
    tail = [p for p in req2.user_prompt if p.name == "video_auto_sent"]
    check("F3 失败补一句尾部提示", bool(tail) and "自动发送失败" in tail[0].content, str(tail))
    check("F4 尾部提示 persist=False", bool(tail) and tail[0].persist is False)

    # 兜底：原始消息不在 batch
    inst._auto_sent["qq:gm:g1"] = {
        "rid": "r", "state": "sent", "bvid": "BV1xx411c7mD", "title": "T",
        "file_path": "/tmp/v.mp4", "match_keys": ["BV1xx411c7mD"], "message_id": "zzz",
        "ts": time.time(),
    }
    req3 = make_req(mod, "另一个人说话")
    other = types.SimpleNamespace(chain=[sys.modules["core.chat.message_elements"].Text("另一个人说话")])
    await inst._inject_auto_sent_note(make_batch(mod, [other]), req3)
    tail3 = [p for p in req3.user_prompt if p.name == "video_auto_sent"]
    check("C1 原消息不在 batch → 尾部提示", bool(tail3) and "已由插件自动下载并发送" in tail3[0].content)
    check("C2 尾部提示不写历史", bool(tail3) and tail3[0].persist is False)
    sc.shutil.rmtree(tmp, ignore_errors=True)


async def main():
    await t_hook_nonblocking()
    await t_card_message()
    await t_fail_and_fallback()
    print("\n" + "=" * 60)
    print(f"通过 {len(PASS)} / 失败 {len(FAIL)}")
    if FAIL:
        print("失败项：" + ", ".join(FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
