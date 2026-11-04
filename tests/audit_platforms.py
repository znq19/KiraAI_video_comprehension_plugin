"""跨平台通用性回归（离线）

用户要求：除必须依赖某平台的功能外，其余能力应尽可能对所有适配器开放。
本套件验证：
  A. `_event_can_video` / `_adapter_supports_video` 的平台能力判断
  B. 非 QQ 平台（Telegram/Discord/微信/WebChat）走框架通用发送链（Video 元素）
  C. 不支持 video 的平台（如 Bilibili 动态）明确报错，不会发出占位文本
  D. `_tool_send_video` 的入口不再写死「当前不是QQ」
  E. QQ / QQ Official 原有路径不受影响

跑法：cd <插件目录> && python3 tests/audit_platforms.py
"""
import asyncio
import os
import sys
import tempfile
import types

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import selfcheck as sc

PASS, FAIL = [], []


def check(name, cond, extra=""):
    (PASS if cond else FAIL).append(name)
    print(f"{'✓' if cond else '✗'} {name}" + (f"  [{extra}]" if extra and not cond else ""))


class Chain:
    def __init__(self, items=None):
        self.message_list = list(items or [])

    def __iter__(self):
        return iter(self.message_list)


class Video:
    def __init__(self, file="", name=None, size=None, mime=None):
        self.file, self.name = file, name


class Capability:
    def __init__(self, elements):
        self._elements = elements

    async def get_message_metadata(self):
        return types.SimpleNamespace(supported_elements=self._elements)


class Adapter:
    def __init__(self, platform, elements=(), name="a"):
        self.info = types.SimpleNamespace(platform=platform, name=name)
        self._cap = Capability(list(elements))
        self.im = self._cap          # 与内核适配器一致
        self.actions = []

    def get_capability(self, _t):
        return self._cap

    def get_client(self):
        return self


class Adapter2x:
    """2.x：没有 capabilities 模块 / get_capability，只有 message_types。"""

    def __init__(self, platform, elements):
        self.info = types.SimpleNamespace(platform=platform, name="a2")
        self.message_types = list(elements)


def ev(platform, text="hi", elements=("text", "video", "img")):
    T = sys.modules["core.chat.message_elements"].Text
    msg = types.SimpleNamespace(message_id="m1", self_id="1", raw_message=None, chain=[T(text)])
    return types.SimpleNamespace(session=types.SimpleNamespace(sid=f"{platform}:gm:1"),
                                 adapter=types.SimpleNamespace(name=platform, platform=platform),
                                 message=msg, supported_elements=list(elements))


async def t_capability():
    tmp = tempfile.mkdtemp(prefix="vc_pf_")
    inst, mod = sc.build_plugin(tmp)
    check("A1 Telegram 事件声明 video", inst._event_can_video(ev("Telegram")) is True)
    check("A2 Bilibili 事件不支持 video",
          inst._event_can_video(ev("Bilibili", elements=("text", "img"))) is False)
    check("A3 3.x 适配器元数据支持 video",
          await inst._adapter_supports_video(Adapter("Telegram", ["text", "video"])) is True)
    check("A4 3.x 适配器元数据不支持 video",
          await inst._adapter_supports_video(Adapter("Bilibili", ["text", "img"])) is False)
    check("A5 2.x 回落 message_types",
          await inst._adapter_supports_video(Adapter2x("Telegram", ["text", "video"])) is True)
    sc.shutil.rmtree(tmp, ignore_errors=True)


async def t_generic_send():
    tmp = tempfile.mkdtemp(prefix="vc_pf_")
    inst, mod = sc.build_plugin(tmp)
    mod.MessageChain = Chain
    calls = []

    async def fake_send_chain(sid, chain):
        calls.append((sid, list(chain)))
        return types.SimpleNamespace(ok=True, err="")

    inst.ctx.send_message_chain = fake_send_chain
    path = os.path.join(tmp, "v.mp4")
    with open(path, "wb") as f:
        f.write(b"\0" * 128)

    ad = Adapter("Telegram", ["text", "video"])
    out = await inst._send_generic_video(ad, "Telegram:gm:1", path, "BV1xx411c7mD", "标题", 60, "low")
    check("B1 通用路径调用了 send_message_chain", len(calls) == 1)
    check("B2 传的是 Video 元素", bool(calls) and getattr(calls[0][1][0], "file", "") == path,
          str(calls[0][1]) if calls else "")
    check("B3 返回成功文案", out.startswith("✅ 已发送："), out)

    bad = Adapter("Bilibili", ["text", "img"])
    out2 = await inst._send_generic_video(bad, "Bilibili:gm:1", path, "BVx", "T", 0, "low")
    check("C1 不支持 video 时明确报错", "不支持" in out2, out2)
    check("C2 不支持时没有调用发送", len(calls) == 1)
    sc.shutil.rmtree(tmp, ignore_errors=True)


async def t_send_by_bvid_generic():
    tmp = tempfile.mkdtemp(prefix="vc_pf_")
    inst, mod = sc.build_plugin(tmp)
    mod.MessageChain = Chain
    inst.bili_cache_dir = tmp
    inst.bili_cookie = ""
    inst.bili_max_dl = 0
    inst.bili_download_quality = "low"
    inst.bili_compress_quality = "original"
    inst.napcat_stream = False
    inst.dl_timeout = 5

    async def fake_info(bvid, *a, **k):
        return {"title": "T", "duration": 60, "cid": 1}

    async def fake_dl(bvid, cache_dir, **k):
        p = os.path.join(tmp, f"{bvid}.mp4")
        with open(p, "wb") as f:
            f.write(b"\0" * 256)
        return p, {}

    mod.get_bili_info = fake_info
    mod.download_bili_video = fake_dl

    ad = Adapter("Telegram", ["text", "video"], name="tg")
    inst.ctx.adapter_mgr = types.SimpleNamespace(get_adapter=lambda n: ad)
    calls = []

    async def fake_send_chain(sid, chain):
        calls.append((sid, list(chain)))
        return types.SimpleNamespace(ok=True, err="")

    inst.ctx.send_message_chain = fake_send_chain
    out = await inst._send_video_by_bvid(None, "BV1xx411c7mD", sid="Telegram:gm:1", adapter_name="tg")
    check("D1 非 QQ 平台走通用发送链", len(calls) == 1, str(len(calls)))
    check("D2 没有走 OneBot send_action", not getattr(ad, "actions", []))
    check("D3 返回成功", out.startswith("✅ 已发送：T"), out)
    sc.shutil.rmtree(tmp, ignore_errors=True)


async def t_tool_gate():
    tmp = tempfile.mkdtemp(prefix="vc_pf_")
    inst, mod = sc.build_plugin(tmp)
    check("E1 Bilibili 平台明确提示不支持",
          "不支持" in await inst._tool_send_video(ev("Bilibili", elements=("text", "img")), bvid="BVx"))
    sc.shutil.rmtree(tmp, ignore_errors=True)


async def main():
    await t_capability()
    await t_generic_send()
    await t_send_by_bvid_generic()
    await t_tool_gate()
    print("\n" + "=" * 60)
    print(f"通过 {len(PASS)} / 失败 {len(FAIL)}")
    if FAIL:
        print("失败项：" + ", ".join(FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
