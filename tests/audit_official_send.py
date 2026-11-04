"""QQ 官方机器人发送路径回归（离线，用假 client / 假 botpy）

覆盖：
  A. 平台判断：QQ / QQ Official 都算 QQ；get_msg 兜底只走 OneBot
  B. 直传路径：file_type=2、群/私聊路由、msg_type=7、msg_id/msg_seq
  C. 桥接路径：检测到 qqbot-fullmsg-bridge 已挂载 → 走框架通用发送链（Video 元素）
  D. 边界：30MB 超限明确拒绝；同一 msg_id 回复次数用完明确提示

跑法：cd <插件目录> && python3 tests/audit_official_send.py
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


class FakeRoute:
    def __init__(self, method, path, **params):
        self.method, self.path, self.params = method, path, params


class FakeHttp:
    def __init__(self):
        self.calls = []

    async def request(self, route, json=None):
        self.calls.append((route, json))
        return {"file_info": "FILE_INFO_123"}


class FakeApi:
    def __init__(self):
        self._http = FakeHttp()
        self.sent = []

    async def post_group_message(self, **kw):
        self.sent.append(("group", kw))
        return {"id": "sent-1"}

    async def post_c2c_message(self, **kw):
        self.sent.append(("c2c", kw))
        return {"id": "sent-2"}


class FakeClient:
    def __init__(self):
        self.api = FakeApi()


class FakeCap:
    def __init__(self):
        self._group_reply_ids = {"g1": "raw-msg-1"}
        self._direct_reply_ids = {"u1": "raw-msg-2"}
        self._reply_msg_seqs = {}


class FakeAdapter:
    def __init__(self, platform="QQ Official"):
        self.info = types.SimpleNamespace(platform=platform, name="qqofficial")
        self.client = FakeClient()
        self.cap = FakeCap()
        self.im = self.cap      # 与内核适配器一致：能力对象同时挂在 .im

    def get_client(self):
        return self.client

    def get_capability(self, _t):
        return self.cap


def install_fake_botpy():
    botpy = types.ModuleType("botpy")
    http = types.ModuleType("botpy.http")
    http.Route = FakeRoute
    botpy.http = http
    sys.modules["botpy"] = botpy
    sys.modules["botpy.http"] = http


def touch(tmp, name="v.mp4", size=1024):
    p = os.path.join(tmp, name)
    with open(p, "wb") as f:
        f.write(b"\0" * size)
    return p


async def t_platform():
    tmp = tempfile.mkdtemp(prefix="vc_of_")
    inst, _ = sc.build_plugin(tmp)
    ev = types.SimpleNamespace(adapter=types.SimpleNamespace(platform="QQ Official", name="qqofficial"))
    check("A1 _is_qq 认 QQ Official", inst._is_qq(ev) is True)
    check("A2 _is_onebot 不认 QQ Official", inst._is_onebot(ev) is False)
    ev2 = types.SimpleNamespace(adapter=types.SimpleNamespace(platform="QQ", name="qq"))
    check("A3 _is_qq 认 OneBot", inst._is_qq(ev2) is True)
    check("A4 _is_onebot 认 OneBot", inst._is_onebot(ev2) is True)
    sc.shutil.rmtree(tmp, ignore_errors=True)


async def t_direct_group():
    tmp = tempfile.mkdtemp(prefix="vc_of_")
    inst, _ = sc.build_plugin(tmp)
    install_fake_botpy()
    ad = FakeAdapter()
    path = touch(tmp)
    out = await inst._send_video_official(ad, "qqofficial:gm:g1", path,
                                          "BV1xx411c7mD", "标题", 65, "low")
    route, payload = ad.client.api._http.calls[0]
    check("B1 直传走 /v2/groups/…/files", route.path == "/v2/groups/{group_openid}/files", route.path)
    check("B2 file_type=2（视频）", payload.get("file_type") == 2, str(payload.get("file_type")))
    check("B3 srv_send_msg=False", payload.get("srv_send_msg") is False)
    check("B4 带 file_data", bool(payload.get("file_data")))
    kind, sent = ad.client.api.sent[0]
    check("B5 msg_type=7（富媒体）", sent.get("msg_type") == 7, str(sent.get("msg_type")))
    check("B6 带 media.file_info", sent.get("media", {}).get("file_info") == "FILE_INFO_123")
    check("B7 带 msg_id（被动回复）", sent.get("msg_id") == "raw-msg-1", str(sent.get("msg_id")))
    check("B8 msg_seq 从 1 开始", sent.get("msg_seq") == 1, str(sent.get("msg_seq")))
    check("B9 返回成功", out.startswith("✅ 已发送："), out)
    check("B10 msg_seq 已回写", ad.cap._reply_msg_seqs.get((True, "g1", "raw-msg-1")) == 1)
    sc.shutil.rmtree(tmp, ignore_errors=True)


async def t_direct_dm():
    tmp = tempfile.mkdtemp(prefix="vc_of_")
    inst, _ = sc.build_plugin(tmp)
    install_fake_botpy()
    ad = FakeAdapter()
    path = touch(tmp)
    await inst._send_video_official(ad, "qqofficial:dm:u1", path, "", "本地视频", 0, "low")
    route, _ = ad.client.api._http.calls[0]
    check("B11 私聊走 /v2/users/…/files", route.path == "/v2/users/{openid}/files", route.path)
    kind, sent = ad.client.api.sent[0]
    check("B12 私聊用 openid", sent.get("openid") == "u1" and "group_openid" not in sent)
    sc.shutil.rmtree(tmp, ignore_errors=True)


class FakeAdapter2x:
    """2.x 结构：没有 capabilities 模块 / 没有 .im，回复 ID 直接在适配器上。"""

    def __init__(self):
        self.info = types.SimpleNamespace(platform="QQ Official", name="qqofficial")
        self.client = FakeClient()
        self._group_reply_ids = {"g1": "raw-msg-1"}
        self._direct_reply_ids = {}
        self._reply_msg_seqs = {}

    def get_client(self):
        return self.client


async def t_2x_shape():
    tmp = tempfile.mkdtemp(prefix="vc_of_")
    inst, _ = sc.build_plugin(tmp)
    install_fake_botpy()
    ad = FakeAdapter2x()
    path = touch(tmp)
    out = await inst._send_video_official(ad, "qqofficial:gm:g1", path, "BVx", "T", 10, "low")
    _, sent = ad.client.api.sent[0]
    check("B13 2.x 结构也能拿到 msg_id", sent.get("msg_id") == "raw-msg-1", str(sent.get("msg_id")))
    check("B14 2.x 结构发送成功", out.startswith("✅ 已发送："), out)
    sc.shutil.rmtree(tmp, ignore_errors=True)


async def t_bridge():
    tmp = tempfile.mkdtemp(prefix="vc_of_")
    inst, _ = sc.build_plugin(tmp)
    ad = FakeAdapter()

    # 模拟桥接：_upload_file 带标记 + _send_message 被包装
    async def _upload(*a, **k):
        return None
    _upload._kira_bridge_ftype = True
    ad.cap._upload_file = _upload
    ad.cap._send_message = lambda *a, **k: None   # 实例属性 = 已包装
    calls = []

    async def fake_send_chain(sid, chain):
        calls.append((sid, chain))
        return types.SimpleNamespace(ok=True, err="")

    inst.ctx.send_message_chain = fake_send_chain
    path = touch(tmp)
    out = await inst._send_video_official(ad, "qqofficial:gm:g1", path,
                                          "BV1xx411c7mD", "标题", 65, "low")
    check("C1 检测到桥接 → 走通用发送链", len(calls) == 1, str(len(calls)))
    check("C2 没有走直传 HTTP", not ad.client.api._http.calls)
    check("C3 返回成功", out.startswith("✅ 已发送："), out)
    sc.shutil.rmtree(tmp, ignore_errors=True)


async def t_limits():
    tmp = tempfile.mkdtemp(prefix="vc_of_")
    inst, _ = sc.build_plugin(tmp)
    install_fake_botpy()
    ad = FakeAdapter()

    async def no_compress(*a, **k):
        raise RuntimeError("no ffmpeg in test")

    inst._compress_cached = no_compress
    big = touch(tmp, "big.mp4", size=31 * 1024 * 1024)
    out = await inst._send_video_official(ad, "qqofficial:gm:g1", big, "BVx", "T", 10, "low")
    check("D1 超 30MB 明确拒绝", "30MB" in out and "未发送" in out, out)
    check("D2 超限时不发请求", not ad.client.api._http.calls and not ad.client.api.sent)

    # msg_seq 用满
    ad2 = FakeAdapter()
    ad2.cap._reply_msg_seqs[(True, "g1", "raw-msg-1")] = 5
    small = touch(tmp, "ok.mp4", 1024)
    out2 = await inst._send_video_official(ad2, "qqofficial:gm:g1", small, "BVx", "T", 10, "low")
    check("D3 回复次数用满有明确提示", "5 次" in out2, out2)
    check("D4 用满时不再发送", not ad2.client.api.sent)
    sc.shutil.rmtree(tmp, ignore_errors=True)


async def main():
    await t_platform()
    await t_direct_group()
    await t_direct_dm()
    await t_2x_shape()
    await t_bridge()
    await t_limits()
    print("\n" + "=" * 60)
    print(f"通过 {len(PASS)} / 失败 {len(FAIL)}")
    if FAIL:
        print("失败项：" + ", ".join(FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
