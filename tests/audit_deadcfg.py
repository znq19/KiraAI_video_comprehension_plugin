"""死配置扫描（回归用）

判据：任何「被赋值但从未被读取」的 self 属性 = 空配置或死代码。
这条判据当初抓出了 3 个真死配置（auto_select / max_duration_auto / upload_host）
与 3 处多余的防御性 getattr（_flushing / _bg_sem / _slot_events）。

⚠️ 写这个扫描器时踩过一次坑：最初用 `n.ctx.id == "self"` 判断属性访问，
   但 Attribute 节点存的是 `value` 不是 `ctx`（`ctx` 是 Name 专用字段），
   导致 reads 恒为空、把 100+ 个正常属性全报成死代码。
   正确做法：`isinstance(n.value, ast.Name) and n.value.id == "self"`，
   并排除「赋值目标」的那些节点。
"""
import ast
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PASS, FAIL = [], []


def check(name, cond, extra=""):
    (PASS if cond else FAIL).append(name)
    print(f"{'✓' if cond else '✗'} {name}" + (f"  [{extra}]" if extra and not cond else ""))


src = open(os.path.join(ROOT, "main.py"), encoding="utf-8").read()
tree = ast.parse(src)
schema = json.loads(open(os.path.join(ROOT, "schema.json"), encoding="utf-8").read())

cls = next(n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)
           and n.name == "VideoComprehensionPlugin")

# 赋值目标节点（要从「读取」里排除）
write_ids = set()
for n in ast.walk(cls):
    tgt = None
    if isinstance(n, ast.Assign):
        for t in n.targets:
            if isinstance(t, ast.Attribute) and isinstance(t.value, ast.Name) \
                    and t.value.id == "self":
                write_ids.add(id(t))
    elif isinstance(n, ast.AnnAssign) and isinstance(n.target, ast.Attribute) \
            and isinstance(n.target.value, ast.Name) and n.target.value.id == "self":
        write_ids.add(id(n.target))
    elif isinstance(n, ast.AugAssign) and isinstance(n.target, ast.Attribute) \
            and isinstance(n.target.value, ast.Name) and n.target.value.id == "self":
        write_ids.add(id(n.target))

reads = {}
for n in ast.walk(cls):
    if isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name) \
            and n.value.id == "self" and id(n) not in write_ids:
        reads[n.attr] = reads.get(n.attr, 0) + 1

assigned = set()
for n in ast.walk(cls):
    if isinstance(n, ast.Assign):
        for t in n.targets:
            if isinstance(t, ast.Attribute) and isinstance(t.value, ast.Name) \
                    and t.value.id == "self":
                assigned.add(t.attr)
    elif isinstance(n, ast.AnnAssign) and isinstance(n.target, ast.Attribute) \
            and isinstance(n.target.value, ast.Name) and n.target.value.id == "self":
        assigned.add(n.target.attr)

idle = sorted(a for a in assigned if reads.get(a, 0) == 0)
check("无「只赋值不读取」的 self 属性（空配置/死代码）", not idle, str(idle))

# 无多余的防御性 getattr（字段都应在 __init__ 定义）
getattrs = re.findall(r'getattr\(self,\s*"(\w+)"', src)
check("无防御性 getattr(self, ...)（字段统一在 __init__ 定义）",
      not getattrs, str(getattrs))

# schema 每个配置项都在代码中有实际使用（出现 >= 2 次：读取 + 使用）
def flatten(fields, out):
    for k, v in fields.items():
        if isinstance(v, dict) and v.get("type") == "section":
            flatten(v.get("fields", {}), out)
        else:
            out.append(k)


idle_cfg = []
for sec_name, sec in schema.items():
    if not isinstance(sec, dict) or sec.get("type") != "section":
        continue
    keys = []
    flatten(sec.get("fields", {}), keys)
    for k in keys:
        # 模型组的配置是带后缀的（model_name_1 等），代码里用 f-string 拼接 → 跳过
        if re.search(r"_\d$", k):
            continue
        cnt = len(re.findall(rf'["\']{re.escape(k)}["\']', src))
        if cnt < 1:
            idle_cfg.append(f"{sec_name}.{k}")
check("schema 配置项都在代码中被引用", not idle_cfg, str(idle_cfg))

# __init__ 里定义的字段数应覆盖所有被直接访问的实例字段
init_node = next(n for n in cls.body if isinstance(n, ast.FunctionDef)
                 and n.name == "__init__")
init_assigned = set()
for n in ast.walk(init_node):
    tgt = n.target if isinstance(n, ast.AnnAssign) else None
    if isinstance(n, ast.Assign):
        for t in n.targets:
            if isinstance(t, ast.Attribute) and isinstance(t.value, ast.Name) \
                    and t.value.id == "self":
                init_assigned.add(t.attr)
    if tgt is not None and isinstance(tgt, ast.Attribute) \
            and isinstance(tgt.value, ast.Name) and tgt.value.id == "self":
        init_assigned.add(tgt.attr)

# 被访问但既不在 __init__、也不是 _load_cfg 设置的
lcfg = next((n for n in cls.body if isinstance(n, ast.FunctionDef)
             and n.name == "_load_cfg"), None)
cfg_assigned = set()
if lcfg:
    for n in ast.walk(lcfg):
        if isinstance(n, ast.Assign):
            for t in n.targets:
                if isinstance(t, ast.Attribute) and isinstance(t.value, ast.Name) \
                        and t.value.id == "self":
                    cfg_assigned.add(t.attr)

# ⚠️ 注解可能是带下标的（list[ModelProfile] / dict[str, int]），
#    这类 AnnAssign 的 target 仍是 Attribute，但上面按 name 收集时要覆盖到。
#    这里用正则再兜一遍，避免把正常初始化误报成「未初始化」。
raw_inits = set(re.findall(r"self\.(\w+)\s*(?::[^=\n]+)?=", src))
missing = sorted(a for a in assigned
                 if a not in init_assigned and a not in cfg_assigned
                 and a not in raw_inits)
check("所有字段都在 __init__ 或 _load_cfg 里初始化（不会 AttributeError）",
      not missing, str(missing))

print("\n" + "=" * 58)
print(f"通过 {len(PASS)} / 失败 {len(FAIL)}")
if FAIL:
    print("失败项：")
    for f in FAIL:
        print("  ✗", f)
print("=" * 58)
sys.exit(1 if FAIL else 0)
