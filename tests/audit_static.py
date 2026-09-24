"""静态全量自查：死代码 / 未定义引用 / 未使用配置 / 矛盾"""
import ast
import json
import re
import sys

ROOT = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
src = open(f"{ROOT}/main.py", encoding="utf-8").read()
tree = ast.parse(src)

cls = next(n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)
           and n.name == "VideoComprehensionPlugin")

methods = {n.name: n for n in cls.body
           if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
print(f"方法总数: {len(methods)}")

# 1. 死方法（定义了但类内无调用、且不是 hook/tool/生命周期）
NON_DEAD = {"__init__", "initialize", "terminate", "reload_cfg"}
dead = []
for name, node in methods.items():
    if name in NON_DEAD:
        continue
    decorated = False
    for d in node.decorator_list:
        t = ast.unparse(d)
        if "register" in t or "on." in t:
            decorated = True
    if decorated:
        continue
    pat = re.compile(rf"(?<![\w.]){re.escape(name)}\s*\(")
    hits = [n.lineno for n in ast.walk(cls)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
            and n.func.attr == name]
    if not hits:
        dead.append(name)
print("\n=== 死方法 ===")
if dead:
    for d in dead:
        print(f"  ✗ {d} (L{methods[d].lineno})")
else:
    print("  ✓ 无")

# 2. self 属性：写了但从未读
assigned = set()
for n in ast.walk(cls):
    if isinstance(n, ast.Attribute) and isinstance(n.ctx, ast.Name) and n.ctx.id == "self":
        if isinstance(n.ctx, ast.Name):
            pass
for n in ast.walk(cls):
    if isinstance(n, ast.Assign):
        for t in n.targets:
            s = ast.unparse(t)
            if s.startswith("self."):
                assigned.add(s[5:])
    if isinstance(n, ast.AnnAssign) and n.target:
        s = ast.unparse(n.target)
        if s.startswith("self."):
            assigned.add(s[5:])
unused = []
for a in sorted(assigned):
    pat = re.compile(rf"self\.{re.escape(a)}\b")
    # 至少要出现 2 次（1 次赋值 + 至少 1 次使用）
    if len(pat.findall(src)) <= 1:
        unused.append(a)
print("\n=== 只赋值未使用的 self 属性 ===")
if unused:
    for u in unused:
        print(f"  ✗ {u}")
else:
    print("  ✓ 无")

# 3. schema 配置项是否都被读取
schema = json.loads(open(f"{ROOT}/schema.json", encoding="utf-8").read())
def walk_fields(fields, out):
    for k, v in fields.items():
        if isinstance(v, dict) and v.get("type") == "section":
            walk_fields(v.get("fields", {}), out)
        else:
            out.add(k)
for sec in ("section_async", "section_bili", "section_audio", "section_limits",
            "section_basic", "section_upload", "section_session", "section_cache"):
    keys = set()
    walk_fields({"x": {"type": "section", "fields": schema[sec]["fields"]}}, keys)
    missing = [k for k in keys if f'"{k}"' not in src]
    print(f"\n=== {sec}: {len(keys)} 项 ===")
    if missing:
        print(f"  ✗ 未在代码中使用: {missing}")
    else:
        print("  ✓ 全部被使用")

# 4. 悬空引用（self.xxx 未在类里定义）
print("\n=== 悬空 self 引用 ===")
defined = set(assigned) | set(methods)
for n in ast.walk(cls):
    if isinstance(n, ast.Attribute) and isinstance(n.ctx, ast.Name) and n.ctx.id == "self":
        if n.attr not in defined and not n.attr.startswith("_") is False:
            pass
used_attrs = set()
for n in ast.walk(cls):
    if isinstance(n, ast.Attribute) and isinstance(n.ctx, ast.Name) and n.ctx.id == "self":
        used_attrs.add(n.attr)
dangling = sorted(a for a in used_attrs if a not in defined)
if dangling:
    for d in dangling:
        print(f"  ? {d}")
else:
    print("  ✓ 无")

# 5. 所有 await 调用的目标是否存在
print("\n=== await self.X 的 X 是否都有定义 ===")
bad = []
for n in ast.walk(cls):
    if isinstance(n, ast.Await) and isinstance(n.value, ast.Call):
        f = n.value.func
        if isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name) \
                and f.value.id == "self":
            if f.attr not in defined:
                bad.append((n.lineno, f.attr))
if bad:
    for ln, a in bad:
        print(f"  ✗ L{ln}: self.{a}")
else:
    print("  ✓ 无")
