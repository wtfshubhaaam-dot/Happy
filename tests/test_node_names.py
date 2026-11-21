"""Check all 50 node IDs get unique deterministic labels."""
import ast
from pathlib import Path
import re
src = Path(__file__).resolve().parents[1] / "utils" / "cluster.py"
tree = ast.parse(src.read_text())
items = [node for node in tree.body if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "_NAMES" for t in node.targets)]
func = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_default_name")
ns = {"re": re}
exec(compile(ast.Module(body=[items[0], func], type_ignores=[]), str(src), "exec"), ns)
labels = [ns["_default_name"](f"n{i}") for i in range(1, 51)]
assert len(labels) == len(set(labels)) == 50
assert labels[0] == "👑 Royal Phantom"
assert labels[49] == "👑 Infinity Emperor"
assert ns["_default_name"]("custom") == "🖥️ Node custom"
print("PASS: 50 distinct stable names, correct endpoints, safe custom ID fallback")
