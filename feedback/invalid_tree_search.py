# -*- coding: utf-8 -*-
"""
inspect_invalid_tree.py

Read invalid_context_trees.json and check whether a path exists.
If it exists, print occurences / ivalid / invalidity for each node.

Path format:
{
  "qtype": 1,  # root key, either int or string
  "from": {"type": 2, "assigned_shard": True,  "joined_on_shard_key": False},
  "ops":  ["JOIN_INNER", "Pred_CorrLeft", "InferSite_Workers"],  # stored operator_type text
  "to":   {"type": 0, "assigned_shard": False, "joined_on_shard_key": False},
}

Note: table_type_info is matched strictly on type / assigned_shard / joined_on_shard_key.
"""

import json
from typing import Any, Dict, List, Optional, Tuple

# ========== Editable area: set the file and path to inspect ==========
JSON_PATH = "./../a.json"

PATHS: List[Dict[str, Any]] = [
    # Example. Replace this with the path to inspect.
    {
      "qtype": 0,
      "from": {"type": 2, "assigned_shard": True,  "joined_on_shard_key": False},
      "ops":  ["left outer_parent","left outer"],
      "to":   {"type": 2, "assigned_shard": True, "joined_on_shard_key": False},
    },
    # {
    #   "qtype": 3,
    #   "from": {"type": 2, "assigned_shard": True,  "joined_on_shard_key": False},
    #   "ops":  ["inner_parent","inner", "column_ref_by","made_from"],
    #   "to":   {"type": 1, "assigned_shard": False, "joined_on_shard_key": False},
    # },
]
# From type: 2, shard:0, join:0, Operators: inner_parent inner column_ref_by made_from , To type: 2, shard:0, join:0
# ===========================================================


def load_roots(path: str) -> Dict[str, Any]:
    with open(path, "rb") as f:
        data = json.load(f)
    roots = data.get("roots", {})
    # JSON keys are usually strings. Keep string keys and integer aliases for lookup.
    out: Dict[str, Any] = {}
    for k, v in roots.items():
        out[str(k)] = v
        try:
            out[str(int(k))] = v  # Make "1" and 1 resolve to the same entry.
        except Exception:
            pass
    return out


def get_counts(node: Dict[str, Any]) -> Tuple[int, int, float]:
    # Preserve the spelling used by the C++ side: occurences.
    occ = int(node.get("occurences", 0))
    iv  = int(node.get("ivalid", 0))
    inv = (iv / occ) if occ > 0 else 0.0
    return occ, iv, inv


def eq_table_info(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    # Strictly compare the three fields. Missing booleans default to False.
    def norm(x: Dict[str, Any]) -> Tuple[int, bool, bool]:
        return (
            int(x.get("type", 0)),
            bool(x.get("assigned_shard", False)),
            bool(x.get("joined_on_shard_key", False)),
        )
    return norm(a) == norm(b)


def find_child_start(cur: Dict[str, Any], from_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    for ch in cur.get("children", []):
        if (not ch.get("is_terminal", False)) and ch.get("is_start", False):
            ti = ch.get("table_info", {})
            if eq_table_info(ti, from_info):
                return ch
    return None


def find_child_op(cur: Dict[str, Any], op: str) -> Optional[Dict[str, Any]]:
    for ch in cur.get("children", []):
        if (not ch.get("is_terminal", False)) and (not ch.get("is_start", False)):
            if ch.get("operator_type", "") == op:
                return ch
    return None


def find_child_terminal(cur: Dict[str, Any], to_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    for ch in cur.get("children", []):
        if ch.get("is_terminal", False):
            ti = ch.get("table_info", {})
            if eq_table_info(ti, to_info):
                return ch
    return None


def pretty_table_info(ti: Dict[str, Any]) -> str:
    return f"(type={ti.get('type')}, assigned_shard={bool(ti.get('assigned_shard', False))}, joined_on_shard_key={bool(ti.get('joined_on_shard_key', False))})"


def inspect_path(
    roots: Dict[str, Any],
    qtype: Any,
    from_info: Dict[str, Any],
    ops: List[str],
    to_info: Dict[str, Any],
) -> None:
    key = str(qtype)
    root = roots.get(key)
    print(f"\n=== Inspect qtype={qtype} ===")
    if not root:
        print(f"[MISS] root not found for qtype={qtype}. Available keys: {sorted(set(roots.keys()))[:16]} ...")
        return

    path_nodes: List[Tuple[str, Dict[str, Any]]] = []  # (label, node)

    # 1) start
    start = find_child_start(root, from_info)
    if not start:
        print(f"[MISS] start node not found: from={pretty_table_info(from_info)}")
        # Optional hint: list available start nodes at the same level.
        cands = [
            pretty_table_info(ch.get("table_info", {}))
            for ch in root.get("children", [])
            if (not ch.get("is_terminal", False)) and ch.get("is_start", False)
        ]
        if cands:
            print("  available start candidates at this root:")
            for s in cands[:20]:
                print("   -", s)
        return
    path_nodes.append(("START " + pretty_table_info(from_info), start))

    # 2) ops
    cur = start
    for op in ops:
        nxt = find_child_op(cur, op)
        if not nxt:
            print(f"[MISS] op node not found after START: op='{op}'")
            # List available operators.
            cands = sorted(
                set(
                    ch.get("operator_type", "")
                    for ch in cur.get("children", [])
                    if (not ch.get("is_terminal", False)) and (not ch.get("is_start", False))
                )
            )
            if cands:
                print("  available ops from here:", ", ".join(cands[:20]))
            return
        path_nodes.append((f"OP '{op}'", nxt))
        cur = nxt

    # 3) terminal
    term = find_child_terminal(cur, to_info)
    if not term:
        print(f"[MISS] terminal node not found: to={pretty_table_info(to_info)}")
        # List available terminal nodes.
        cands = [
            pretty_table_info(ch.get("table_info", {}))
            for ch in cur.get("children", [])
            if ch.get("is_terminal", False)
        ]
        if cands:
            print("  available terminal candidates from here:")
            for s in cands[:20]:
                print("   -", s)
        return
    path_nodes.append(("TERMINAL " + pretty_table_info(to_info), term))

    # 4) Print counts for every node in the path.
    print("[HIT] path found. node stats:")
    for label, node in path_nodes:
        occ, iv, inv = get_counts(node)
        print(f"  - {label}: occurences={occ}, ivalid={iv}, invalidity={inv:.3f}")


def main() -> None:
    roots = load_roots(JSON_PATH)
    if not PATHS:
        print("Set PATHS at the top of this script to the path to inspect.")
        # Print loaded qtype keys as a reference.
        print("Loaded qtype keys:", sorted({k for k in roots.keys()})[:20], "...")
        return

    for item in PATHS:
        qtype = item["qtype"]
        from_info = item["from"]
        ops = item.get("ops", [])
        to_info = item["to"]
        inspect_path(roots, qtype, from_info, ops, to_info)


if __name__ == "__main__":
    main()
