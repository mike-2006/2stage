#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Variant #3 – Config Mgmt practicals, Stages 1–3
Simple CLI app (no external libs) that:
  • Stage 1: reads CSV config and prints key=val
  • Stage 2: fetches direct deps (Alpine APKINDEX format) OR from test file
  • Stage 3: builds transitive graph using iterative DFS (no recursion), with max depth and cycle handling
Author: You 👋
"""

import csv
import sys
import os
from urllib.request import urlopen
from urllib.error import URLError, HTTPError

# ---------- helpers ----------

def read_csv_config(path):
    """
    CSV format: key,value per line
    Returns dict
    """
    params = {}
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            if not row or len(row) < 2:
                # skip empty/bad lines silently
                continue
            key = row[0].strip()
            value = row[1].strip()
            if key:
                params[key] = value
    return params


def validate_params(p):
    required = [
        "package_name",
        "repo_or_test_path",
        "mode",            # 'real' or 'test'
        "version",         # specific version for Stage 2 (only for real mode)
        "max_depth"        # int >= 0
    ]
    for k in required:
        if k not in p or p[k] == "":
            raise ValueError(f"Missing required parameter: {k}")

    if p["mode"] not in ("real", "test"):
        raise ValueError("mode must be 'real' or 'test'")

    # validate max_depth
    try:
        md = int(p["max_depth"])
        if md < 0:
            raise ValueError
    except ValueError:
        raise ValueError("max_depth must be a non-negative integer")

    # minimal check for URL/file
    if p["mode"] == "real":
        # for simplicity, expect direct URL to APKINDEX
        if not (p["repo_or_test_path"].startswith("http://") or p["repo_or_test_path"].startswith("https://")):
            raise ValueError("In 'real' mode, repo_or_test_path must be a direct URL to APKINDEX")
        if not p["repo_or_test_path"].lower().endswith("apkindex"):
            # not strict, but warn
            print("[WARN] URL does not end with 'APKINDEX' - make sure it points to APKINDEX", file=sys.stderr)
    else:
        # test mode expects a local text file
        if not os.path.exists(p["repo_or_test_path"]):
            raise FileNotFoundError(f"Test graph file not found: {p['repo_or_test_path']}")


# ---------- Stage 2 data collection ----------

def download_text(url):
    try:
        with urlopen(url, timeout=20) as resp:
            data = resp.read()
        return data.decode('utf-8', errors='replace')
    except (URLError, HTTPError) as e:
        raise RuntimeError(f"Network error while fetching {url}: {e}")


def parse_apkindex(text):
    """
    Very small parser for Alpine APKINDEX-like content.
    We only care about fields:
      P:<name>
      V:<version>
      D:<space or tab separated deps, optional; deps may include version constraints, we drop them>
    Returns: dict name -> dict version -> list[str] direct deps (names only)
    """
    db = {}
    current = {}
    for line in text.splitlines():
        if not line.strip():
            # end of a block
            if "P" in current and "V" in current:
                name = current.get("P")
                ver = current.get("V")
                deps_line = current.get("D", "")
                deps = []
                if deps_line:
                    raw = deps_line.replace(",", " ").split()
                    # deps may look like "so:libssl3>=3.3" or "busybox>=1.36-r0"
                    # keep only alpha/-,._+ chars before first comparator
                    for token in raw:
                        # drop virtual/soname deps starting with "so:"
                        if token.startswith("so:"):
                            continue
                        dep = token.split("<")[0].split(">")[0].split("=")[0]
                        dep = dep.strip()
                        if dep:
                            deps.append(dep)
                db.setdefault(name, {}).setdefault(ver, deps)
            current = {}
            continue
        if ":" in line:
            key, val = line.split(":", 1)
            key = key.strip()
            val = val.strip()
            if key in ("P", "V", "D"):
                current[key] = val
    # flush last
    if "P" in current and "V" in current:
        name = current.get("P")
        ver = current.get("V")
        deps_line = current.get("D", "")
        deps = []
        if deps_line:
            raw = deps_line.replace(",", " ").split()
            for token in raw:
                if token.startswith("so:"):
                    continue
                dep = token.split("<")[0].split(">")[0].split("=")[0]
                dep = dep.strip()
                if dep:
                    deps.append(dep)
        db.setdefault(name, {}).setdefault(ver, deps)
    return db


def get_direct_deps_real(apkindex_db, pkg, version):
    """
    Return direct deps list for package 'pkg' at exact 'version'.
    If exact version not found -> raise error (as per assignment for Stage 2).
    """
    versions = apkindex_db.get(pkg, {})
    if not versions:
        raise KeyError(f"Package not found in APKINDEX: {pkg}")
    if version not in versions:
        raise KeyError(f"Exact version not found for {pkg}. Expected: {version}. Available: {', '.join(versions.keys())}")
    return versions[version]


def read_test_graph(path):
    """
    Test file format (very simple):
      Each non-empty line: NODE:dep1,dep2,dep3
      Example:
        A:B,C
        B:D
        C:D,E
        D:
    Returns dict node -> list[str] deps
    """
    graph = {}
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if ":" not in line:
                # allow node with no deps if line is just "X"
                node = line
                graph.setdefault(node, [])
                continue
            node, rest = line.split(":", 1)
            node = node.strip()
            deps = [d.strip() for d in rest.split(",") if d.strip()] if rest.strip() else []
            graph[node] = deps
    return graph


# ---------- Stage 3: Build graph via iterative DFS (no recursion) ----------

def build_graph_iterative_dfs(start_pkg, max_depth, neighbors_func):
    """
    neighbors_func(node) -> list of deps
    DFS (stack) without recursion
    Handles cycles (skips nodes already fully processed)
    Respects max_depth (0 => only node; 1 => node + its direct deps; ...)
    Returns adjacency dict and traversal order
    """
    adj = {}
    visited = set()       # fully processed
    in_stack = set()      # nodes currently in path (for cycle detection)
    order = []

    # stack holds tuples (node, depth, state)
    # state: 0 = pre-visit (push children), 1 = post-visit (mark visited)
    stack = [(start_pkg, 0, 0)]

    while stack:
        node, depth, state = stack.pop()
        if state == 0:
            if node in visited:
                continue
            if node in in_stack:
                # cycle detected, skip further expansion
                continue
            in_stack.add(node)
            order.append(node)

            # prepare for post-visit
            stack.append((node, depth, 1))

            # fetch neighbors (respect depth)
            try:
                neigh = neighbors_func(node) if depth < max_depth else []
            except KeyError:
                neigh = []
            adj.setdefault(node, [])
            for nb in neigh:
                adj[node].append(nb)
                if nb not in visited:
                    stack.append((nb, depth + 1, 0))
        else:
            # post-visit
            in_stack.discard(node)
            visited.add(node)

    return adj, order


# ---------- CLI flow ----------

def stage1_print_params(p):
    print("=== Stage 1: Parameters (key=value) ===")
    for k in ("package_name","repo_or_test_path","mode","version","max_depth"):
        print(f"{k}={p[k]}")
    print()


def stage2_get_direct_deps(p):
    print("=== Stage 2: Direct dependencies ===")
    pkg = p["package_name"]
    if p["mode"] == "test":
        graph = read_test_graph(p["repo_or_test_path"])
        deps = graph.get(pkg, [])
        print(f"{pkg}: {', '.join(deps) if deps else '(no direct deps)'}")
        return lambda n: graph.get(n, [])
    else:
        apk_text = download_text(p["repo_or_test_path"])
        db = parse_apkindex(apk_text)
        deps = get_direct_deps_real(db, pkg, p["version"])
        print(f"{pkg} ({p['version']}): {', '.join(deps) if deps else '(no direct deps)'}")

        def neigh(n):
            # for neighbors we try exact version if this is the root, otherwise pick any newest available
            if n == pkg:
                return deps
            # try to pick "first" available version (simple strategy)
            versions = db.get(n, {})
            if not versions:
                raise KeyError(f"Package not found in APKINDEX: {n}")
            # pick an arbitrary version deterministically (sorted max)
            ver = sorted(versions.keys())[-1]
            return versions[ver]
        return neigh


def stage3_build_graph(p, neighbors_func):
    print("\n=== Stage 3: Full graph (DFS iterative, max_depth) ===")
    max_depth = int(p["max_depth"])
    start = p["package_name"]
    adj, order = build_graph_iterative_dfs(start, max_depth, neighbors_func)

    print(f"Traversal order: {', '.join(order)}")
    print("Adjacency list:")
    for node in adj:
        deps = adj[node]
        print(f"  {node} -> {', '.join(deps) if deps else '∅'}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python main.py path/to/config.csv")
        sys.exit(1)
    cfg_path = sys.argv[1]
    try:
        params = read_csv_config(cfg_path)
        validate_params(params)
        stage1_print_params(params)               # Stage 1
        neigh = stage2_get_direct_deps(params)    # Stage 2
        stage3_build_graph(params, neigh)         # Stage 3
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
