#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Вариант №3 – Этапы 1–5 (RU)
— Этап 1: печать параметров из CSV
— Этап 2: прямые зависимости (APKINDEX(.tar.gz) или тестовый файл)
— Этап 3: полный граф (итеративный DFS, без рекурсии), max_depth, циклы, skip_substring
— Этап 4: обратные зависимости тем же DFS
— Этап 5: визуализация:
    • Mermaid: graph.mmd + graph.html (авто-открытие)
    • Python (matplotlib): graph.png + graph.pdf  ← то, что требует препод
"""

import csv, sys, os, io, tarfile, gzip, webbrowser
from urllib.request import urlopen
from urllib.error import URLError, HTTPError

# ---------- CSV / валидация ----------

def read_csv_config(path):
    params = {}
    if not os.path.exists(path):
        raise FileNotFoundError(f"Файл конфигурации не найден: {path}")
    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            if not row or len(row) < 2:
                continue
            key = row[0].strip()
            value = row[1].strip()
            if key:
                params[key] = value
    return params

def validate_params(p):
    required = ["package_name","repo_or_test_path","mode","version","max_depth"]
    for k in required:
        if k not in p or p[k] == "":
            raise ValueError(f"Отсутствует обязательный параметр: {k}")
    if p["mode"] not in ("real","test"):
        raise ValueError("mode должен быть 'real' или 'test'")
    try:
        md = int(p["max_depth"])
        if md < 0:
            raise ValueError
    except ValueError:
        raise ValueError("max_depth должен быть неотрицательным целым числом")
    # мягкое предупреждение по URL
    if p["mode"] == "real":
        url = p["repo_or_test_path"].lower()
        if not (url.startswith("http://") or url.startswith("https://")):
            raise ValueError("В 'real' repo_or_test_path должен быть URL")
        if not (url.endswith("apkindex") or url.endswith("apkindex.tar.gz")):
            print("[ПРЕДУПРЕЖДЕНИЕ] URL лучше указывать на APKINDEX или APKINDEX.tar.gz", file=sys.stderr)
    else:
        if not os.path.exists(p["repo_or_test_path"]):
            raise FileNotFoundError(f"Файл тестового графа не найден: {p['repo_or_test_path']}")

# ---------- Загрузка индекса (поддержка APKINDEX и APKINDEX.tar.gz) ----------

def download_apkindex_text(url):
    """
    Поддерживает:
      - .../APKINDEX              (плоский текст, иногда gzip)
      - .../APKINDEX.tar.gz       (tar.gz, внутри файл APKINDEX)
    Возвращает str с содержимым APKINDEX.
    """
    try:
        with urlopen(url, timeout=30) as resp:
            data = resp.read()
    except (URLError, HTTPError) as e:
        raise RuntimeError(f"Ошибка сети при загрузке {url}: {e}")

    low = url.lower()
    if low.endswith(".tar.gz"):
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tf:
            member = next((m for m in tf.getmembers() if m.name.endswith("APKINDEX")), None)
            if not member:
                raise RuntimeError("В APKINDEX.tar.gz файл APKINDEX не найден")
            with tf.extractfile(member) as f:
                return f.read().decode("utf-8", errors="replace")
    else:
        # либо чистый текст, либо просто gzip
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            try:
                return gzip.decompress(data).decode("utf-8", errors="replace")
            except Exception:
                raise RuntimeError("Не удалось распаковать APKINDEX: формат не распознан")

# ---------- Парсинг APKINDEX ----------

def parse_apkindex(text):
    """
    Мини-парсер APKINDEX:
      P:<имя>, V:<версия>, D:<deps>
    Возвращает: dict name -> dict version -> list[str] прямых зависимостей
    """
    db = {}
    current = {}
    def flush():
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
                    dep = token.split("<")[0].split(">")[0].split("=")[0].strip()
                    if dep:
                        deps.append(dep)
            db.setdefault(name, {}).setdefault(ver, deps)

    for line in text.splitlines():
        if not line.strip():
            flush(); current = {}
            continue
        if ":" in line:
            key, val = line.split(":", 1)
            key = key.strip(); val = val.strip()
            if key in ("P","V","D"):
                current[key] = val
    flush()
    return db

def get_direct_deps_real(apkindex_db, pkg, version):
    versions = apkindex_db.get(pkg, {})
    if not versions:
        raise KeyError(f"Пакет не найден в APKINDEX: {pkg}")
    if version not in versions:
        raise KeyError(f"Точная версия не найдена для {pkg}. Ожидалась: {version}. Доступны: {', '.join(versions.keys())}")
    return versions[version]

# ---------- Тестовый граф ----------

def read_test_graph(path):
    graph = {}
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if ":" not in line:
                graph.setdefault(line, [])
                continue
            node, rest = line.split(":", 1)
            node = node.strip()
            deps = [d.strip() for d in rest.split(",") if d.strip()] if rest.strip() else []
            graph[node] = deps
    return graph

# ---------- Этап 3: итеративный DFS + фильтр подстроки ----------

def build_graph_iterative_dfs(start_pkg, max_depth, neighbors_func):
    adj = {}
    visited = set()
    in_stack = set()
    order = []
    stack = [(start_pkg, 0, 0)]  # (node, depth, state) state 0=pre,1=post

    while stack:
        node, depth, state = stack.pop()
        if state == 0:
            if node in visited or node in in_stack:
                continue
            in_stack.add(node)
            order.append(node)
            stack.append((node, depth, 1))
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
            in_stack.discard(node)
            visited.add(node)
    return adj, order

def make_filtered_neighbors(base_neighbors, skip_substring):
    if not skip_substring:
        return base_neighbors
    def wrapper(node):
        return [nb for nb in base_neighbors(node) if skip_substring not in nb]
    return wrapper

# ---------- Этап 5: Mermaid ----------

def mermaid_from_adj(adj):
    lines = ["graph TD"]
    seen = set()
    for src, deps in adj.items():
        if not deps:
            lines.append(f"    {src}")
        for dst in deps:
            e = (src, dst)
            if e in seen: continue
            seen.add(e)
            lines.append(f"    {src} --> {dst}")
    return "\n".join(lines)

def write_mermaid_files(adj, out_dir="."):
    mmd = mermaid_from_adj(adj)
    mmd_path = os.path.join(out_dir, "graph.mmd")
    with open(mmd_path, "w", encoding="utf-8") as f:
        f.write(mmd)
    html = f"""<!DOCTYPE html>
<html lang="ru"><head><meta charset="utf-8"><title>Граф зависимостей</title></head>
<body>
  <pre class="mermaid">
{mmd}
  </pre>
  <script type="module">
    import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs";
    mermaid.initialize({{ startOnLoad: true }});
  </script>
</body></html>"""
    html_path = os.path.join(out_dir, "graph.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    return mmd_path, html_path

# ---------- Этап 5: Картинки через matplotlib ----------

def _compute_layers(adj, roots):
    from collections import deque, defaultdict
    layer = {}
    q = deque()
    for r in roots:
        layer[r] = 0
        q.append(r)
    while q:
        u = q.popleft()
        for v in adj.get(u, []):
            if v not in layer:
                layer[v] = layer[u] + 1
                q.append(v)
    for u in adj.keys():
        if u not in layer:
            layer[u] = 0
    return layer

def save_graph_images_matplotlib(adj, roots, out_png="graph.png", out_pdf="graph.pdf"):
    try:
        import matplotlib.pyplot as plt
        from matplotlib.patches import FancyArrowPatch, Circle
    except Exception as e:
        print(f"[ПРЕДУПРЕЖДЕНИЕ] matplotlib недоступен: {e}")
        return None, None

    layer = _compute_layers(adj, roots)
    # группируем по слоям
    layers = {}
    for n, l in layer.items():
        layers.setdefault(l, []).append(n)

    # координаты
    pos = {}
    for l, nodes in layers.items():
        nodes_sorted = sorted(nodes)
        k = len(nodes_sorted)
        xs = [0.0] if k == 1 else [i/(k-1) for i in range(k)]
        y = -float(l)
        for i, n in enumerate(nodes_sorted):
            pos[n] = (xs[i], y)

    fig = plt.figure(figsize=(8, 6))
    ax = fig.add_subplot(111)
    ax.set_axis_off()

    # рёбра
    for u, nbrs in adj.items():
        x1, y1 = pos.get(u, (0, 0))
        for v in nbrs:
            x2, y2 = pos.get(v, (0, 0))
            if (x1, y1) == (x2, y2):
                continue
            arrow = FancyArrowPatch((x1, y1), (x2, y2), arrowstyle='-|>', mutation_scale=10, lw=1)
            ax.add_patch(arrow)

    # узлы
    for n, (x, y) in pos.items():
        circ = Circle((x, y), 0.03)
        ax.add_patch(circ)
        ax.text(x, y+0.06, n, ha='center', va='center')

    ax.set_xlim(-0.1, 1.1)
    min_layer = min(layers.keys()) if layers else 0
    max_layer = max(layers.keys()) if layers else 0
    ax.set_ylim(-max_layer-1, -min_layer+1)

    fig.tight_layout()
    try:
        fig.savefig(out_png, dpi=150)
        fig.savefig(out_pdf)
        plt.close(fig)
        return out_png, out_pdf
    except Exception as e:
        print(f"[ПРЕДУПРЕЖДЕНИЕ] не удалось сохранить изображения: {e}")
        plt.close(fig)
        return None, None

# ---------- CLI-поток ----------

def stage1_print_params(p):
    print("=== Этап 1: Параметры (ключ=значение) ===")
    for k in ("package_name","repo_or_test_path","mode","version","max_depth"):
        print(f"{k}={p[k]}")
    if p.get("skip_substring"):
        print(f"skip_substring={p['skip_substring']}")
    if p.get("visualize_mode"):
        print(f"visualize_mode={p['visualize_mode']}")
    print()

def stage2_get_direct_deps(p):
    print("=== Этап 2: Прямые зависимости ===")
    pkg = p["package_name"]
    if p["mode"] == "test":
        graph = read_test_graph(p["repo_or_test_path"])
        deps = graph.get(pkg, [])
        print(f"{pkg}: {', '.join(deps) if deps else '(нет прямых зависимостей)'}")
        base = (lambda n: graph.get(n, []))
        return make_filtered_neighbors(base, p.get("skip_substring",""))
    else:
        apk_text = download_apkindex_text(p["repo_or_test_path"])
        db = parse_apkindex(apk_text)
        deps = get_direct_deps_real(db, pkg, p["version"])
        print(f"{pkg} ({p['version']}): {', '.join(deps) if deps else '(нет прямых зависимостей)'}")
        def neigh(n):
            if n == pkg:
                return deps
            versions = db.get(n, {})
            if not versions:
                raise KeyError(f"Пакет не найден в APKINDEX: {n}")
            ver = sorted(versions.keys())[-1]  # простейший выбор версии
            return versions[ver]
        return make_filtered_neighbors(neigh, p.get("skip_substring",""))

def stage3_build_graph(p, neighbors_func):
    print("\n=== Этап 3: Полный граф (итеративный DFS, max_depth) ===")
    max_depth = int(p["max_depth"])
    start = p["package_name"]
    adj, order = build_graph_iterative_dfs(start, max_depth, neighbors_func)
    print(f"Порядок обхода: {', '.join(order)}")
    print("Список смежности:")
    for node in adj:
        deps = adj[node]
        print(f"  {node} -> {', '.join(deps) if deps else '∅'}")
    return adj

def stage4_reverse_dependencies(p, forward_adj):
    print("\n=== Этап 4: Обратные зависимости (кто зависит от пакета) ===")
    rev = {}
    for src, deps in forward_adj.items():
        rev.setdefault(src, [])
        for d in deps:
            rev.setdefault(d, [])
            if src not in rev[d]:
                rev[d].append(src)
    start = p["package_name"]
    max_depth = int(p["max_depth"])
    def rev_neighbors(n): return rev.get(n, [])
    rev_adj, order = build_graph_iterative_dfs(start, max_depth, rev_neighbors)
    print(f"Порядок обхода (обратный): {', '.join(order)}")
    print("Обратная смежность (зависящие):")
    for node in rev_adj:
        deps = rev_adj[node]
        print(f"  {node} <- {', '.join(deps) if deps else '∅'}")
    return rev_adj

def stage5_visualize(adj):
    print("\n=== Этап 5: Визуализация (Mermaid + Python) ===")
    mmd_path, html_path = write_mermaid_files(adj, out_dir=".")
    print(f"Mermaid-текст сохранён в: {mmd_path}")
    print(f"HTML с диаграммой: {html_path}")

    # PNG и PDF средствами matplotlib (полноценные «картинки графов»)
    incoming = {n: 0 for n in adj.keys()}
    for u, nbrs in adj.items():
        for v in nbrs:
            incoming.setdefault(v, 0); incoming[v] += 1
    roots = [n for n, deg in incoming.items() if deg == 0] or list(adj.keys())[:1]
    png_path, pdf_path = save_graph_images_matplotlib(adj, roots, out_png="graph.png", out_pdf="graph.pdf")
    if png_path and pdf_path:
        print(f"PNG сохранён: {png_path}")
        print(f"PDF сохранён: {pdf_path}")

    try:
        webbrowser.open(os.path.abspath(html_path))
    except Exception:
        pass

def main():
    if len(sys.argv) < 2:
        print("Использование: python main.py путь/к/config.csv")
        sys.exit(1)
    cfg_path = sys.argv[1]
    try:
        p = read_csv_config(cfg_path)
        validate_params(p)
        stage1_print_params(p)
        neigh = stage2_get_direct_deps(p)
        adj = stage3_build_graph(p, neigh)
        rev = stage4_reverse_dependencies(p, adj)
        to_draw = adj if p.get("visualize_mode","forward").lower() != "reverse" else rev
        stage5_visualize(to_draw)
    except Exception as e:
        print(f"[ОШИБКА] {e}", file=sys.stderr)
        sys.exit(2)

if __name__ == "__main__":
    main()
