#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Вариант №3 – Этапы 1–3 (русская версия вывода)
Простое CLI‑приложение без внешних библиотек:
  • Этап 1: читает конфиг CSV и печатает параметры (ключ=значение)
  • Этап 2: получает прямые зависимости (Alpine APKINDEX или тестовый файл)
  • Этап 3: строит транзитивный граф через итеративный DFS (без рекурсии), с глубиной и обработкой циклов
"""

import csv
import sys
import os
from urllib.request import urlopen
from urllib.error import URLError, HTTPError

# ---------- Вспомогательные функции ----------

def read_csv_config(path):
    """
    Формат CSV: по одной паре key,value на строку
    Возвращает dict
    """
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
    required = [
        "package_name",
        "repo_or_test_path",
        "mode",            # 'real' или 'test'
        "version",         # точная версия для этапа 2 (в режиме real)
        "max_depth"        # целое неотрицательное
    ]
    for k in required:
        if k not in p or p[k] == "":
            raise ValueError(f"Отсутствует обязательный параметр: {k}")

    if p["mode"] not in ("real", "test"):
        raise ValueError("mode должен быть 'real' или 'test'")

    try:
        md = int(p["max_depth"])
        if md < 0:
            raise ValueError
    except ValueError:
        raise ValueError("max_depth должен быть неотрицательным целым числом")

    if p["mode"] == "real":
        if not (p["repo_or_test_path"].startswith("http://") or p["repo_or_test_path"].startswith("https://")):
            raise ValueError("В режиме 'real' repo_or_test_path должен быть прямой URL на APKINDEX")
        if not p["repo_or_test_path"].lower().endswith("apkindex"):
            print("[ПРЕДУПРЕЖДЕНИЕ] URL не оканчивается на 'APKINDEX' — проверьте правильность.", file=sys.stderr)
    else:
        if not os.path.exists(p["repo_or_test_path"]):
            raise FileNotFoundError(f"Файл тестового графа не найден: {p['repo_or_test_path']}")


def download_text(url):
    try:
        with urlopen(url, timeout=20) as resp:
            data = resp.read()
        return data.decode('utf-8', errors='replace')
    except (URLError, HTTPError) as e:
        raise RuntimeError(f"Ошибка сети при загрузке {url}: {e}")


def parse_apkindex(text):
    """
    Мини‑парсер индекса Alpine APKINDEX.
    Нас интересуют строки:
      P:<имя пакета>
      V:<версия>
      D:<зависимости> (разделены пробелами/табами; версии отбрасываем)
    Возвращает: dict имя -> dict версия -> list[str] прямые зависимости
    """
    db = {}
    current = {}
    for line in text.splitlines():
        if not line.strip():
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
            current = {}
            continue
        if ":" in line:
            key, val = line.split(":", 1)
            key = key.strip()
            val = val.strip()
            if key in ("P", "V", "D"):
                current[key] = val
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
    return db


def get_direct_deps_real(apkindex_db, pkg, version):
    versions = apkindex_db.get(pkg, {})
    if not versions:
        raise KeyError(f"Пакет не найден в APKINDEX: {pkg}")
    if version not in versions:
        raise KeyError(f"Точная версия не найдена для {pkg}. Ожидалась: {version}. Доступны: {', '.join(versions.keys())}")
    return versions[version]


def read_test_graph(path):
    """
    Формат тестового файла:
      NODE:dep1,dep2,dep3
    Пример:
      A:B,C
      B:D
      C:D,E
      D:
    """
    graph = {}
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if ":" not in line:
                node = line
                graph.setdefault(node, [])
                continue
            node, rest = line.split(":", 1)
            node = node.strip()
            deps = [d.strip() for d in rest.split(",") if d.strip()] if rest.strip() else []
            graph[node] = deps
    return graph


def build_graph_iterative_dfs(start_pkg, max_depth, neighbors_func):
    """
    Итеративный DFS без рекурсии (через стек).
    neighbors_func(node) -> список зависимостей
    Учитывает max_depth, обрабатывает циклы.
    Возвращает (adjacency, order)
    """
    adj = {}
    visited = set()
    in_stack = set()
    order = []
    stack = [(start_pkg, 0, 0)]  # (узел, глубина, 0=pre 1=post)

    while stack:
        node, depth, state = stack.pop()
        if state == 0:
            if node in visited:
                continue
            if node in in_stack:
                # цикл — не разворачиваем дальше
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


# ---------- CLI‑поток ----------

def stage1_print_params(p):
    print("=== Этап 1: Параметры (ключ=значение) ===")
    for k in ("package_name","repo_or_test_path","mode","version","max_depth"):
        print(f"{k}={p[k]}")
    print()


def stage2_get_direct_deps(p):
    print("=== Этап 2: Прямые зависимости ===")
    pkg = p["package_name"]
    if p["mode"] == "test":
        graph = read_test_graph(p["repo_or_test_path"])
        deps = graph.get(pkg, [])
        print(f"{pkg}: {', '.join(deps) if deps else '(нет прямых зависимостей)'}")
        return lambda n: graph.get(n, [])
    else:
        apk_text = download_text(p["repo_or_test_path"])
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
        return neigh


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


def main():
    if len(sys.argv) < 2:
        print("Использование: python main.py путь/к/config.csv")
        sys.exit(1)
    cfg_path = sys.argv[1]
    try:
        params = read_csv_config(cfg_path)
        validate_params(params)
        stage1_print_params(params)               # Этап 1
        neigh = stage2_get_direct_deps(params)    # Этап 2
        stage3_build_graph(params, neigh)         # Этап 3
    except Exception as e:
        print(f"[ОШИБКА] {e}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
