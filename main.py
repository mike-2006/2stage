#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import sys
import os
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
import math

# =========================
# Этап 1. ЧТЕНИЕ КОНФИГА
# =========================

def read_config(path):
    if not os.path.exists(path):
        print(f"[ОШИБКА] Файл конфигурации не найден: {path}")
        sys.exit(1)

    params = {}
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 2:
                continue
            key = row[0].strip()
            value = row[1].strip()
            if key != "":
                params[key] = value
    return params


def validate_config(params):
    required = ["package_name", "repo_or_test_path", "mode", "version", "max_depth"]
    for name in required:
        if name not in params or params[name] == "":
            print(f"[ОШИБКА] Нет обязательного параметра: {name}")
            sys.exit(1)

    mode = params["mode"]
    if mode not in ("test", "real"):
        print("[ОШИБКА] mode должен быть 'test' или 'real'")
        sys.exit(1)

    try:
        depth = int(params["max_depth"])
        if depth < 0:
            raise ValueError()
    except ValueError:
        print("[ОШИБКА] max_depth должно быть неотрицательным целым числом")
        sys.exit(1)

    if mode == "test":
        path = params["repo_or_test_path"]
        if not os.path.exists(path):
            print(f"[ОШИБКА] Файл тестового графа не найден: {path}")
            sys.exit(1)


def print_params(params):
    print("=== Этап 1: Параметры (ключ=значение) ===")
    for key, value in params.items():
        print(f"{key}={value}")
    print()


# =========================
# Этап 2. ПРЯМЫЕ ЗАВИСИМОСТИ
# =========================

def load_text_from_url(url):
    """
    Простой загрузчик, который поддерживает:
    - APKINDEX.tar.gz (нужно распаковать)
    - APKINDEX (обычный текст)
    """
    import gzip
    import tarfile
    import io

    try:
        with urlopen(url, timeout=20) as resp:
            data = resp.read()
    except Exception as e:
        print(f"[ОШИБКА] не удалось загрузить {url}: {e}")
        sys.exit(1)

    url_low = url.lower()

    # ----- если это tar.gz -----
    if url_low.endswith(".tar.gz"):
        try:
            bio = io.BytesIO(data)
            with tarfile.open(fileobj=bio, mode="r:gz") as tar:
                for member in tar.getmembers():
                    if member.name.endswith("APKINDEX"):
                        f = tar.extractfile(member)
                        text = f.read().decode("utf-8", errors="replace")
                        return text
            print("[ОШИБКА] В APKINDEX.tar.gz не найден файл APKINDEX")
            sys.exit(1)
        except Exception as e:
            print(f"[ОШИБКА] ошибка при распаковке tar.gz: {e}")
            sys.exit(1)

    # ----- если это обычный APKINDEX -----
    try:
        return data.decode("utf-8", errors="replace")
    except:
        # может быть gzip без tar
        try:
            decoded = gzip.decompress(data).decode("utf-8", errors="replace")
            return decoded
        except:
            print("[ОШИБКА] Невозможно прочитать APKINDEX")
            sys.exit(1)



def parse_apkindex(text):
    packages = {}
    current_name = None
    current_version = None
    current_deps = []

    for line in text.splitlines():
        line = line.strip()
        if line == "":
            if current_name is not None and current_version is not None:
                if current_name not in packages:
                    packages[current_name] = {}
                packages[current_name][current_version] = current_deps
            current_name = None
            current_version = None
            current_deps = []
            continue

        if line.startswith("P:"):
            current_name = line[2:].strip()
        elif line.startswith("V:"):
            current_version = line[2:].strip()
        elif line.startswith("D:"):
            deps_line = line[2:].strip()
            if deps_line == "":
                current_deps = []
            else:
                parts = deps_line.replace(",", " ").split()
                deps = []
                for item in parts:
                    if item.startswith("so:"):
                        continue
                    cut = item.split("<")[0]
                    cut = cut.split(">")[0]
                    cut = cut.split("=")[0]
                    dep_name = cut.strip()
                    if dep_name != "":
                        deps.append(dep_name)
                current_deps = deps

    if current_name is not None and current_version is not None:
        if current_name not in packages:
            packages[current_name] = {}
        packages[current_name][current_version] = current_deps

    return packages


def read_test_graph(path):
    graph = {}
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if line == "" or line.startswith("#"):
                continue
            if ":" in line:
                name, deps_str = line.split(":", 1)
                name = name.strip()
                if deps_str.strip() == "":
                    deps = []
                else:
                    deps = [x.strip() for x in deps_str.split(",") if x.strip() != ""]
                graph[name] = deps
            else:
                graph[line] = []
    return graph


def build_package_deps_real(packages, root_name, root_version):
    if root_name not in packages:
        print(f"[ОШИБКА] Пакет {root_name} не найден в APKINDEX")
        sys.exit(1)
    if root_version not in packages[root_name]:
        print(f"[ОШИБКА] У пакета {root_name} нет версии {root_version}")
        print("Доступные версии:", ", ".join(packages[root_name].keys()))
        sys.exit(1)

    deps = {}
    deps[root_name] = packages[root_name][root_version]

    for name, ver_map in packages.items():
        if name == root_name:
            continue
        all_versions = sorted(ver_map.keys())
        chosen_version = all_versions[-1]
        deps[name] = ver_map[chosen_version]

    return deps


def stage2_get_direct_deps(params):
    print("=== Этап 2: Прямые зависимости ===")
    mode = params["mode"]
    root = params["package_name"]

    if mode == "test":
        graph = read_test_graph(params["repo_or_test_path"])
        direct = graph.get(root, [])
        if direct:
            print(f"{root}: {', '.join(direct)}")
        else:
            print(f"{root}: (нет прямых зависимостей)")
        return graph

    url = params["repo_or_test_path"]
    text = load_text_from_url(url)
    packages = parse_apkindex(text)
    root_version = params["version"]
    deps = build_package_deps_real(packages, root, root_version)
    direct = deps.get(root, [])
    if direct:
        print(f"{root} ({root_version}): {', '.join(direct)}")
    else:
        print(f"{root} ({root_version}): (нет прямых зависимостей)")
    return deps


# =========================
# Этап 3. ГРАФ ЗАВИСИМОСТЕЙ
# =========================

def build_forward_graph(params, deps):
    print("\n=== Этап 3: Полный граф (итеративный DFS) ===")
    start = params["package_name"]
    max_depth = int(params["max_depth"])
    skip_substring = params.get("skip_substring", "")

    adjacency = {}
    visited = set()
    stack = [(start, 0)]

    while stack:
        current, depth = stack.pop()
        if current in visited:
            continue

        if skip_substring != "" and skip_substring in current:
            visited.add(current)
            continue

        visited.add(current)
        neighbors = deps.get(current, [])
        adjacency[current] = neighbors

        if depth >= max_depth:
            continue

        for name in neighbors:
            if name not in visited:
                stack.append((name, depth + 1))

    print("Список смежности (прямой граф):")
    for name in adjacency:
        neigh = adjacency[name]
        line = ", ".join(neigh) if neigh else "∅"
        print(f"  {name} -> {line}")

    return adjacency


# =========================
# Этап 4. ОБРАТНЫЕ ЗАВИСИМОСТИ
# =========================

def build_reverse_graph(params, forward_adj):
    print("\n=== Этап 4: Обратные зависимости (кто зависит от пакета) ===")

    # строим обратное отображение
    reverse_adj = {}
    for src in forward_adj:
        if src not in reverse_adj:
            reverse_adj[src] = []
        for dst in forward_adj[src]:
            if dst not in reverse_adj:
                reverse_adj[dst] = []
            if src not in reverse_adj[dst]:
                reverse_adj[dst].append(src)

    start = params["package_name"]
    max_depth = int(params["max_depth"])
    skip_substring = params.get("skip_substring", "")

    result_adj = {}
    visited = set()
    stack = [(start, 0)]

    while stack:
        current, depth = stack.pop()
        if current in visited:
            continue

        if skip_substring != "" and skip_substring in current:
            visited.add(current)
            continue

        visited.add(current)
        neighbors = reverse_adj.get(current, [])
        result_adj[current] = neighbors

        if depth >= max_depth:
            continue

        for name in neighbors:
            if name not in visited:
                stack.append((name, depth + 1))

    print("Список смежности (обратный граф):")
    for name in result_adj:
        neigh = result_adj[name]
        line = ", ".join(neigh) if neigh else "∅"
        print(f"  {name} <- {line}")

    return result_adj


# =========================
# Этап 5. КАРТИНКА ГРАФА (PNG)
# =========================

def draw_graph_png(adjacency, filename):
    """
    Рисуем простой граф зависимостей:
    каждый пакет в отдельной строке (по вертикали),
    зависимости стрелками вниз.
    Подходит для цепочек и небольших графов.
    """
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("[ПРЕДУПРЕЖДЕНИЕ] matplotlib не установлен, картинка не будет создана")
        return

    # Собираем список всех узлов (и ключи, и их зависимости)
    nodes = []
    for src in adjacency:
        if src not in nodes:
            nodes.append(src)
        for dst in adjacency[src]:
            if dst not in nodes:
                nodes.append(dst)

    if not nodes:
        print("[ПРЕДУПРЕЖДЕНИЕ] Пустой граф, нечего рисовать")
        return

    # Располагаем узлы по вертикали: x = 0, y = -i
    positions = {}
    for i, name in enumerate(nodes):
        x = 0.0
        y = -i
        positions[name] = (x, y)

    # Готовим рисунок
    fig, ax = plt.subplots(figsize=(4, max(3, len(nodes))))  # высота зависит от числа узлов
    ax.set_aspect("equal")
    ax.axis("off")

    # Рёбра (просто стрелки вниз)
    for src in adjacency:
        x1, y1 = positions[src]
        for dst in adjacency[src]:
            x2, y2 = positions.get(dst, (0.0, 0.0))
            ax.annotate(
                "", xy=(x2, y2 + 0.2), xytext=(x1, y1 - 0.2),
                arrowprops=dict(arrowstyle="->", linewidth=1)
            )

    # Узлы (точки + подписи)
    for name in nodes:
        x, y = positions[name]
        ax.scatter([x], [y], s=50)
        ax.text(
            x, y, name,
            fontsize=10,
            ha="center", va="center",
            bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="black", lw=0.5)
        )

    plt.tight_layout()
    plt.savefig(filename, dpi=150)
    plt.close(fig)
    print(f"Картинка графа сохранена в файл: {filename}")


# =========================
# MAIN
# =========================

def main():
    if len(sys.argv) < 2:
        print("Использование: python main.py config.csv")
        sys.exit(1)

    config_path = sys.argv[1]
    params = read_config(config_path)
    validate_config(params)
    print_params(params)                    # Этап 1

    deps = stage2_get_direct_deps(params)   # Этап 2
    forward_adj = build_forward_graph(params, deps)   # Этап 3
    reverse_adj = build_reverse_graph(params, forward_adj)  # Этап 4

    # Этап 5: картинка. Можно выбрать, что рисовать: прямой или обратный граф.
    # Для примера рисуем прямой граф:
    draw_graph_png(forward_adj, "graph_forward.png")
    # И, при желании, обратный:
    draw_graph_png(reverse_adj, "graph_reverse.png")


if __name__ == "__main__":
    main()
# End of main.py