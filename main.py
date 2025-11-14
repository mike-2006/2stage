#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import sys
import os
from urllib.request import urlopen
from urllib.error import URLError, HTTPError

# =========================
# Этап 1. ЧТЕНИЕ КОНФИГА
# =========================

def read_config(path):
    """Читаем config.csv в словарь key -> value."""
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
    """Проверяем обязательные параметры и режим."""
    required = ["package_name", "repo_or_test_path", "mode", "version", "max_depth"]
    for name in required:
        if name not in params or params[name] == "":
            print(f"[ОШИБКА] Нет обязательного параметра: {name}")
            sys.exit(1)

    mode = params["mode"]
    if mode not in ("test", "real"):
        print("[ОШИБКА] mode должен быть 'test' или 'real'")
        sys.exit(1)

    # max_depth должно быть целым неотрицательным
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
    # В режиме real просто считаем, что URL корректный, проверка будет при загрузке


def print_params(params):
    """Этап 1: выводим параметры key=value."""
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
    """
    Простой парсер APKINDEX.
    Нас интересуют только:
    P:имя
    V:версия
    D:зависимости (строка)
    Возвращаем словарь:
      packages[name][version] = список_зависимостей
    """
    packages = {}
    current_name = None
    current_version = None
    current_deps = []

    for line in text.splitlines():
        line = line.strip()
        if line == "":
            # конец записи
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
                # разделяем по пробелам и убираем версии / so:
                parts = deps_line.replace(",", " ").split()
                deps = []
                for item in parts:
                    if item.startswith("so:"):
                        continue
                    # отбрасываем всё после знаков сравнения и '='
                    cut = item.split("<")[0]
                    cut = cut.split(">")[0]
                    cut = cut.split("=")[0]
                    dep_name = cut.strip()
                    if dep_name != "":
                        deps.append(dep_name)
                current_deps = deps

    # на случай, если запись не завершилась пустой строкой
    if current_name is not None and current_version is not None:
        if current_name not in packages:
            packages[current_name] = {}
        packages[current_name][current_version] = current_deps

    return packages


def read_test_graph(path):
    """
    Формат файла test_graph.txt:
      A:B,C
      B:D
      C:D,E
      D:
    Возвращаем dict: имя -> список зависимостей
    """
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
    """
    Преобразуем структуру packages[name][version] в
    более простой словарь: deps[name] = список зависимостей.
    Для корневого пакета используем именно root_version (если нет — ошибка).
    Для остальных пакетов берём "какую-нибудь" версию (максимальную по строке).
    """
    if root_name not in packages:
        print(f"[ОШИБКА] Пакет {root_name} не найден в APKINDEX")
        sys.exit(1)

    versions = packages[root_name]
    if root_version not in versions:
        print(f"[ОШИБКА] У пакета {root_name} нет версии {root_version}")
        print("Доступные версии:", ", ".join(versions.keys()))
        sys.exit(1)

    deps = {}
    # сначала корневой пакет
    deps[root_name] = versions[root_version]

    # остальные пакеты
    for name, ver_map in packages.items():
        if name == root_name:
            continue
        # берём "максимальную" версию по строке
        all_versions = sorted(ver_map.keys())
        chosen_version = all_versions[-1]
        deps[name] = ver_map[chosen_version]

    return deps


def stage2_get_direct_deps(params):
    """Этап 2: печатаем прямые зависимости и возвращаем словарь deps[name] = [..]."""
    print("=== Этап 2: Прямые зависимости ===")
    mode = params["mode"]
    root = params["package_name"]

    if mode == "test":
        graph = read_test_graph(params["repo_or_test_path"])
        if root in graph:
            direct = graph[root]
        else:
            direct = []
        print(f"{root}: {', '.join(direct) if direct else '(нет прямых зависимостей)'}")
        return graph

    # mode == real
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
# Этап 3. ГРАФ (DFS БЕЗ РЕКУРСИИ)
# =========================

def stage3_build_graph(params, deps):
    """Этап 3: итеративный обход в глубину и построение списка смежности."""
    print("\n=== Этап 3: Полный граф зависимостей (итеративный DFS) ===")

    start = params["package_name"]
    max_depth = int(params["max_depth"])
    skip_substring = params.get("skip_substring", "")

    adjacency = {}       # node -> list of neighbours
    visited = set()      # уже полностью обработанные узлы
    stack = []           # стек для DFS: элементы (имя_пакета, текущая_глубина)

    stack.append((start, 0))

    while stack:
        current, depth = stack.pop()
        if current in visited:
            continue

        # фильтр по подстроке (если задан)
        if skip_substring != "" and skip_substring in current:
            # просто не разворачиваем такой узел
            visited.add(current)
            continue

        visited.add(current)

        # получаем список соседей
        neighbors = deps.get(current, [])
        adjacency[current] = neighbors

        # если достигли максимальной глубины, дальше не идём
        if depth >= max_depth:
            continue

        # добавляем соседей в стек
        for name in neighbors:
            if name not in visited:
                stack.append((name, depth + 1))

    # вывод результата
    print("Список смежности:")
    for name in adjacency:
        neigh = adjacency[name]
        if len(neigh) == 0:
            line = "∅"
        else:
            line = ", ".join(neigh)
        print(f"  {name} -> {line}")


def main():
    if len(sys.argv) < 2:
        print("Использование: python main.py config.csv")
        sys.exit(1)

    config_path = sys.argv[1]
    params = read_config(config_path)
    validate_config(params)
    print_params(params)               # Этап 1
    deps = stage2_get_direct_deps(params)  # Этап 2
    stage3_build_graph(params, deps)   # Этап 3


if __name__ == "__main__":
    main()
