#!/usr/bin/env python3
import os
import io
import sys
import json
import yaml
import time
import pandas as pd
import requests
from pathlib import Path

CONFIG_PATH = Path("config.yaml")
SKU_PATH = Path("data/skus.txt")

def load_config():
    if not CONFIG_PATH.exists():
        print("config.yaml не найден", file=sys.stderr)
        sys.exit(1)
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def load_skus():
    if not SKU_PATH.exists():
        print("data/skus.txt не найден", file=sys.stderr)
        sys.exit(1)
    skus = []
    with open(SKU_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            skus.append(line.upper())
    if not skus:
        print("Список артикулов пуст в data/skus.txt", file=sys.stderr)
        sys.exit(1)
    return set(skus)

def download_feed(url: str) -> bytes:
    if not url:
        print("FEED_URL не задан. Добавьте секрет репозитория FEED_URL.", file=sys.stderr)
        sys.exit(1)
    print(f"Скачиваю файл: {url}")
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    return r.content

def guess_format(url: str, content: bytes) -> str:
    url = (url or "").lower()
    if url.endswith(".xlsx") or url.endswith(".xlsm"):
        return "xlsx"
    if url.endswith(".xls"):
        return "xls"
    if url.endswith(".csv"):
        return "csv"
    # fallback по заголовкам
    return "csv" if b"," in content[:4096] or b";" in content[:4096] else "xlsx"

def read_dataframe(fmt: str, content: bytes, cfg: dict) -> pd.DataFrame:
    if fmt in ("xlsx", "xls"):
        sheet_name = cfg.get("sheet_name", None)
        try:
            df = pd.read_excel(io.BytesIO(content), sheet_name=sheet_name)
        except Exception as e:
            print(f"Ошибка чтения Excel: {e}", file=sys.stderr)
            sys.exit(1)
        return df
    else:
        delimiter = cfg.get("delimiter", ",") or ","
        encoding = cfg.get("encoding", "utf-8") or "utf-8"
        try:
            df = pd.read_csv(io.BytesIO(content), sep=delimiter, encoding=encoding)
        except Exception as e:
            print(f"Ошибка чтения CSV: {e}", file=sys.stderr)
            sys.exit(1)
        return df

def normalize_series_to_str(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip()

def to_number(series: pd.Series, decimal: str):
    s = series.astype(str).str.strip()
    if decimal == ",":
        s = s.str.replace(".", "", regex=False)  # тысячи .
        s = s.str.replace(",", ".", regex=False) # десятичная ,
    else:
        s = s.str.replace(",", "", regex=False)  # тысячи ,
    return pd.to_numeric(s, errors="coerce")

def main():
    cfg = load_config()
    sku_column = cfg.get("sku_column")
    name_column = cfg.get("name_column")
    qty_column = cfg.get("qty_column")
    price_column = cfg.get("price_column")
    decimal = cfg.get("decimal", ".")
    out_csv = Path(cfg.get("out_csv", "public/filtered_products.csv"))
    out_json = Path(cfg.get("out_json", "public/filtered_products.json"))
    keep = cfg.get("keep_columns", ["sku","name","qty","price"])

    skus = load_skus()
    feed_url = os.environ.get("FEED_URL", "").strip()
    content = download_feed(feed_url)
    fmt = guess_format(feed_url, content)
    df = read_dataframe(fmt, content, cfg)

    # Проверяем наличие колонок
    cols = set(df.columns.astype(str))
    needed = {sku_column, name_column, qty_column, price_column}
    missing = [c for c in needed if c not in cols]
    if missing:
        print("Колонки не найдены:", missing, file=sys.stderr)
        print("Колонки в файле:", list(df.columns), file=sys.stderr)
        sys.exit(1)

    # Нормализация
    df[sku_column] = normalize_series_to_str(df[sku_column]).str.upper()
    df[name_column] = normalize_series_to_str(df[name_column])

    # Фильтр по артикулам
    filtered = df[df[sku_column].isin(skus)].copy()

    # Приведение qty/price
    filtered["__qty__"] = to_number(filtered[qty_column], decimal=decimal).fillna(0).astype("Int64")
    filtered["__price__"] = to_number(filtered[price_column], decimal=decimal)

    # Переименование и финальная форма
    out = pd.DataFrame({
        "sku": filtered[sku_column],
        "name": filtered[name_column],
        "qty": filtered["__qty__"],
        "price": filtered["__price__"],
    })

    # Сортировка по артикулу
    out = out.sort_values(by=["sku"], kind="stable").reset_index(drop=True)

    # Оставляем только нужные колонки, если заданы
    if keep:
        missing_keep = [c for c in keep if c not in out.columns]
        if missing_keep:
            print(f"Внимание: в keep_columns есть отсутствующие поля: {missing_keep}", file=sys.stderr)
        out = out[[c for c in keep if c in out.columns]]

    # Создание директорий и запись
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_json.parent.mkdir(parents=True, exist_ok=True)

    out.to_csv(out_csv, index=False, encoding="utf-8")
    out.to_json(out_json, orient="records", force_ascii=False, indent=2)

    print(f"Готово: {out_csv} ({len(out)} позиций), {out_json}")

if __name__ == "__main__":
    main()
