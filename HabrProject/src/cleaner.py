import pandas as pd
import json
import os
import re
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_FILE = os.path.join(PROJECT_ROOT, "data", "my_habr_articles.json")
OUTPUT_JSON = os.path.join(PROJECT_ROOT, "data", "cleaned_articles.json")
OUTPUT_CSV = os.path.join(PROJECT_ROOT, "data", "cleaned_articles.csv")


def turn_into_number(text):
    if not text or text == "0":
        return 0

    text = str(text).strip().lower()

    if "k" in text or "к" in text:
        text = text.replace("k", "").replace("к", "")
        try:
            return int(float(text) * 1000)
        except:
            return 0

    numbers_only = re.sub(r"[^\d-]", "", text)
    try:
        return int(numbers_only) if numbers_only else 0
    except:
        return 0


def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Input file not found: {INPUT_FILE}. Run scraper first.")
        return

    with open(INPUT_FILE, "r", encoding="utf-8") as file:
        raw_articles = json.load(file)

    df = pd.DataFrame(raw_articles)

    print("Columns:", df.columns.tolist())

    before = len(df)
    df = df.drop_duplicates(subset=["url"])
    after = len(df)
    print(f"Deduplicated: {before} -> {after}")

    # Нормализация пустых полей
    for column in ["views", "comments_count", "bookmarks_count", "rating"]:
        if column in df.columns:
            df[column] = df[column].fillna("0")

    for column in ["author", "hubs", "preview_text"]:
        if column in df.columns:
            df[column] = df[column].fillna("")

    df = df.dropna(subset=["title", "url"])

    for col in ["views", "comments_count", "bookmarks_count", "rating"]:
        if col in df.columns:
            df[col] = df[col].apply(turn_into_number)
            print(f"   → Колонка '{col}' теперь с нормальными числами")

    if "title" in df.columns:
        df["title"] = df["title"].str.strip()
        df["title"] = df["title"].str.replace(r"\s+", " ", regex=True)

    if "author" in df.columns:
        df["author"] = df["author"].str.strip().str.replace("@", "")

    if "preview_text" in df.columns:
        df["preview_text"] = df["preview_text"].str.strip()
        df["preview_text"] = df["preview_text"].str.replace(r"\s+", " ", regex=True)
        df["preview_text"] = df["preview_text"].str[:500]

    if "hubs" in df.columns:
        df["hubs"] = df["hubs"].str.strip()

    if "publication_date" in df.columns:
        df["publication_date"] = pd.to_datetime(df["publication_date"], errors="coerce")
        df["publication_date"] = df["publication_date"].dt.strftime("%Y-%m-%d %H:%M:%S")
        df["publication_date"] = df["publication_date"].fillna("")

    if "scraped_at" in df.columns:
        df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")
        df["scraped_at"] = df["scraped_at"].dt.strftime("%Y-%m-%d %H:%M:%S")

    needed = ["title", "url", "author", "publication_date"]
    for col in needed:
        if col not in df.columns:
            print(f"Ой! Нет колонки: {col}")

    os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)

    df.to_json(OUTPUT_JSON, orient="records", force_ascii=False, indent=4)
    print(f" JSON → {OUTPUT_JSON}")

    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f" CSV → {OUTPUT_CSV}")

    print("Rows after cleaning:", len(df))


if __name__ == "__main__":
    main()
