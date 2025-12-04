import sqlite3
import json
import os
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
DATABASE_FILE = os.path.join(DATA_DIR, "habr_articles.db")
CLEANED_JSON = os.path.join(DATA_DIR, "cleaned_articles.json")


def create_database_schema(cursor):
    """
    Создаёт таблицу articles с правильной схемой
    """
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS articles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        url TEXT UNIQUE NOT NULL,
        author TEXT,
        publication_date TEXT,
        views INTEGER DEFAULT 0,
        rating INTEGER DEFAULT 0,
        hubs TEXT,
        preview_text TEXT,
        comments_count INTEGER DEFAULT 0,
        bookmarks_count INTEGER DEFAULT 0,
        scraped_at TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
    )
    ''')
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_url ON articles(url)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_publication_date ON articles(publication_date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_author ON articles(author)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_rating ON articles(rating)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_views ON articles(views)')
    
    print("✓ Database schema created successfully")


def load_cleaned_data():
    """
    Загружает очищенные данные из JSON файла
    """
    if not os.path.exists(CLEANED_JSON):
        raise FileNotFoundError(f"Cleaned JSON not found: {CLEANED_JSON}. Run cleaner first.")
    
    with open(CLEANED_JSON, "r", encoding="utf-8") as f:
        articles = json.load(f)
    
    print(f"✓ Loaded {len(articles)} articles from {CLEANED_JSON}")
    return articles


def insert_or_update_article(cursor, article):
    """
    Вставляет новую статью или обновляет существующую
    """
    url = article.get("url")
    if not url:
        return None, "No URL"
    
    cursor.execute("SELECT id FROM articles WHERE url = ?", (url,))
    exists = cursor.fetchone()
    
    if exists:
        cursor.execute('''
        UPDATE articles SET
            title = ?,
            author = ?,
            publication_date = ?,
            views = ?,
            rating = ?,
            hubs = ?,
            preview_text = ?,
            comments_count = ?,
            bookmarks_count = ?,
            scraped_at = ?,
            updated_at = datetime('now')
        WHERE url = ?
        ''', (
            article.get("title", "Без названия"),
            article.get("author", ""),
            article.get("publication_date", ""),
            article.get("views", 0),
            article.get("rating", 0),
            article.get("hubs", ""),
            article.get("preview_text", ""),
            article.get("comments_count", 0),
            article.get("bookmarks_count", 0),
            article.get("scraped_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            url
        ))
        return exists[0], "updated"
    else:
        cursor.execute('''
        INSERT INTO articles
        (title, url, author, publication_date, views, rating, hubs,
         preview_text, comments_count, bookmarks_count, scraped_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            article.get("title", "Без названия"),
            url,
            article.get("author", ""),
            article.get("publication_date", ""),
            article.get("views", 0),
            article.get("rating", 0),
            article.get("hubs", ""),
            article.get("preview_text", ""),
            article.get("comments_count", 0),
            article.get("bookmarks_count", 0),
            article.get("scraped_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        ))
        return cursor.lastrowid, "inserted"


def print_statistics(cursor):
    """
    Выводит статистику по базе данных
    """
    print("\n" + "="*60)
    print("DATABASE STATISTICS")
    print("="*60)
    
    cursor.execute("SELECT COUNT(*) FROM articles")
    total = cursor.fetchone()[0]
    print(f"Total articles in database: {total}")
    
    try:
        cursor.execute("""
            SELECT 
                MIN(publication_date) as earliest,
                MAX(publication_date) as latest
            FROM articles 
            WHERE publication_date != '' AND publication_date IS NOT NULL
        """)
        dates = cursor.fetchone()
        if dates and dates[0]:
            print(f"📅 Date range: {dates[0]} to {dates[1]}")
    except Exception:
        pass
    
    try:
        cursor.execute("""
            SELECT author, COUNT(*) as count
            FROM articles
            WHERE author != '' AND author IS NOT NULL
            GROUP BY author
            ORDER BY count DESC
            LIMIT 5
        """)
        print("\n👥 Top 5 authors:")
        for author, count in cursor.fetchall():
            print(f"   • {author}: {count} articles")
    except Exception:
        pass
    
    try:
        cursor.execute("""
            SELECT hubs, COUNT(*) as count
            FROM articles
            WHERE hubs != '' AND hubs IS NOT NULL
            GROUP BY hubs
            ORDER BY count DESC
            LIMIT 5
        """)
        print("\n🏷️  Top 5 hub combinations:")
        for hub, count in cursor.fetchall():
            print(f"   • {hub[:50]}...: {count} articles")
    except Exception:
        pass
    
    try:
        cursor.execute("""
            SELECT title, views
            FROM articles
            WHERE views > 0
            ORDER BY views DESC
            LIMIT 5
        """)
        print("\n👀 Top 5 most viewed articles:")
        for title, views in cursor.fetchall():
            print(f"   • {title[:50]}... ({views:,} views)")
    except Exception:
        pass
    
    try:
        cursor.execute("""
            SELECT title, rating
            FROM articles
            WHERE rating != 0
            ORDER BY rating DESC
            LIMIT 5
        """)
        print("\n⭐ Top 5 highest rated articles:")
        for title, rating in cursor.fetchall():
            print(f"   • {title[:50]}... (rating: {rating:+d})")
    except Exception:
        pass
    
    print("="*60 + "\n")


def main():
    """
    Основная функция загрузки данных в SQLite
    """
    print("Starting data loading process...")
    print(f"Database location: {DATABASE_FILE}\n")
    
    os.makedirs(DATA_DIR, exist_ok=True)
    
    try:
        articles = load_cleaned_data()
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return
    
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    
    try:
        create_database_schema(cursor)
        conn.commit()
        
        added = 0
        updated = 0
        errors = 0
        
        print("\nProcessing articles...")
        for i, article in enumerate(articles, 1):
            try:
                article_id, status = insert_or_update_article(cursor, article)
                
                if status == "inserted":
                    added += 1
                elif status == "updated":
                    updated += 1
                else:
                    errors += 1
                
                if i % 50 == 0:
                    conn.commit()
                    print(f"  Processed {i}/{len(articles)} articles...")
                
            except Exception as e:
                errors += 1
                print(f"Error processing article {i}: {e}")
        
        conn.commit()
        
        print("\n" + "="*60)
        print("LOADING RESULTS")
        print("="*60)
        print(f"New articles added: {added}")
        print(f"Existing articles updated: {updated}")
        print(f"Errors (skipped): {errors}")
        print(f"Total processed: {added + updated}")
        print("="*60)
        
        print_statistics(cursor)
        
        print(f"✓ Data successfully loaded to: {DATABASE_FILE}")
        
    except Exception as e:
        conn.rollback()
        print(f"Critical error: {e}")
        raise
    
    finally:
        conn.close()


if __name__ == "__main__":
    main()