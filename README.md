# Habr.com Articles Scraping Pipeline

**Course:** Data Collection & Preparation  
**Website:** https://habr.com/ru/articles/  
**Team Members:** Bagytzhan Zhalgas Arman Zhilikbay 

## Description

Automated data pipeline that scrapes articles from Habr.com, cleans the data, and stores it in SQLite. The workflow runs daily using Apache Airflow.

**Why Habr?** Dynamic JavaScript-rendered content with infinite scroll makes it ideal for demonstrating Selenium web scraping.

## Database Schema

```sql
CREATE TABLE articles (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    url TEXT UNIQUE NOT NULL,
    author TEXT,
    publication_date TEXT,
    views INTEGER,
    rating INTEGER,
    hubs TEXT,
    preview_text TEXT,
    comments_count INTEGER,
    bookmarks_count INTEGER,
    scraped_at TEXT NOT NULL
);
```

## Installation

```bash
# Clone and navigate
git clone url
cd habr_pipeline

# Create virtual environment
python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux/Mac

# Install dependencies
pip install -r requirements.txt

# Create data directory
mkdir data
```

## Usage

### Run Individual Components

```bash
# Scrape articles
python src/scraper.py

# Clean data
python src/cleaner.py

# Load to database
python src/loader.py
```

### Run with Airflow

```bash
# Initialize Airflow
export AIRFLOW_HOME=~/airflow
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Copy DAG file
cp airflow_dag.py ~/airflow/dags/

# Start Airflow
airflow webserver --port 8080  # Terminal 1
airflow scheduler               # Terminal 2
```

### Access Airflow UI

1. Open http://localhost:8080
2. Login (admin/admin)
3. Enable DAG: `habr_articles_pipeline`
4. Trigger manually or wait for daily schedule

## Pipeline Flow

```
scrape_articles → clean_articles → load_to_database → pipeline_summary
```

**Schedule:** Daily (`@daily`)  
**Retries:** 2 attempts with 5-minute delay

## Data Cleaning

- Remove duplicates (based on URL)
- Handle missing values (numeric → 0, text → empty)
- Normalize text (trim spaces, clean characters)
- Convert types (strings to integers, dates to ISO format)
- Validate minimum 100 records

## Expected Output

```
Scraping: ~120 articles
Cleaning: ~120 records
Database: 120+ articles stored
```

## Verify Database

```bash
sqlite3 data/habr_articles.db

SELECT COUNT(*) FROM articles;
SELECT title, author, views FROM articles LIMIT 5;
```

## Requirements Checklist

✅ Dynamic website with JavaScript rendering  
✅ Selenium for scraping  
✅ Data cleaning & preprocessing  
✅ SQLite storage with clear schema  
✅ Airflow DAG with daily schedule  
✅ 100+ records minimum  
✅ Comprehensive logging  
✅ Successful pipeline execution

## Troubleshooting

**Not enough articles?**
```python
# In scraper.py, increase:
scraper.scrape(num_scrolls=20)  # or higher
```

**DAG not appearing?**
```bash
# Check file location
ls ~/airflow/dags/airflow_dag.py
# Restart scheduler
```

**Database locked?**
Close all SQLite connections and retry.


---

**Submission:** December 4, 2025 - 23:59:59  
**Defense:** December 5, 2025