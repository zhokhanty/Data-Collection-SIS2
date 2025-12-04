import time
import json
import os
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
SAVE_TO_FILE = os.path.join(DATA_DIR, "my_habr_articles.json")

HEADLESS = True
HOW_MANY_PAGES = 6
WAIT_BETWEEN_PAGES = 1  
PAGE_LOAD_TIMEOUT = 10  


def create_browser():
    """Создаёт оптимизированный браузер"""
    options = webdriver.ChromeOptions()
    
    if HEADLESS:
        options.add_argument('--headless=new')
    
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-software-rasterizer')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-images')
    options.add_argument('--blink-settings=imagesEnabled=false')
    options.add_argument('--window-size=1920,1080')
    
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    prefs = {
        "profile.managed_default_content_settings.images": 2,  
        "profile.default_content_setting_values.notifications": 2, 
    }
    options.add_experimental_option("prefs", prefs)
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    
    # Анти-детект
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => false});")
    
    return driver


def get_one_article_info(article_block):
    """Извлекает информацию об одной статье"""
    article = {}
    
    try:
        title_link = article_block.find_element(By.CSS_SELECTOR, "h2 a")
        article["title"] = title_link.text.strip()
        article["url"] = title_link.get_attribute("href")
    except:
        article["title"] = "(без заголовка)"
        article["url"] = None
    
    try:
        author = article_block.find_element(By.CSS_SELECTOR, "a.tm-user-info__username")
        article["author"] = author.text.strip()
    except:
        article["author"] = None
    
    try:
        time_tag = article_block.find_element(By.CSS_SELECTOR, "time")
        article["publication_date"] = time_tag.get_attribute("datetime")
    except:
        article["publication_date"] = None
    
    try:
        views = article_block.find_element(By.CSS_SELECTOR, "span.tm-icon-counter__value")
        article["views"] = views.text.strip()
    except:
        article["views"] = None
    
    try:
        rating = article_block.find_element(By.CSS_SELECTOR, "span.tm-votes-meter__value")
        article["rating"] = rating.text.strip()
    except:
        article["rating"] = None
    
    try:
        hubs = article_block.find_elements(By.CSS_SELECTOR, "a.tm-publication-hub__link")
        hub_names = [h.text.strip() for h in hubs]
        article["hubs"] = ", ".join(hub_names)
    except:
        article["hubs"] = None
    
    try:
        preview = article_block.find_element(By.CSS_SELECTOR, "div.article-formatted-body")
        text = preview.text.strip()
        article["preview_text"] = text[:500] + "..." if len(text) > 500 else text
    except:
        article["preview_text"] = None
    
    try:
        comments = article_block.find_element(By.CSS_SELECTOR, "span.tm-article-comments-counter__value")
        article["comments_count"] = comments.text.strip()
    except:
        article["comments_count"] = None
    
    try:
        bookmarks = article_block.find_element(By.CSS_SELECTOR, "span.bookmarks-button__counter")
        article["bookmarks_count"] = bookmarks.text.strip()
    except:
        article["bookmarks_count"] = None
    
    article["scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return article


def scrape_one_page(driver, page_number):
    """Скрапит одну страницу"""
    start_time = time.time()
    
    if page_number == 1:
        url = "https://habr.com/ru/articles/"
    else:
        url = f"https://habr.com/ru/articles/page{page_number}/"
    
    print(f"📄 Page {page_number}: {url}")
    
    try:
        driver.get(url)
        
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "article.tm-articles-list__item"))
        )
        
        articles_blocks = driver.find_elements(By.CSS_SELECTOR, "article.tm-articles-list__item")
        print(f"   ✓ Found {len(articles_blocks)} articles", end="")
        
        page_articles = []
        for block in articles_blocks:
            info = get_one_article_info(block)
            if info["title"] != "(без заголовка)":
                page_articles.append(info)
        
        elapsed = time.time() - start_time
        print(f" (took {elapsed:.1f}s)")
        
        return page_articles
        
    except Exception as e:
        print(f"Error: {e}")
        return []


def main():
    """Основная функция scraping"""
    print("="*60)
    print("STARTING HABR SCRAPER")
    print("="*60)
    print(f"Target: {HOW_MANY_PAGES} pages")
    print(f"Output: {SAVE_TO_FILE}")
    print(f"Headless: {HEADLESS}")
    print("="*60 + "\n")
    
    total_start = time.time()
    
    os.makedirs(DATA_DIR, exist_ok=True)
    
    print("Starting browser...")
    browser_start = time.time()
    driver = create_browser()
    print(f"   ✓ Browser ready (took {time.time() - browser_start:.1f}s)\n")
    
    all_articles = []
    
    try:
        for page in range(1, HOW_MANY_PAGES + 1):
            articles_this_page = scrape_one_page(driver, page)
            all_articles.extend(articles_this_page)
            
            if page < HOW_MANY_PAGES:
                time.sleep(WAIT_BETWEEN_PAGES)
        
        print(f"\n💾 Saving {len(all_articles)} articles...")
        with open(SAVE_TO_FILE, "w", encoding="utf-8") as f:
            json.dump(all_articles, f, ensure_ascii=False, indent=4)
        
        total_time = time.time() - total_start
        
        print("\n" + "="*60)
        print("SCRAPING COMPLETED")
        print("="*60)
        print(f"Total articles: {len(all_articles)}")
        print(f"⏱Total time: {total_time:.1f}s ({total_time/60:.1f} min)")
        print(f"Avg per page: {total_time/HOW_MANY_PAGES:.1f}s")
        print(f"Saved to: {SAVE_TO_FILE}")
        print("="*60)
        
    except Exception as e:
        print(f"\nCritical error: {e}")
        raise
    
    finally:
        driver.quit()
        print("\n🔒 Browser closed")


if __name__ == "__main__":
    main()