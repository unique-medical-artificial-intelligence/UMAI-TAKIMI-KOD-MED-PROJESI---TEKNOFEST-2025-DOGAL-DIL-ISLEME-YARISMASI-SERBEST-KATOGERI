# ICD-10 Academic Article Scraper for Google Colab
# Bu kod, ICD-10 kodlarını işleyerek akademik platformlardan makale toplar

import os
import time
import json
import random
import zipfile
import pandas as pd
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple
import logging
from tqdm import tqdm
import re

# Selenium imports
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException

# Google Colab specific imports
from google.colab import files
import shutil

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ICDArticleScraper:
    """ICD-10 kodları için akademik makale toplama sınıfı"""

    def __init__(self):
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0"
        ]

        self.output_dir = "collected_json"
        self.failed_codes = []
        self.stats = {
            "total_codes": 0,
            "processed_codes": 0,
            "failed_codes": 0,
            "total_articles": 0,
            "articles_by_source": {
                "PubMed": 0,
                "Google Scholar": 0,
                "Semantic Scholar": 0,
                "ArXiv": 0
            }
        }

        # Geçici klasörleri oluştur
        os.makedirs(self.output_dir, exist_ok=True)

    def get_random_user_agent(self) -> str:
        """Rastgele bir User-Agent döndürür"""
        return random.choice(self.user_agents)

    def create_driver(self) -> webdriver.Chrome:
        """Headless Chrome WebDriver oluşturur"""
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument(f"--user-agent={self.get_random_user_agent()}")

        return webdriver.Chrome(options=chrome_options)

    def wait_random(self, min_seconds: float = 2, max_seconds: float = 5):
        """Rastgele bekleme süresi"""
        time.sleep(random.uniform(min_seconds, max_seconds))

    def scrape_pubmed(self, icd_code: str, disease_name: str) -> List[Dict]:
        """PubMed'den makale toplar"""
        articles = []
        driver = None

        try:
            driver = self.create_driver()

            # PubMed arama URL'si
            search_query = f"{icd_code} {disease_name}" if disease_name else icd_code
            url = f"https://pubmed.ncbi.nlm.nih.gov/?term={search_query}"

            driver.get(url)
            self.wait_random(2, 4)

            # Makale elementlerini bekle
            wait = WebDriverWait(driver, 10)

            try:
                # Makale listesini bekle
                articles_container = wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, "search-results-chunk"))
                )

                # Makale elementlerini bul
                article_elements = driver.find_elements(By.CLASS_NAME, "docsum-content")

                for element in article_elements[:10]:  # İlk 10 makaleyi al
                    try:
                        article_data = self._extract_pubmed_article_data(element, driver)
                        if article_data:
                            articles.append(article_data)
                    except Exception as e:
                        logger.warning(f"PubMed makale çıkarma hatası: {e}")
                        continue

            except TimeoutException:
                logger.warning(f"PubMed için {icd_code} arama sonuçları bulunamadı")

        except Exception as e:
            logger.error(f"PubMed scraping hatası {icd_code}: {e}")
        finally:
            if driver:
                driver.quit()

        return articles

    def _extract_pubmed_article_data(self, element, driver) -> Optional[Dict]:
        """PubMed makale verilerini çıkarır"""
        try:
            # Başlık
            title_elem = element.find_element(By.CLASS_NAME, "docsum-title")
            title = title_elem.text.strip()

            # URL
            url_elem = title_elem.find_element(By.TAG_NAME, "a")
            url = url_elem.get_attribute("href")

            # Yazarlar
            authors = []
            try:
                authors_elem = element.find_element(By.CLASS_NAME, "docsum-authors")
                authors_text = authors_elem.text.strip()
                authors = [author.strip() for author in authors_text.split(",")]
            except NoSuchElementException:
                pass

            # Dergi ve tarih
            journal = None
            pub_date = None
            try:
                journal_elem = element.find_element(By.CLASS_NAME, "docsum-journal-citation")
                journal_text = journal_elem.text.strip()

                # Dergi adını ve tarihi ayır
                parts = journal_text.split(".")
                if len(parts) >= 2:
                    journal = parts[0].strip()
                    # Tarih regex ile çıkar
                    date_match = re.search(r'(\d{4})', journal_text)
                    if date_match:
                        pub_date = date_match.group(1)

            except NoSuchElementException:
                pass

            # Abstract (detay sayfasından)
            abstract = None
            doi = None
            try:
                # Detay sayfasına git
                driver.execute_script(f"window.open('{url}', '_blank');")
                driver.switch_to.window(driver.window_handles[1])

                # Abstract'ı bekle
                abstract_elem = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "abstract-content"))
                )
                abstract = abstract_elem.text.strip()

                # DOI'yi bul
                try:
                    doi_elem = driver.find_element(By.CSS_SELECTOR, "[data-ga-action='DOI']")
                    doi = doi_elem.text.strip()
                except NoSuchElementException:
                    pass

                # Pencereyi kapat
                driver.close()
                driver.switch_to.window(driver.window_handles[0])

            except Exception:
                if len(driver.window_handles) > 1:
                    driver.close()
                    driver.switch_to.window(driver.window_handles[0])

            return {
                "title": title,
                "authors": authors,
                "publication_date": pub_date,
                "journal_or_conference": journal,
                "source": "PubMed",
                "url": url,
                "abstract": abstract,
                "doi": doi
            }

        except Exception as e:
            logger.warning(f"PubMed makale veri çıkarma hatası: {e}")
            return None

    def scrape_google_scholar(self, icd_code: str, disease_name: str) -> List[Dict]:
        """Google Scholar'dan makale toplar"""
        articles = []
        driver = None

        try:
            driver = self.create_driver()

            # Google Scholar arama URL'si
            search_query = f"{icd_code} {disease_name}" if disease_name else icd_code
            url = f"https://scholar.google.com/scholar?q={search_query}"

            driver.get(url)
            self.wait_random(3, 6)

            # Makale elementlerini bekle
            wait = WebDriverWait(driver, 10)

            try:
                # Sonuçları bekle
                results_container = wait.until(
                    EC.presence_of_element_located((By.ID, "gs_res_ccl_mid"))
                )

                # Makale elementlerini bul
                article_elements = driver.find_elements(By.CLASS_NAME, "gs_r")

                for element in article_elements[:10]:  # İlk 10 makaleyi al
                    try:
                        article_data = self._extract_google_scholar_article_data(element)
                        if article_data:
                            articles.append(article_data)
                    except Exception as e:
                        logger.warning(f"Google Scholar makale çıkarma hatası: {e}")
                        continue

            except TimeoutException:
                logger.warning(f"Google Scholar için {icd_code} arama sonuçları bulunamadı")

        except Exception as e:
            logger.error(f"Google Scholar scraping hatası {icd_code}: {e}")
        finally:
            if driver:
                driver.quit()

        return articles

    def _extract_google_scholar_article_data(self, element) -> Optional[Dict]:
        """Google Scholar makale verilerini çıkarır"""
        try:
            # Başlık ve URL
            title_elem = element.find_element(By.TAG_NAME, "h3")
            title_link = title_elem.find_element(By.TAG_NAME, "a")
            title = title_link.text.strip()
            url = title_link.get_attribute("href")

            # Yazarlar ve dergi bilgisi
            authors = []
            journal = None
            pub_date = None

            try:
                authors_elem = element.find_element(By.CLASS_NAME, "gs_a")
                authors_text = authors_elem.text.strip()

                # Yazarları ve dergi bilgisini ayır
                parts = authors_text.split(" - ")
                if len(parts) >= 2:
                    authors_part = parts[0]
                    authors = [author.strip() for author in authors_part.split(",")]

                    # Dergi ve tarih
                    journal_part = parts[1]
                    date_match = re.search(r'(\d{4})', journal_part)
                    if date_match:
                        pub_date = date_match.group(1)
                        journal = journal_part.replace(pub_date, "").strip().rstrip(",")
                    else:
                        journal = journal_part

            except NoSuchElementException:
                pass

            # Özet
            abstract = None
            try:
                abstract_elem = element.find_element(By.CLASS_NAME, "gs_rs")
                abstract = abstract_elem.text.strip()
            except NoSuchElementException:
                pass

            return {
                "title": title,
                "authors": authors,
                "publication_date": pub_date,
                "journal_or_conference": journal,
                "source": "Google Scholar",
                "url": url,
                "abstract": abstract,
                "doi": None
            }

        except Exception as e:
            logger.warning(f"Google Scholar makale veri çıkarma hatası: {e}")
            return None

    def scrape_semantic_scholar(self, icd_code: str, disease_name: str) -> List[Dict]:
        """Semantic Scholar'dan makale toplar"""
        articles = []

        try:
            # Semantic Scholar API kullan
            search_query = f"{icd_code} {disease_name}" if disease_name else icd_code
            url = f"https://api.semanticscholar.org/graph/v1/paper/search"

            params = {
                "query": search_query,
                "limit": 10,
                "fields": "title,authors,year,venue,abstract,url,externalIds"
            }

            headers = {
                "User-Agent": self.get_random_user_agent()
            }

            response = requests.get(url, params=params, headers=headers, timeout=30)
            self.wait_random(1, 3)

            if response.status_code == 200:
                data = response.json()

                for paper in data.get("data", []):
                    try:
                        authors = [author.get("name", "") for author in paper.get("authors", [])]

                        # DOI'yi bul
                        doi = None
                        external_ids = paper.get("externalIds", {})
                        if external_ids and "DOI" in external_ids:
                            doi = external_ids["DOI"]

                        article_data = {
                            "title": paper.get("title", ""),
                            "authors": authors,
                            "publication_date": str(paper.get("year", "")) if paper.get("year") else None,
                            "journal_or_conference": paper.get("venue", ""),
                            "source": "Semantic Scholar",
                            "url": paper.get("url", ""),
                            "abstract": paper.get("abstract", ""),
                            "doi": doi
                        }

                        articles.append(article_data)

                    except Exception as e:
                        logger.warning(f"Semantic Scholar makale işleme hatası: {e}")
                        continue

        except Exception as e:
            logger.error(f"Semantic Scholar scraping hatası {icd_code}: {e}")

        return articles

    def scrape_arxiv(self, icd_code: str, disease_name: str) -> List[Dict]:
        """ArXiv'den makale toplar"""
        articles = []

        try:
            # ArXiv API kullan
            search_query = f"{icd_code} {disease_name}" if disease_name else icd_code
            url = f"http://export.arxiv.org/api/query"

            params = {
                "search_query": f"all:{search_query}",
                "start": 0,
                "max_results": 10
            }

            headers = {
                "User-Agent": self.get_random_user_agent()
            }

            response = requests.get(url, params=params, headers=headers, timeout=30)
            self.wait_random(1, 3)

            if response.status_code == 200:
                # XML yanıtını işle
                import xml.etree.ElementTree as ET
                root = ET.fromstring(response.content)

                # Namespace tanımla
                ns = {"atom": "http://www.w3.org/2005/Atom"}

                entries = root.findall(".//atom:entry", ns)

                for entry in entries:
                    try:
                        title = entry.find("atom:title", ns).text.strip()

                        # Yazarları topla
                        authors = []
                        for author in entry.findall("atom:author", ns):
                            name = author.find("atom:name", ns)
                            if name is not None:
                                authors.append(name.text.strip())

                        # Tarih
                        published = entry.find("atom:published", ns)
                        pub_date = published.text[:4] if published is not None else None

                        # URL
                        url = entry.find("atom:id", ns).text.strip()

                        # Abstract
                        abstract_elem = entry.find("atom:summary", ns)
                        abstract = abstract_elem.text.strip() if abstract_elem is not None else None

                        article_data = {
                            "title": title,
                            "authors": authors,
                            "publication_date": pub_date,
                            "journal_or_conference": "ArXiv",
                            "source": "ArXiv",
                            "url": url,
                            "abstract": abstract,
                            "doi": None
                        }

                        articles.append(article_data)

                    except Exception as e:
                        logger.warning(f"ArXiv makale işleme hatası: {e}")
                        continue

        except Exception as e:
            logger.error(f"ArXiv scraping hatası {icd_code}: {e}")

        return articles

    def scrape_all_sources(self, icd_code: str, disease_name: str) -> List[Dict]:
        """Tüm kaynaklardan paralel olarak makale toplar"""
        all_articles = []

        # Paralel scraping
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(self.scrape_pubmed, icd_code, disease_name): "PubMed",
                executor.submit(self.scrape_google_scholar, icd_code, disease_name): "Google Scholar",
                executor.submit(self.scrape_semantic_scholar, icd_code, disease_name): "Semantic Scholar",
                executor.submit(self.scrape_arxiv, icd_code, disease_name): "ArXiv"
            }

            for future in as_completed(futures):
                source = futures[future]
                try:
                    articles = future.result()
                    all_articles.extend(articles)
                    self.stats["articles_by_source"][source] += len(articles)
                    logger.info(f"{source} - {len(articles)} makale bulundu")
                except Exception as e:
                    logger.error(f"{source} scraping hatası: {e}")

        return all_articles

    def save_articles_to_json(self, icd_code: str, disease_name: str, articles: List[Dict]):
        """Makaleleri JSON dosyasına kaydet"""
        try:
            data = {
                "icd_code": icd_code,
                "disease_name": disease_name,
                "search_timestamp_utc": datetime.utcnow().isoformat() + "Z",
                "articles": articles
            }

            filename = f"{icd_code}.json"
            filepath = os.path.join(self.output_dir, filename)

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            logger.info(f"JSON dosyası kaydedildi: {filepath}")

        except Exception as e:
            logger.error(f"JSON kaydetme hatası {icd_code}: {e}")

    def create_zip_archive(self):
        """Toplanan verileri ZIP arşivine koy"""
        try:
            # Geçici klasör yapısı oluştur
            temp_dir = "temp_archive"
            archive_path = os.path.join(temp_dir, "collected_data", "content", "collected_json")
            os.makedirs(archive_path, exist_ok=True)

            # JSON dosyalarını kopyala
            for filename in os.listdir(self.output_dir):
                if filename.endswith('.json'):
                    src = os.path.join(self.output_dir, filename)
                    dst = os.path.join(archive_path, filename)
                    shutil.copy2(src, dst)

            # ZIP dosyası oluştur
            zip_filename = "collected_data.zip"
            with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, dirs, files in os.walk(temp_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, temp_dir)
                        zipf.write(file_path, arcname)

            # Geçici klasörü temizle
            shutil.rmtree(temp_dir)

            logger.info(f"ZIP arşivi oluşturuldu: {zip_filename}")
            return zip_filename

        except Exception as e:
            logger.error(f"ZIP oluşturma hatası: {e}")
            return None

    def save_failed_codes(self):
        """Başarısız kodları dosyaya kaydet"""
        if self.failed_codes:
            with open("failed_codes.txt", 'w', encoding='utf-8') as f:
                for code in self.failed_codes:
                    f.write(f"{code}\n")
            logger.info(f"Başarısız kodlar kaydedildi: failed_codes.txt")

    def process_icd_codes(self, df: pd.DataFrame):
        """Ana işlem fonksiyonu"""
        self.stats["total_codes"] = len(df)

        # İlerleme çubuğu
        pbar = tqdm(total=len(df), desc="ICD kodları işleniyor")

        for index, row in df.iterrows():
            icd_code = row['icd_code']
            disease_name = row.get('disease_name', '')

            try:
                logger.info(f"İşlenen Kod: {icd_code} - {disease_name}")

                # Tüm kaynaklardan makale topla
                articles = self.scrape_all_sources(icd_code, disease_name)

                # JSON'a kaydet
                self.save_articles_to_json(icd_code, disease_name, articles)

                # İstatistikleri güncelle
                self.stats["processed_codes"] += 1
                self.stats["total_articles"] += len(articles)

                logger.info(f"İşlenen Kod: {icd_code} - {len(articles)} makale bulundu. ({self.stats['processed_codes']}/{self.stats['total_codes']})")

                # Her 10 kodda bir ZIP oluştur
                if self.stats["processed_codes"] % 10 == 0:
                    self.create_zip_archive()
                    logger.info(f"Ara kayıt: {self.stats['processed_codes']} kod işlendi")

            except Exception as e:
                logger.error(f"Kod işleme hatası {icd_code}: {e}")
                self.failed_codes.append(icd_code)
                self.stats["failed_codes"] += 1

            pbar.update(1)

        pbar.close()

        # Nihai ZIP oluştur
        zip_file = self.create_zip_archive()

        # Başarısız kodları kaydet
        self.save_failed_codes()

        # Sonuç raporunu yazdır
        self.print_final_report()

        return zip_file

    def print_final_report(self):
        """Nihai raporu yazdır"""
        print("\n" + "="*50)
        print("İŞLEM RAPORU")
        print("="*50)
        print(f"Toplam işlenen ICD kodu sayısı: {self.stats['total_codes']}")
        print(f"Başarıyla işlenen kod sayısı: {self.stats['processed_codes']}")
        print(f"Başarısız olan kod sayısı: {self.stats['failed_codes']}")
        if self.stats['failed_codes'] > 0:
            print("(failed_codes.txt dosyasına bakınız)")
        print(f"Toplam toplanan makale sayısı: {self.stats['total_articles']}")
        print("\nKaynak başına toplanan makale sayısı:")
        for source, count in self.stats['articles_by_source'].items():
            print(f"  {source}: {count}")
        print("="*50)


def main():
    """Ana fonksiyon"""
    # Kurulum
    print("Gerekli kütüphaneler kuruluyor...")
    os.system("pip install selenium pandas requests tqdm")

    # ChromeDriver kur
    os.system("apt-get update")
    os.system("apt-get install -y chromium-browser")
    os.system("apt-get install -y chromium-chromedriver")

    # Dosya yükleme
    print("\nLütfen icd_codes_filtered.csv dosyasını yükleyin:")
    uploaded = files.upload()

    if not uploaded:
        print("Dosya yüklenmedi. İşlem sonlandırılıyor.")
        return

    csv_filename = list(uploaded.keys())[0]

    # CSV'yi oku
    try:
        df = pd.read_csv(csv_filename)
        print(f"\nCSV dosyası okundu: {len(df)} ICD kodu bulundu")
        print(f"Sütunlar: {list(df.columns)}")

        # Zorunlu sütun kontrolü
        if 'icd_code' not in df.columns:
            print("Hata: CSV dosyasında 'icd_code' sütunu bulunamadı!")
            return

    except Exception as e:
        print(f"CSV okuma hatası: {e}")
        return

    # Scraper'ı başlat
    scraper = ICDArticleScraper()

    print(f"\n{len(df)} ICD kodu için makale toplama işlemi başlatılıyor...")
    print("Bu işlem uzun sürebilir. Lütfen bekleyiniz...\n")

    # İşlemi başlat
    zip_file = scraper.process_icd_codes(df)

    # İndirme linki sağla
    if zip_file and os.path.exists(zip_file):
        print(f"\nİşlem tamamlandı! ZIP dosyası hazır: {zip_file}")
        files.download(zip_file)
    else:
        print("Hata: ZIP dosyası oluşturulamadı!")

if __name__ == "__main__":
    main()