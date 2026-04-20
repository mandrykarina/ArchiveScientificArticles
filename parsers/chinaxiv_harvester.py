"""
ChinaRxiv harvester / monitor with full HTML enrichment (final version).
Extracts all available metadata from the article page.
"""
from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup


# =========================
# Configuration
# =========================
DEFAULT_BASE_URL = os.getenv("CHINARXIV_BASE_URL", "https://chinarxiv.org")
DEFAULT_API_PREFIX = "/api/v1"
DEFAULT_PAGE_SIZE = 50
DEFAULT_TIMEOUT = 30
DEFAULT_DELAY_SECONDS = 1.0
DEFAULT_NUM_RETRIES = 3
DEFAULT_LOOKBACK_DAYS = 7


# =========================
# Data model (fully extended)
# =========================
@dataclass
class ChinaRxivArticle:
    # --- API fields ---
    id: str
    title: str = ""
    abstract: str = ""
    authors: List[str] = field(default_factory=list)
    date: Optional[str] = None          # YYYY-MM-DD from API
    source_language: str = ""
    source_url: str = ""
    subjects: List[str] = field(default_factory=list)
    subcategory: str = ""               # manual
    has_figures: bool = False
    has_full_text: bool = False
    has_pdf: bool = False
    links: Dict[str, str] = field(default_factory=dict)
    raw: Dict[str, Any] = field(default_factory=dict)

    # --- HTML-extracted fields ---
    domain: str = ""                     # main category
    subject: str = ""                    # subcategory
    doi: str = ""
    cstr: str = ""
    txid: str = ""
    submitted_date_full: str = ""        # full datetime string
    version: str = ""
    license: str = ""
    metrics_views: int = 0
    metrics_downloads: int = 0
    keywords: List[str] = field(default_factory=list)

    @property
    def full_text_url(self) -> Optional[str]:
        return self.links.get("full_text")

    @property
    def pdf_url(self) -> Optional[str]:
        return self.links.get("pdf")

    @property
    def figures_url(self) -> Optional[str]:
        return self.links.get("figures")

    @property
    def self_url(self) -> Optional[str]:
        return self.links.get("self")


# =========================
# Parser
# =========================
class ChinaRxivParser:
    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        delay_seconds: float = DEFAULT_DELAY_SECONDS,
        num_retries: int = DEFAULT_NUM_RETRIES,
        timeout: int = DEFAULT_TIMEOUT,
        user_agent: str = "Mozilla/5.0",
        api_email: Optional[str] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_prefix = DEFAULT_API_PREFIX
        self.delay_seconds = delay_seconds
        self.num_retries = num_retries
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": user_agent,
            }
        )
        if api_email:
            self.session.headers["X-API-Email"] = api_email

    # ---------- API methods ----------
    def _api_url(self, path: str) -> str:
        return f"{self.base_url}{self.api_prefix}{path}"

    def _request_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = self._api_url(path)
        last_error = None
        for attempt in range(1, self.num_retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                if self.delay_seconds:
                    time.sleep(self.delay_seconds)
                return response.json()
            except Exception as exc:
                last_error = exc
                if attempt < self.num_retries:
                    time.sleep(min(2.0 * attempt, 5.0))
                else:
                    break
        raise RuntimeError(f"Failed to fetch {url}: {last_error}") from last_error

    @staticmethod
    def _normalize_article(item: Dict[str, Any]) -> ChinaRxivArticle:
        links = item.get("_links") or {}
        return ChinaRxivArticle(
            id=str(item.get("id", "")),
            title=str(item.get("title", "") or ""),
            abstract=str(item.get("abstract", "") or ""),
            authors=list(item.get("authors") or []),
            date=item.get("date"),
            source_language=str(item.get("source_language", "") or ""),
            source_url=str(item.get("source_url", "") or ""),
            subjects=list(item.get("subjects") or []),
            subcategory="",
            has_figures=bool(item.get("has_figures", False)),
            has_full_text=bool(item.get("has_full_text", False)),
            has_pdf=bool(item.get("has_pdf", False)),
            links={k: str(v) for k, v in links.items()},
            raw=item,
        )

    @staticmethod
    def _parse_date(value: str | date | datetime) -> date:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        return datetime.strptime(str(value), "%Y-%m-%d").date()

    def _extract_items(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        if isinstance(payload.get("data"), list):
            return payload["data"]
        for key in ("items", "results", "papers"):
            if isinstance(payload.get(key), list):
                return payload[key]
        return []

    def _get_page(self, **params) -> Dict[str, Any]:
        return self._request_json("/papers", params=params)

    # ---------- HTML parsing (final version based on real HTML) ----------
    def _parse_article_html(self, article: ChinaRxivArticle) -> Dict[str, Any]:
        url = article.source_url
        if not url:
            return {}
        result = {
            "domain": "", "subject": "", "doi": "", "cstr": "", "txid": "",
            "submitted_date_full": "", "version": "", "license": "",
            "metrics_views": 0, "metrics_downloads": 0, "keywords": []
        }
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "lxml")

            # 1. Domain and Subject (from 分类 block)
            cat_li = soup.find("li", string=re.compile(r"分类[：:]"))
            if cat_li:
                links = cat_li.find_all("a")
                if len(links) >= 1:
                    result["domain"] = links[0].get_text(strip=True)
                if len(links) >= 2:
                    result["subject"] = links[1].get_text(strip=True)
            if not result["domain"]:
                domain_link = soup.find("a", href=re.compile(r"field=domain"))
                if domain_link:
                    result["domain"] = domain_link.get_text(strip=True)
            if not result["subject"]:
                subject_link = soup.find("a", href=re.compile(r"field=subject"))
                if subject_link:
                    result["subject"] = subject_link.get_text(strip=True)

            # 2. DOI
            doi_link = soup.find("a", href=re.compile(r"dx\.doi\.org"))
            if doi_link:
                href = doi_link.get("href", "")
                if "doi.org/" in href:
                    result["doi"] = href.split("doi.org/")[-1]
                else:
                    result["doi"] = doi_link.get_text(strip=True).replace("DOI:", "").strip()
            # 3. CSTR
            cstr_link = soup.find("a", href=re.compile(r"cstr\.cn"))
            if cstr_link:
                result["cstr"] = cstr_link.get_text(strip=True).replace("CSTR:", "").strip()
            # 4. TXID
            txid_link = soup.find("a", href=re.compile(r"sciencechain\.ac\.cn"))
            if txid_link:
                result["txid"] = txid_link.get_text(strip=True)

            # 5. Submit Time - improved search
            submit_label = soup.find("b", string=re.compile(r"提交时间[：:]"))
            if submit_label:
                # Get parent <li> and extract text after the <b>
                parent_li = submit_label.find_parent("li")
                if parent_li:
                    # The text node is usually after the <b> tag
                    text = parent_li.get_text()
                    match = re.search(r"提交时间[：:]\s*(.+)", text)
                    if match:
                        result["submitted_date_full"] = match.group(1).strip()

            # 6. Version - find table after "版本历史" heading
            version_header = soup.find("h4", string=re.compile(r"版本历史"))
            if version_header:
                version_table = version_header.find_next("table")
                if version_table:
                    rows = version_table.find_all("tr")
                    for row in rows:
                        cells = row.find_all("td")
                        if cells and len(cells) >= 1:
                            ver_text = cells[0].get_text(strip=True)
                            if ver_text.startswith("[V") or ver_text.startswith("V"):
                                result["version"] = ver_text.strip("[]")
                                break

            # 7. License
            license_box = soup.find("div", class_="box", string=re.compile(r"许可声明"))
            if license_box:
                lic_link = license_box.find("a")
                if lic_link:
                    result["license"] = lic_link.get_text(strip=True)
                else:
                    img = license_box.find("img")
                    if img and img.get("alt"):
                        result["license"] = img["alt"]
            if not result["license"]:
                # fallback: look for CC license text anywhere
                cc_match = soup.find(string=re.compile(r"CC BY"))
                if cc_match:
                    result["license"] = cc_match.strip()

            # 8. Metrics (views and downloads)
            metrics_box = soup.find("div", class_="box", string=re.compile(r"metrics指标"))
            if metrics_box:
                # Views (点击量)
                views_li = metrics_box.find("li", string=re.compile(r"点击量"))
                if views_li:
                    views_span = views_li.find("span")
                    if views_span and views_span.get_text(strip=True).isdigit():
                        result["metrics_views"] = int(views_span.get_text(strip=True))
                # Downloads (下载量)
                downloads_li = metrics_box.find("li", string=re.compile(r"下载量"))
                if downloads_li:
                    downloads_span = downloads_li.find("span")
                    if downloads_span and downloads_span.get_text(strip=True).isdigit():
                        result["metrics_downloads"] = int(downloads_span.get_text(strip=True))

            # 9. Keywords
            keywords_div = soup.find("div", class_="brdge")
            if keywords_div:
                kw_spans = keywords_div.find_all("span", class_="spankwd")
                for span in kw_spans:
                    kw_text = span.get_text(strip=True)
                    if kw_text:
                        result["keywords"].append(kw_text)

            return result
        except Exception as e:
            print(f"Warning: failed to parse HTML for {article.id}: {e}")
            return result

    def _enrich_with_html(self, article: ChinaRxivArticle) -> ChinaRxivArticle:
        if not article.source_url:
            return article
        html_data = self._parse_article_html(article)
        for key, value in html_data.items():
            if hasattr(article, key):
                setattr(article, key, value)
        return article

    # ---------- Public API ----------
    def get_latest(self, count: int = 20, fetch_html: bool = False) -> List[ChinaRxivArticle]:
        count = max(1, count)
        articles = []
        page = 1
        seen_ids = set()
        while len(articles) < count and page <= 200:
            payload = self._get_page(page=page, per_page=min(DEFAULT_PAGE_SIZE, count))
            items = self._extract_items(payload)
            if not items:
                break
            for item in items:
                art = self._normalize_article(item)
                if art.id and art.id not in seen_ids:
                    articles.append(art)
                    seen_ids.add(art.id)
                    if len(articles) >= count:
                        break
            next_cursor = payload.get("next_cursor")
            if not next_cursor and len(items) < min(DEFAULT_PAGE_SIZE, count):
                break
            page += 1

        if fetch_html:
            enriched = []
            for art in articles[:count]:
                enriched.append(self._enrich_with_html(art))
                if self.delay_seconds:
                    time.sleep(self.delay_seconds)
            return enriched[:count]
        return articles[:count]

    def get_article_by_id(self, article_id: str, fetch_html: bool = True) -> Optional[ChinaRxivArticle]:
        article_id = article_id.strip()
        if not article_id:
            return None
        try:
            payload = self._request_json(f"/papers/{article_id}")
            if payload:
                article = self._normalize_article(payload)
                if fetch_html:
                    article = self._enrich_with_html(article)
                return article
        except Exception:
            pass
        for art in self.get_latest(200, fetch_html=False):
            if art.id == article_id:
                if fetch_html:
                    art = self._enrich_with_html(art)
                return art
        return None

    def get_articles_for_date(self, target_date: date, fetch_html: bool = False) -> List[ChinaRxivArticle]:
        target = self._parse_date(target_date)
        payload = self._get_page(from_date=target.isoformat(), to_date=target.isoformat(), per_page=DEFAULT_PAGE_SIZE)
        items = self._extract_items(payload)
        articles = [self._normalize_article(item) for item in items]
        if fetch_html:
            enriched = []
            for art in articles:
                enriched.append(self._enrich_with_html(art))
                if self.delay_seconds:
                    time.sleep(self.delay_seconds)
            return enriched
        return articles

    def get_articles_for_period(self, days_to_parse: int, fetch_html: bool = False) -> List[ChinaRxivArticle]:
        days_to_parse = max(1, days_to_parse)
        all_articles = []
        seen_ids = set()
        current_date = datetime.now(datetime.UTC).date()
        for day_offset in range(1, days_to_parse + 1):
            target_date = current_date - timedelta(days=day_offset)
            for article in self.get_articles_for_date(target_date, fetch_html=False):
                if article.id not in seen_ids:
                    all_articles.append(article)
                    seen_ids.add(article.id)
        if fetch_html:
            enriched = []
            for art in all_articles:
                enriched.append(self._enrich_with_html(art))
                if self.delay_seconds:
                    time.sleep(self.delay_seconds)
            return enriched
        return all_articles

    # ---------- JSON export ----------
    def article_to_dict(self, article: ChinaRxivArticle) -> Dict[str, Any]:
        url = article.source_url or article.self_url or ""
        return {
            "id": article.id,
            "url": url,
            "title": article.title,
            "authors": article.authors,
            "date": article.date or "",
            "language": article.source_language or "",
            "subjects": article.subjects,
            "subcategory": article.subcategory or "",
            "has_full_text": article.has_full_text,
            "has_pdf": article.has_pdf,
            "has_figures": article.has_figures,
            "abstract": article.abstract,
            "full_text_url": article.full_text_url,
            "pdf_url": article.pdf_url,
            "figures_url": article.figures_url,
            "domain": article.domain,
            "subject": article.subject,
            "doi": article.doi,
            "cstr": article.cstr,
            "txid": article.txid,
            "submitted_date_full": article.submitted_date_full,
            "version": article.version,
            "license": article.license,
            "metrics_views": article.metrics_views,
            "metrics_downloads": article.metrics_downloads,
            "keywords": article.keywords,
        }

    def export_articles_to_json(self, articles: List[ChinaRxivArticle], filepath: str) -> None:
        data = [self.article_to_dict(art) for art in articles]
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"[OK] Exported {len(articles)} articles to {filepath}")

    # ---------- Presentation ----------
    @staticmethod
    def print_separator(char: str = "=", width: int = 80) -> None:
        print(char * width)

    @staticmethod
    def print_result(article: ChinaRxivArticle, number: int) -> None:
        print(f"\n{'-' * 80}")
        print(f"[{number}] ID: {article.id}")
        print(f"    URL: {article.source_url or article.self_url or ''}")
        print(f"    Title: {article.title}")
        print(f"    Authors ({len(article.authors)}): {', '.join(article.authors)}")
        print(f"    Date (API): {article.date or 'n/a'}")
        print(f"    Language: {article.source_language or 'n/a'}")
        print(f"    Subjects (API): {', '.join(article.subjects) if article.subjects else 'n/a'}")
        print(f"    Subcategory (manual): {article.subcategory or 'n/a'}")
        print(f"    Domain: {article.domain or 'n/a'}")
        print(f"    Subject: {article.subject or 'n/a'}")
        print(f"    DOI: {article.doi or 'n/a'}")
        print(f"    CSTR: {article.cstr or 'n/a'}")
        print(f"    TXID: {article.txid or 'n/a'}")
        print(f"    Submitted (full): {article.submitted_date_full or 'n/a'}")
        print(f"    Version: {article.version or 'n/a'}")
        print(f"    License: {article.license or 'n/a'}")
        print(f"    Views: {article.metrics_views}, Downloads: {article.metrics_downloads}")
        print(f"    Keywords: {', '.join(article.keywords) if article.keywords else 'n/a'}")
        print(f"    has_full_text: {article.has_full_text}")
        print(f"    has_pdf: {article.has_pdf}")
        print(f"    has_figures: {article.has_figures}")
        if article.abstract:
            abstract = article.abstract.replace("\n", " ").strip()
            print(f"    Abstract ({len(abstract)} chars):")
            print(f"      {abstract[:900]}{'...' if len(abstract) > 900 else ''}")
        if article.full_text_url:
            print(f"    Full text: {article.full_text_url}")
        if article.pdf_url:
            print(f"    PDF: {article.pdf_url}")
        if article.figures_url:
            print(f"    Figures: {article.figures_url}")

    def print_statistics(self, articles: List[ChinaRxivArticle], mode: str) -> None:
        self.print_separator()
        print("STATISTICS")
        self.print_separator()
        print(f"Mode: {mode}")
        print(f"Total articles: {len(articles)}")
        if not articles:
            return
        dates = []
        for a in articles:
            if not a.date:
                continue
            try:
                dates.append(self._parse_date(a.date))
            except Exception:
                pass
        if dates:
            print(f"Date range: {min(dates).isoformat()} ... {max(dates).isoformat()}")
        by_lang = {}
        with_pdf = 0
        with_full_text = 0
        with_figures = 0
        for a in articles:
            by_lang[a.source_language or "unknown"] = by_lang.get(a.source_language or "unknown", 0) + 1
            with_pdf += int(a.has_pdf)
            with_full_text += int(a.has_full_text)
            with_figures += int(a.has_figures)
        print(f"With full text: {with_full_text}")
        print(f"With PDF: {with_pdf}")
        print(f"With figures: {with_figures}")
        print("Languages:")
        for lang, count in sorted(by_lang.items(), key=lambda x: (-x[1], x[0])):
            print(f"  {lang}: {count}")


# =========================
# Monitor (unchanged)
# =========================
class ChinaRxivMonitor:
    def __init__(self, parser: ChinaRxivParser):
        self.parser = parser

    @staticmethod
    def check_monitoring_window(now_utc: Optional[datetime] = None) -> Tuple[bool, str]:
        return True, "Monitoring possible anytime"

    def list_new_articles(
        self,
        since_date: Optional[date] = None,
        count: int = 50,
        fetch_html: bool = False,
    ) -> List[ChinaRxivArticle]:
        latest = self.parser.get_latest(count, fetch_html=fetch_html)
        if since_date is None:
            return latest
        cutoff = since_date
        result = []
        for article in latest:
            if not article.date:
                continue
            try:
                article_date = self.parser._parse_date(article.date)
            except Exception:
                continue
            if article_date >= cutoff:
                result.append(article)
        return result

    def monitor_changes(
        self,
        last_check_date: datetime,
        fetch_full: bool = False,
        max_items: int = 50,
        fetch_html: bool = False,
    ) -> Tuple[List[str], List[ChinaRxivArticle]]:
        can_monitor, explanation = self.check_monitoring_window()
        print(f"Change monitoring check: {explanation}")
        if not can_monitor:
            return [], []
        since_date = last_check_date.date()
        print(f"Searching new articles from {since_date.isoformat()}")
        articles = self.list_new_articles(since_date=since_date, count=max_items, fetch_html=fetch_html)
        ids = [a.id for a in articles if a.id]
        print(f"Found articles: {len(ids)}")
        if not fetch_full:
            return ids, articles
        full_articles = []
        for i, article_id in enumerate(ids, 1):
            article = self.parser.get_article_by_id(article_id, fetch_html=fetch_html)
            if article:
                full_articles.append(article)
            if i < len(ids):
                time.sleep(1)
        return ids, full_articles

    def continuous_monitoring(self, check_interval_hours: int = 6, fetch_html: bool = False) -> None:
        print(f"Starting continuous monitoring of ChinaRxiv (interval: {check_interval_hours} hours)")
        last_check = datetime.now(datetime.UTC) - timedelta(days=DEFAULT_LOOKBACK_DAYS)
        while True:
            print(f"\n[{datetime.now(datetime.UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}]")
            print("-" * 50)
            try:
                changed_ids, articles = self.monitor_changes(last_check, fetch_full=False, fetch_html=fetch_html)
                if changed_ids:
                    last_check = datetime.now(datetime.UTC) - timedelta(minutes=5)
                    by_lang = {}
                    for article in articles:
                        by_lang[article.source_language or "unknown"] = by_lang.get(article.source_language or "unknown", 0) + 1
                    print("Change statistics:")
                    for lang, count in sorted(by_lang.items(), key=lambda x: (-x[1], x[0])):
                        print(f"  {lang}: {count}")
                else:
                    print("No new articles found")
            except Exception as exc:
                print(f"Error during monitoring: {exc}")
            print(f"\nNext check in {check_interval_hours} hours...")
            time.sleep(check_interval_hours * 3600)


# =========================
# Demo
# =========================
def main() -> None:
    print("=== ChinaRxiv Parser with Full HTML Enrichment ===\n")
    parser = ChinaRxivParser(delay_seconds=0.5, num_retries=2)
    print("✅ Parser created\n")
    articles = parser.get_latest(2, fetch_html=True)
    for i, art in enumerate(articles, 1):
        parser.print_result(art, i)
        print()
    parser.export_articles_to_json(articles, "chinaxiv_full.json")


if __name__ == "__main__":
    main()