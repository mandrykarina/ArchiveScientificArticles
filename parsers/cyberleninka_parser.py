"""
CyberLeninka Parser Library
Библиотека для работы с научными статьями КиберЛенинки через OAI-PMH и HTML-парсинг.

Аналог arxiv-библиотеки, адаптированный для российского репозитория.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Iterator, Callable
import time
import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import re
import json
from pathlib import Path


@dataclass
class CyberArticle:
    """
    Представление статьи из КиберЛенинки.
    Аналог arxiv.Result, но с полями, специфичными для CyberLeninka.
    """
    # Базовые поля из OAI-PMH
    identifier: str  # URL статьи
    title: str
    authors: List[str] = field(default_factory=list)
    publisher: str = ""
    datestamp: Optional[datetime] = None  # Дата добавления/обновления в OAI
    subjects: List[str] = field(default_factory=list)  # Тематические рубрики (dc:subject)

    # Новое поле: подкатегория (пока пустая)
    subcategory: str = ""

    # Расширенные поля из HTML meta-тегов
    abstract: str = ""
    keywords: List[str] = field(default_factory=list)
    doi: str = ""
    journal_title: str = ""
    publication_date: str = ""  # Год публикации
    volume: str = ""
    issue: str = ""
    issn: str = ""
    eissn: str = ""
    pdf_url: str = ""
    citation: str = ""  # Готовая библиографическая ссылка
    place_of_publication: str = ""
    field_of_science: str = ""  # Область наук (из HTML-блока)

    # Метаданные парсинга
    fetched_from_html: bool = False

    def __str__(self) -> str:
        """Удобное строковое представление"""
        authors_str = ", ".join(self.authors[:3])
        if len(self.authors) > 3:
            authors_str += f" et al. ({len(self.authors)} authors)"

        parts = [f"[{self.publication_date or 'N/A'}]"]
        if authors_str:
            parts.append(authors_str)
        parts.append(f'"{self.title}"')
        if self.journal_title:
            parts.append(f"// {self.journal_title}")
        if self.doi:
            parts.append(f"DOI: {self.doi}")

        # Категория (область наук) и подкатегория
        cat = self.field_of_science if self.field_of_science else "—"
        subcat = self.subcategory if self.subcategory else "—"
        parts.append(f"[Категория: {cat}; Подкатегория: {subcat}]")

        return " ".join(parts)

    @property
    def entry_id(self) -> str:
        """Извлекает короткий ID из URL"""
        if "/article/n/" in self.identifier:
            return self.identifier.split("/article/n/")[-1].rstrip("/")
        return self.identifier


class CyberOAIClient:
    """
    Низкоуровневый клиент для работы с OAI-PMH КиберЛенинки.
    """

    BASE_URL = "https://cyberleninka.ru/oai"
    OAI_NAMESPACE = {
        "oai": "http://www.openarchives.org/OAI/2.0/",
        "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
        "dc": "http://purl.org/dc/elements/1.1/",
    }

    def __init__(self, delay_seconds: float = 3.0):
        self.delay_seconds = delay_seconds
        self.last_request_time = 0.0
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "CyberLeninka-OAI-Client/1.0 (Educational/Research)",
            }
        )

    def _wait_if_needed(self):
        elapsed = time.time() - self.last_request_time
        if elapsed < self.delay_seconds:
            time.sleep(self.delay_seconds - elapsed)
        self.last_request_time = time.time()

    def _make_request(self, params: dict) -> ET.Element:
        self._wait_if_needed()
        response = self.session.get(self.BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        root = ET.fromstring(response.content)
        error_elem = root.find(".//oai:error", self.OAI_NAMESPACE)
        if error_elem is not None:
            error_code = error_elem.get("code", "unknown")
            error_msg = error_elem.text or "No message"
            raise ValueError(f"OAI-PMH error [{error_code}]: {error_msg}")
        return root

    def _parse_dc_record(self, record_elem: ET.Element) -> dict:
        result: Dict[str, Any] = {}
        header = record_elem.find("oai:header", self.OAI_NAMESPACE)
        if header is not None:
            result["identifier"] = header.findtext(
                "oai:identifier", namespaces=self.OAI_NAMESPACE, default=""
            )
            datestamp_str = header.findtext(
                "oai:datestamp", namespaces=self.OAI_NAMESPACE, default=""
            )
            if datestamp_str:
                try:
                    result["datestamp"] = datetime.fromisoformat(
                        datestamp_str.replace("Z", "+00:00")
                    )
                except ValueError:
                    result["datestamp"] = None
            else:
                result["datestamp"] = None
        else:
            result["datestamp"] = None

        dc_elem = record_elem.find(".//oai_dc:dc", self.OAI_NAMESPACE)
        if dc_elem is not None:
            result["title"] = dc_elem.findtext(
                "dc:title", namespaces=self.OAI_NAMESPACE, default=""
            )
            result["creators"] = [
                elem.text
                for elem in dc_elem.findall("dc:creator", self.OAI_NAMESPACE)
                if elem.text
            ]
            result["publisher"] = dc_elem.findtext(
                "dc:publisher", namespaces=self.OAI_NAMESPACE, default=""
            )
            result["type"] = dc_elem.findtext(
                "dc:type", namespaces=self.OAI_NAMESPACE, default=""
            )
            result["subjects"] = [
                elem.text
                for elem in dc_elem.findall("dc:subject", self.OAI_NAMESPACE)
                if elem.text
            ]
        return result

    def identify(self) -> dict:
        root = self._make_request({"verb": "Identify"})
        identify_elem = root.find(".//oai:Identify", self.OAI_NAMESPACE)
        result: Dict[str, Any] = {}
        if identify_elem is not None:
            for child in identify_elem:
                tag = child.tag.replace("{http://www.openarchives.org/OAI/2.0/}", "")
                result[tag] = child.text
        return result

    def list_sets(self) -> List[dict]:
        root = self._make_request({"verb": "ListSets"})
        sets: List[dict] = []
        for set_elem in root.findall(".//oai:set", self.OAI_NAMESPACE):
            spec = set_elem.findtext("oai:setSpec", namespaces=self.OAI_NAMESPACE)
            name = set_elem.findtext("oai:setName", namespaces=self.OAI_NAMESPACE)
            if spec:
                sets.append({"setSpec": spec, "setName": name or spec})
        return sets

    def get_record(self, identifier: str) -> dict:
        params = {
            "verb": "GetRecord",
            "identifier": identifier,
            "metadataPrefix": "oai_dc",
        }
        root = self._make_request(params)
        record_elem = root.find(".//oai:record", self.OAI_NAMESPACE)
        if record_elem is None:
            raise ValueError(f"Record not found: {identifier}")
        return self._parse_dc_record(record_elem)

    def list_identifiers(
        self,
        from_date: Optional[str] = None,
        until_date: Optional[str] = None,
        set_spec: Optional[str] = None,
    ) -> Iterator[dict]:
        params: Dict[str, Any] = {"verb": "ListIdentifiers", "metadataPrefix": "oai_dc"}
        if from_date:
            params["from"] = from_date
        if until_date:
            params["until"] = until_date
        if set_spec:
            params["set"] = set_spec

        while True:
            root = self._make_request(params)
            for header_elem in root.findall(".//oai:header", self.OAI_NAMESPACE):
                identifier = header_elem.findtext(
                    "oai:identifier", namespaces=self.OAI_NAMESPACE
                )
                datestamp_str = header_elem.findtext(
                    "oai:datestamp", namespaces=self.OAI_NAMESPACE
                )
                datestamp: Optional[datetime] = None
                if datestamp_str:
                    try:
                        datestamp = datetime.fromisoformat(
                            datestamp_str.replace("Z", "+00:00")
                        )
                    except ValueError:
                        pass
                if identifier:
                    yield {"identifier": identifier, "datestamp": datestamp}
            token_elem = root.find(".//oai:resumptionToken", self.OAI_NAMESPACE)
            if token_elem is None or not token_elem.text:
                break
            params = {"verb": "ListIdentifiers", "resumptionToken": token_elem.text}

    def list_records(
        self,
        from_date: Optional[str] = None,
        until_date: Optional[str] = None,
        set_spec: Optional[str] = None,
    ) -> Iterator[dict]:
        params: Dict[str, Any] = {"verb": "ListRecords", "metadataPrefix": "oai_dc"}
        if from_date:
            params["from"] = from_date
        if until_date:
            params["until"] = until_date
        if set_spec:
            params["set"] = set_spec

        while True:
            root = self._make_request(params)
            for record_elem in root.findall(".//oai:record", self.OAI_NAMESPACE):
                yield self._parse_dc_record(record_elem)
            token_elem = root.find(".//oai:resumptionToken", self.OAI_NAMESPACE)
            if token_elem is None or not token_elem.text:
                break
            params = {"verb": "ListRecords", "resumptionToken": token_elem.text}


class CyberHTMLParser:
    """
    Парсер HTML-страниц статей КиберЛенинки для извлечения расширенных метаданных.
    """

    def __init__(self, delay_seconds: float = 2.0):
        self.delay_seconds = delay_seconds
        self.last_request_time = 0.0
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (compatible; CyberLeninka-Research-Parser/1.0)",
            }
        )

    def _wait_if_needed(self):
        elapsed = time.time() - self.last_request_time
        if elapsed < self.delay_seconds:
            time.sleep(self.delay_seconds - elapsed)
        self.last_request_time = time.time()

    def extract_metadata(self, url: str) -> Dict[str, Any]:
        self._wait_if_needed()
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Warning: Failed to fetch HTML for {url}: {e}")
            return {}

        soup = BeautifulSoup(response.content, "html.parser")
        metadata: Dict[str, Any] = {}

        # citation_ мета-теги
        citation_map = {
            "citation_publication_date": "publication_date",
            "citation_journal_title": "journal_title",
            "citation_volume": "volume",
            "citation_issue": "issue",
            "citation_issn": "issn",
            "citation_eissn": "eissn",
            "citation_pdf_url": "pdf_url",
        }
        for meta_name, field_name in citation_map.items():
            tag = soup.find("meta", attrs={"name": meta_name})
            if tag and tag.get("content"):
                metadata[field_name] = tag["content"]

        # Keywords
        keywords_tag = soup.find("meta", attrs={"name": "citation_keywords"})
        if keywords_tag and keywords_tag.get("content"):
            raw_keywords = keywords_tag["content"]
            metadata["keywords"] = [
                kw.strip() for kw in raw_keywords.split(",") if kw.strip()
            ]
        else:
            metadata["keywords"] = []

        # Abstract
        desc_tag = soup.find("meta", attrs={"name": "description"})
        if desc_tag and desc_tag.get("content"):
            metadata["abstract"] = desc_tag["content"]

        # DOI
        doi_div = soup.find("div", class_="statitem label-doi")
        if doi_div and doi_div.get("title"):
            title_text = doi_div["title"]
            doi_match = re.search(r"10\.\d+/[^\s]+", title_text)
            if doi_match:
                metadata["doi"] = doi_match.group(0)
        if "doi" not in metadata:
            for p in soup.find_all("p")[:15]:
                text = p.get_text()
                doi_match = re.search(r"DOI:\s*(10\.\d+/[^\s]+)", text, re.IGNORECASE)
                if doi_match:
                    metadata["doi"] = doi_match.group(1)
                    break

        # Библиографическая ссылка
        citation_tag = soup.find("meta", attrs={"name": "eprints.citation"})
        if citation_tag and citation_tag.get("content"):
            metadata["citation"] = citation_tag["content"]

        # Место публикации
        place_tag = soup.find("meta", attrs={"name": "eprints.place_of_pub"})
        if place_tag and place_tag.get("content"):
            metadata["place_of_publication"] = place_tag["content"]

        # Область наук – исправлено: удаляем префикс "Область наук"
        field_div = soup.find("div", class_="half-right")
        if field_div:
            raw = field_div.get_text(strip=True)
            if raw:
                # Удаляем подстроку "Область наук" (может быть слитно или с пробелом/двоеточием)
                cleaned = re.sub(r'^Область наук\s*:?\s*', '', raw)
                metadata["field_of_science"] = cleaned.strip()

        return metadata


class CyberParser:
    """
    Парсер статей КиберЛенинки.
    Аналог ArxivParser - предоставляет высокоуровневые методы для получения статей.
    """

    def __init__(
        self,
        oai_delay: float = 3.0,
        html_delay: float = 2.0,
        enrich_metadata: bool = True,
    ):
        self.oai_client = CyberOAIClient(delay_seconds=oai_delay)
        self.html_parser = (
            CyberHTMLParser(delay_seconds=html_delay) if enrich_metadata else None
        )
        self.enrich_metadata = enrich_metadata

    def _oai_to_article(self, oai_record: dict) -> CyberArticle:
        return CyberArticle(
            identifier=oai_record.get("identifier", ""),
            title=oai_record.get("title", ""),
            authors=oai_record.get("creators", []),
            publisher=oai_record.get("publisher", ""),
            datestamp=oai_record.get("datestamp"),
            subjects=oai_record.get("subjects", []),
        )

    def _enrich_article(self, article: CyberArticle) -> CyberArticle:
        if not self.html_parser or not article.identifier:
            return article
        try:
            html_meta = self.html_parser.extract_metadata(article.identifier)
            article.abstract = html_meta.get("abstract", "")
            article.keywords = html_meta.get("keywords", [])
            article.doi = html_meta.get("doi", "")
            article.journal_title = html_meta.get("journal_title", "")
            article.publication_date = html_meta.get("publication_date", "")
            article.volume = html_meta.get("volume", "")
            article.issue = html_meta.get("issue", "")
            article.issn = html_meta.get("issn", "")
            article.eissn = html_meta.get("eissn", "")
            article.pdf_url = html_meta.get("pdf_url", "")
            article.citation = html_meta.get("citation", "")
            article.place_of_publication = html_meta.get("place_of_publication", "")
            article.field_of_science = html_meta.get("field_of_science", "")
            article.fetched_from_html = True
        except Exception as e:
            print(f"Warning: Failed to enrich article {article.entry_id}: {e}")
        return article

    def get_article_by_id(
        self, identifier: str, enrich: Optional[bool] = None
    ) -> CyberArticle:
        should_enrich = enrich if enrich is not None else self.enrich_metadata
        oai_record = self.oai_client.get_record(identifier)
        article = self._oai_to_article(oai_record)
        if should_enrich:
            article = self._enrich_article(article)
        return article

    def _get_articles_for_period(
        self,
        from_date: str,
        until_date: Optional[str] = None,
        set_spec: Optional[str] = None,
        max_articles: Optional[int] = None,
    ) -> List[CyberArticle]:
        articles: List[CyberArticle] = []
        for oai_record in self.oai_client.list_records(
            from_date=from_date, until_date=until_date, set_spec=set_spec
        ):
            article = self._oai_to_article(oai_record)
            if self.enrich_metadata:
                article = self._enrich_article(article)
            articles.append(article)
            if max_articles and len(articles) >= max_articles:
                break
        return articles

    def get_latest(
        self, count: int = 10, set_spec: Optional[str] = None
    ) -> List[CyberArticle]:
        articles: List[CyberArticle] = []
        days_back = 1
        max_days = 90
        while len(articles) < count and days_back <= max_days:
            from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
            until_date = datetime.now().strftime("%Y-%m-%d")
            batch = self._get_articles_for_period(
                from_date=from_date,
                until_date=until_date,
                set_spec=set_spec,
                max_articles=count,
            )
            articles.extend(batch)
            if len(articles) >= count:
                break
            days_back = min(days_back * 2, max_days)
        return articles[:count]

    # ---------- JSON export methods ----------
    def article_to_dict(self, article: CyberArticle) -> Dict[str, Any]:
        """
        Преобразует CyberArticle в словарь, содержащий ВСЕ поля статьи.
        """
        return {
            "id": article.entry_id,
            "url": article.identifier,
            "title": article.title,
            "authors": article.authors,
            "publication_date": article.publication_date,
            "language": "ru",   # можно определить из HTML, пока статика
            "subjects": article.subjects,
            "subcategory": article.subcategory,
            "abstract": article.abstract,
            "keywords": article.keywords,
            "doi": article.doi,
            "journal_title": article.journal_title,
            "volume": article.volume,
            "issue": article.issue,
            "issn": article.issn,
            "eissn": article.eissn,
            "pdf_url": article.pdf_url,
            "citation": article.citation,
            "place_of_publication": article.place_of_publication,
            "field_of_science": article.field_of_science,
            "fetched_from_html": article.fetched_from_html,
            "datestamp": article.datestamp.isoformat() if article.datestamp else None,
        }

    def export_articles_to_json(self, articles: List[CyberArticle], filepath: str) -> None:
        """
        Экспортирует список статей в JSON-файл.
        """
        data = [self.article_to_dict(article) for article in articles]
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"[OK] Экспортировано {len(articles)} статей в {filepath}")


class CyberMonitor:
    """
    Монитор изменений в репозитории КиберЛенинки через OAI-PMH.
    """

    def __init__(self, oai_delay: float = 3.0, state_file: Optional[str] = None):
        self.oai_client = CyberOAIClient(delay_seconds=oai_delay)
        self.state_file = Path(state_file) if state_file else None
        self.last_check_date: Optional[datetime] = None
        if self.state_file and self.state_file.exists():
            self._load_state()

    def _load_state(self):
        try:
            with open(self.state_file, "r") as f:
                data = json.load(f)
            last_check_str = data.get("last_check_date")
            if last_check_str:
                self.last_check_date = datetime.fromisoformat(last_check_str)
        except Exception as e:
            print(f"Warning: Failed to load state from {self.state_file}: {e}")

    def _save_state(self):
        if not self.state_file:
            return
        try:
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.state_file, "w") as f:
                json.dump(
                    {
                        "last_check_date": self.last_check_date.isoformat()
                        if self.last_check_date
                        else None,
                        "updated_at": datetime.now().isoformat(),
                    },
                    f,
                    indent=2,
                )
        except Exception as e:
            print(f"Warning: Failed to save state to {self.state_file}: {e}")

    def list_modified_identifiers(
        self,
        from_date: str,
        until_date: Optional[str] = None,
        set_spec: Optional[str] = None,
    ) -> List[str]:
        identifiers: List[str] = []
        try:
            for record in self.oai_client.list_identifiers(
                from_date=from_date, until_date=until_date, set_spec=set_spec
            ):
                identifiers.append(record["identifier"])
        except ValueError as e:
            if "noRecordsMatch" in str(e):
                pass
            else:
                raise
        return identifiers

    def monitor_changes(
        self,
        last_check_date: datetime,
        parser: CyberParser,
        set_spec: Optional[str] = None,
    ) -> List[CyberArticle]:
        from_date = last_check_date.strftime("%Y-%m-%d")
        until_date = datetime.now().strftime("%Y-%m-%d")
        print(f"Мониторинг изменений с {from_date} по {until_date}")
        if set_spec:
            print(f"Фильтр по коллекции: {set_spec}")

        identifiers = self.list_modified_identifiers(
            from_date=from_date, until_date=until_date, set_spec=set_spec
        )
        print(f"Найдено {len(identifiers)} изменённых записей")

        articles: List[CyberArticle] = []
        for i, identifier in enumerate(identifiers, 1):
            try:
                article = parser.get_article_by_id(identifier)
                articles.append(article)
                if i % 10 == 0:
                    print(f"Обработано {i}/{len(identifiers)} статей...")
            except Exception as e:
                print(f"Warning: Failed to fetch article {identifier}: {e}")
        self.last_check_date = datetime.now()
        self._save_state()
        return articles

    def continuous_monitoring(
        self,
        parser: CyberParser,
        callback: Callable[[List[CyberArticle]], None],
        interval_hours: int = 24,
        set_spec: Optional[str] = None,
    ):
        print(f"Запуск непрерывного мониторинга (интервал: {interval_hours} ч)")
        if self.last_check_date is None:
            self.last_check_date = datetime.now() - timedelta(days=1)
            print(f"Начальная дата мониторинга: {self.last_check_date}")
        while True:
            try:
                articles = self.monitor_changes(
                    last_check_date=self.last_check_date,
                    parser=parser,
                    set_spec=set_spec,
                )
                if articles:
                    callback(articles)
                else:
                    print("Новых статей не обнаружено")
                print(f"Следующая проверка через {interval_hours} ч")
                time.sleep(interval_hours * 3600)
            except KeyboardInterrupt:
                print("\nМониторинг остановлен пользователем")
                break
            except Exception as e:
                print(f"Ошибка мониторинга: {e}")
                print(f"Повтор через {interval_hours} ч")
                time.sleep(interval_hours * 3600)