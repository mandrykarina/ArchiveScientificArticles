import json
from pathlib import Path

from sqlalchemy import select

from app.db import SessionLocal
from app.models import Article, Author, ArticleAuthor, Keyword


PROCESSED_FILES = {
    "arxiv": Path("processed/arxiv_articles_processed.json"),
    "cyberleninka": Path("processed/cyberleninka_articles_processed.json"),
    "chinaxiv": Path("processed/chinaxiv_articles_processed.json"),
}


def unique_preserve_order(items):
    """
    Убирает дубликаты, сохраняя исходный порядок.
    """
    seen = set()
    result = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def normalize_text(value):
    """
    Приводит значение к строке без лишних пробелов.
    """
    if value is None:
        return ""
    return str(value).strip()


def normalize_list(values):
    """
    Чистит список строк, удаляет пустые значения и дубликаты.
    """
    if not values:
        return []

    cleaned = []
    for value in values:
        text = normalize_text(value)
        if text:
            cleaned.append(text)

    return unique_preserve_order(cleaned)


def make_absolute_url(url: str, base: str = "") -> str:
    """
    Делает относительную ссылку абсолютной.
    Если ссылка уже абсолютная — возвращает как есть.
    """
    url = normalize_text(url)
    if not url:
        return ""

    if url.startswith("http://") or url.startswith("https://"):
        return url

    if url.startswith("/") and base:
        return f"{base}{url}"

    return url


def normalize_article(source: str, item: dict) -> dict:
    """
    Приводит статью из конкретного источника к единому формату БД.
    """
    raw_url = normalize_text(item.get("url"))
    raw_pdf_url = normalize_text(item.get("pdf_url"))

    if source == "arxiv":
        bucket = "eng"
        category = item.get("archive") or "Другое"
        subcategory = item.get("subcategory") or item.get("primary_category_code") or "Общее"
        published_at = item.get("submitted_date")
        updated_at = item.get("updated_date")

        url = raw_url
        pdf_url = raw_pdf_url

    elif source == "cyberleninka":
        bucket = "rus"
        category = item.get("category") or item.get("field_of_science") or "Другое"
        subcategory = item.get("subcategory") or item.get("field_of_science") or "Общее"
        published_at = item.get("publication_date")
        updated_at = item.get("datestamp")

        url = raw_url
        pdf_url = raw_pdf_url

    elif source == "chinaxiv":
        bucket = "chn"
        category = item.get("domain") or "Другое"
        subcategory = item.get("subject") or "Общее"
        published_at = item.get("date") or item.get("submitted_date_full")
        updated_at = item.get("submitted_date_full")

        # ChinaXiv часто отдает относительные ссылки -> делаем абсолютными
        url = make_absolute_url(raw_url, base="https://chinaxiv.org")
        pdf_url = make_absolute_url(raw_pdf_url, base="https://chinaxiv.org")

    else:
        raise ValueError(f"Unknown source: {source}")

    authors = normalize_list(item.get("authors", []))
    keywords = normalize_list(item.get("keywords", []))

    return {
        "source": source,
        "bucket": bucket,
        "external_id": normalize_text(item.get("id")),
        "title": normalize_text(item.get("title")),
        "abstract": normalize_text(item.get("abstract")),
        "url": url,
        "pdf_url": pdf_url,
        "doi": normalize_text(item.get("doi")),
        "category": normalize_text(category) or "Другое",
        "subcategory": normalize_text(subcategory) or "Общее",
        "published_at": normalize_text(published_at),
        "updated_at": normalize_text(updated_at),
        "journal_title": normalize_text(item.get("journal_title")),
        "citation": normalize_text(item.get("citation")),
        "authors": authors,
        "keywords": keywords,
        "raw_json": json.dumps(item, ensure_ascii=False),
    }


def get_or_create_author(session, name: str) -> Author:
    """
    Возвращает автора по имени или создает нового.
    """
    stmt = select(Author).where(Author.name == name)
    author = session.execute(stmt).scalar_one_or_none()

    if author is not None:
        return author

    author = Author(name=name)
    session.add(author)
    session.flush()
    return author


def upsert_article(session, data: dict):
    """
    Создает новую статью или обновляет существующую.
    Уникальность определяется по (source, external_id).
    """
    stmt = select(Article).where(
        Article.source == data["source"],
        Article.external_id == data["external_id"]
    )
    article = session.execute(stmt).scalar_one_or_none()

    if article is None:
        article = Article(
            source=data["source"],
            bucket=data["bucket"],
            external_id=data["external_id"],
            title=data["title"],
            abstract=data["abstract"],
            url=data["url"],
            pdf_url=data["pdf_url"],
            doi=data["doi"],
            category=data["category"],
            subcategory=data["subcategory"],
            published_at=data["published_at"],
            updated_at=data["updated_at"],
            journal_title=data["journal_title"],
            citation=data["citation"],
            raw_json=data["raw_json"],
        )
        session.add(article)
        session.flush()
    else:
        article.bucket = data["bucket"]
        article.title = data["title"]
        article.abstract = data["abstract"]
        article.url = data["url"]
        article.pdf_url = data["pdf_url"]
        article.doi = data["doi"]
        article.category = data["category"]
        article.subcategory = data["subcategory"]
        article.published_at = data["published_at"]
        article.updated_at = data["updated_at"]
        article.journal_title = data["journal_title"]
        article.citation = data["citation"]
        article.raw_json = data["raw_json"]

        # Полностью пересобираем связи авторов и keywords для актуальности
        session.query(ArticleAuthor).filter(
            ArticleAuthor.article_id == article.id
        ).delete()

        session.query(Keyword).filter(
            Keyword.article_id == article.id
        ).delete()

        session.flush()

    for author_name in data["authors"]:
        author = get_or_create_author(session, author_name)
        session.add(ArticleAuthor(article_id=article.id, author_id=author.id))

    for keyword in data["keywords"]:
        session.add(Keyword(article_id=article.id, keyword=keyword))


def import_processed_file(source: str, path: Path):
    """
    Импортирует один processed JSON-файл в БД.
    """
    if not path.exists():
        print(f"⚠ Файл не найден: {path}")
        return

    with open(path, "r", encoding="utf-8") as f:
        items = json.load(f)

    imported_count = 0
    skipped_count = 0

    with SessionLocal() as session:
        for item in items:
            data = normalize_article(source, item)

            if not data["external_id"] or not data["title"]:
                skipped_count += 1
                continue

            upsert_article(session, data)
            imported_count += 1

        session.commit()

    print(
        f"✅ Импортировано: {source} -> {path.name} "
        f"(добавлено/обновлено: {imported_count}, пропущено: {skipped_count})"
    )


def import_all_processed():
    """
    Импортирует все processed-файлы в БД.
    """
    for source, path in PROCESSED_FILES.items():
        import_processed_file(source, path)