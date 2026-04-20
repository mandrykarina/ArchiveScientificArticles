import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import List, Dict, Any, Tuple

from sqlalchemy import select

from config import ARXIV_COUNT, CYBER_COUNT, CHINAXIV_COUNT
from processors.processor import init_models, process_file

# ====================== ИМПОРТ ПАРСЕРОВ ======================
from parsers.arxiv_feed_parser import ArxivParser
from parsers.cyberleninka_parser import CyberParser
from parsers.chinaxiv_harvester import ChinaRxivParser

# ====================== ИМПОРТ БД ======================
from app.db import Base, engine, SessionLocal
from app.models import Article
from app.importer import import_processed_file

RAW_DIR = Path("raw")
PROCESSED_DIR = Path("processed")

RAW_DIR.mkdir(exist_ok=True)
PROCESSED_DIR.mkdir(exist_ok=True)


def save_raw(articles: list, filename: str):
    """Сохраняет сырые данные от парсера в папку raw."""
    path = RAW_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)
    print(f"✅ Сохранено {len(articles)} новых сырых статей → {filename}")


def ensure_database():
    """Создает таблицы базы данных, если они еще не существуют."""
    print("🗄️ Инициализация базы данных...")
    Base.metadata.create_all(bind=engine)
    print("✅ База данных готова")


def deduplicate_incoming_articles(articles: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int, int]:
    """
    Убирает дубликаты внутри одного входного батча по полю id.
    Возвращает:
    - уникальные статьи
    - число дублей внутри батча
    - число статей без id
    """
    seen_ids = set()
    unique_articles = []
    duplicate_count = 0
    missing_id_count = 0

    for article in articles:
        article_id = str(article.get("id", "")).strip()

        if not article_id:
            missing_id_count += 1
            continue

        if article_id in seen_ids:
            duplicate_count += 1
            continue

        seen_ids.add(article_id)
        unique_articles.append(article)

    return unique_articles, duplicate_count, missing_id_count


def chunked(items: List[str], size: int):
    """Разбивает список на чанки."""
    for i in range(0, len(items), size):
        yield items[i:i + size]


def get_existing_article_ids(source: str, article_ids: List[str]) -> set[str]:
    """
    Возвращает множество external_id статей, которые уже есть в БД
    для конкретного источника.
    """
    if not article_ids:
        return set()

    existing_ids = set()

    with SessionLocal() as session:
        for batch in chunked(article_ids, 500):
            rows = session.execute(
                select(Article.external_id).where(
                    Article.source == source,
                    Article.external_id.in_(batch)
                )
            ).scalars().all()

            existing_ids.update(rows)

    return existing_ids


def filter_new_articles(source: str, articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Оставляет только новые статьи:
    - сначала убирает дубли внутри входного батча
    - затем исключает статьи, которые уже есть в БД
    """
    unique_articles, duplicate_count, missing_id_count = deduplicate_incoming_articles(articles)

    incoming_ids = [str(article["id"]).strip() for article in unique_articles if article.get("id")]
    existing_ids = get_existing_article_ids(source, incoming_ids)

    new_articles = []
    already_in_db_count = 0

    for article in unique_articles:
        article_id = str(article.get("id", "")).strip()
        if not article_id:
            continue

        if article_id in existing_ids:
            already_in_db_count += 1
            continue

        new_articles.append(article)

    print(
        f"🔎 {source}: получено={len(articles)}, "
        f"дубли_в_батче={duplicate_count}, "
        f"без_id={missing_id_count}, "
        f"уже_в_БД={already_in_db_count}, "
        f"новых={len(new_articles)}"
    )

    return new_articles


def run_pipeline():
    print(f"\n🚀 ЗАПУСК ИНКРЕМЕНТАЛЬНОГО ПАЙПЛАЙНА — {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("=" * 100)

    # 0. БД нужна заранее, чтобы проверять существующие статьи
    ensure_database()

    # 1. Загружаем модели обработки один раз
    print("🔄 Инициализация моделей обработки...")
    init_models()

    # 2. Параллельно запускаем все парсеры
    print("📡 Запуск парсеров...")
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(get_arxiv_articles),
            executor.submit(get_cyber_articles),
            executor.submit(get_chinaxiv_articles),
        ]

        for future in as_completed(futures):
            source_name, articles = future.result()

            if not articles:
                print(f"⚠️ {source_name} — ничего не получено")
                continue

            # 3. Оставляем только новые статьи
            new_articles = filter_new_articles(source_name, articles)

            if not new_articles:
                print(f"⏭️ {source_name} — новых статей нет, обработка пропущена")
                continue

            # 4. Сохраняем только новые статьи в raw
            raw_filename = f"{source_name}_articles.json"
            raw_path = RAW_DIR / raw_filename
            save_raw(new_articles, raw_filename)

            # 5. Обрабатываем raw -> processed только новые статьи
            print(f"⚙️ Обработка только новых статей: {raw_filename}...")
            process_file(raw_path)

            # 6. Импортируем только что созданный processed-файл в БД
            processed_filename = f"{raw_path.stem}_processed.json"
            processed_path = PROCESSED_DIR / processed_filename

            if processed_path.exists():
                print(f"📥 Импорт новых обработанных статей в БД: {processed_filename}...")
                import_processed_file(source_name, processed_path)
                print(f"✅ Импорт завершён: {source_name}")
            else:
                print(f"⚠️ Не найден processed-файл: {processed_path}")

    print("\n🎉 ИНКРЕМЕНТАЛЬНЫЙ ПАЙПЛАЙН ЗАВЕРШЁН УСПЕШНО!\n")


def get_arxiv_articles():
    print("📥 Запуск парсера arXiv...")
    parser = ArxivParser(delay_seconds=1.0)
    articles = parser.get_latest(ARXIV_COUNT)
    data = [parser.article_to_dict(article) for article in articles]
    return "arxiv", data


def get_cyber_articles():
    print("📥 Запуск парсера CyberLeninka...")
    parser = CyberParser(enrich_metadata=True)
    articles = parser.get_latest(CYBER_COUNT)
    data = [parser.article_to_dict(article) for article in articles]
    return "cyberleninka", data


def get_chinaxiv_articles():
    print("📥 Запуск парсера ChinaXiv...")
    parser = ChinaRxivParser(delay_seconds=0.6)
    articles = parser.get_latest(CHINAXIV_COUNT, fetch_html=True)
    data = [parser.article_to_dict(article) for article in articles]
    return "chinaxiv", data


if __name__ == "__main__":
    run_pipeline()