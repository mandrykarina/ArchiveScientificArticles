from sqlalchemy import select, or_
from app.db import SessionLocal
from app.models import Article, Author, ArticleAuthor, Keyword


def get_categories(bucket: str):
    with SessionLocal() as session:
        rows = session.execute(
            select(Article.category)
            .where(Article.bucket == bucket)
            .distinct()
            .order_by(Article.category.asc())
        ).scalars().all()
        return rows


def get_subcategories(bucket: str, category: str):
    with SessionLocal() as session:
        rows = session.execute(
            select(Article.subcategory)
            .where(
                Article.bucket == bucket,
                Article.category == category
            )
            .distinct()
            .order_by(Article.subcategory.asc())
        ).scalars().all()
        return rows


def get_articles(bucket: str, category: str | None = None, subcategory: str | None = None):
    with SessionLocal() as session:
        stmt = select(Article).where(Article.bucket == bucket)

        if category:
            stmt = stmt.where(Article.category == category)

        if subcategory:
            stmt = stmt.where(Article.subcategory == subcategory)

        stmt = stmt.order_by(Article.title.asc())

        return session.execute(stmt).scalars().all()


def build_search_patterns(query: str):
    query = (query or "").strip()
    if not query:
        return []

    variants = [
        query,
        query.lower(),
        query.upper(),
        query.title(),
    ]

    seen = set()
    result = []
    for v in variants:
        if v not in seen:
            seen.add(v)
            result.append(f"%{v}%")

    return result


def search_articles(query: str, bucket: str | None = None):
    if not query:
        return []

    patterns = build_search_patterns(query)

    with SessionLocal() as session:
        conditions = []

        for pattern in patterns:
            conditions.extend([
                Article.title.like(pattern),
                Article.abstract.like(pattern),
                Keyword.keyword.like(pattern),
                Author.name.like(pattern),
            ])

        stmt = (
            select(Article)
            .outerjoin(Keyword, Keyword.article_id == Article.id)
            .outerjoin(ArticleAuthor, ArticleAuthor.article_id == Article.id)
            .outerjoin(Author, Author.id == ArticleAuthor.author_id)
            .where(or_(*conditions))
            .distinct()
            .order_by(Article.title.asc())
        )

        if bucket:
            stmt = stmt.where(Article.bucket == bucket)

        return session.execute(stmt).scalars().all()


def search_articles_scoped(
    query: str,
    bucket: str,
    category: str | None = None,
    subcategory: str | None = None,
):
    """
    Поиск внутри текущей вкладки / категории / подкатегории.
    """
    if not query:
        return []

    patterns = build_search_patterns(query)

    with SessionLocal() as session:
        conditions = []

        for pattern in patterns:
            conditions.extend([
                Article.title.like(pattern),
                Article.abstract.like(pattern),
                Keyword.keyword.like(pattern),
                Author.name.like(pattern),
            ])

        stmt = (
            select(Article)
            .outerjoin(Keyword, Keyword.article_id == Article.id)
            .outerjoin(ArticleAuthor, ArticleAuthor.article_id == Article.id)
            .outerjoin(Author, Author.id == ArticleAuthor.author_id)
            .where(Article.bucket == bucket)
            .where(or_(*conditions))
            .distinct()
            .order_by(Article.title.asc())
        )

        if category:
            stmt = stmt.where(Article.category == category)

        if subcategory:
            stmt = stmt.where(Article.subcategory == subcategory)

        return session.execute(stmt).scalars().all()


def get_article_by_id(article_id: int):
    with SessionLocal() as session:
        stmt = select(Article).where(Article.id == article_id)
        return session.execute(stmt).scalar_one_or_none()


def get_authors(article_id: int):
    with SessionLocal() as session:
        rows = session.execute(
            select(Author.name)
            .join(ArticleAuthor, ArticleAuthor.author_id == Author.id)
            .where(ArticleAuthor.article_id == article_id)
            .order_by(Author.name.asc())
        ).scalars().all()
        return rows


def get_keywords(article_id: int):
    with SessionLocal() as session:
        rows = session.execute(
            select(Keyword.keyword)
            .where(Keyword.article_id == article_id)
            .order_by(Keyword.keyword.asc())
        ).scalars().all()
        return rows