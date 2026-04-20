from sqlalchemy import text, select

from app.db import SessionLocal, engine
from app.models import Article, ArticleSearch, Author, ArticleAuthor, Keyword
from app.search_index import combine_search_fields


def create_search_tables():
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS article_search (
                article_id INTEGER PRIMARY KEY,
                bucket TEXT NOT NULL,
                source TEXT NOT NULL,
                title_lemma TEXT,
                abstract_lemma TEXT,
                keywords_lemma TEXT,
                authors_lemma TEXT,
                category_lemma TEXT,
                subcategory_lemma TEXT,
                all_text_lemma TEXT NOT NULL,
                FOREIGN KEY(article_id) REFERENCES articles(id) ON DELETE CASCADE
            )
        """))

        conn.execute(text("""
            CREATE VIRTUAL TABLE IF NOT EXISTS article_search_fts
            USING fts5(
                bucket UNINDEXED,
                source UNINDEXED,
                all_text_lemma,
                content='article_search',
                content_rowid='article_id'
            )
        """))


def rebuild_search_index():
    create_search_tables()

    with SessionLocal() as session:
        articles = session.execute(select(Article)).scalars().all()

        session.execute(text("DELETE FROM article_search"))
        session.commit()

        for article in articles:
            authors = session.execute(
                select(Author.name)
                .join(ArticleAuthor, ArticleAuthor.author_id == Author.id)
                .where(ArticleAuthor.article_id == article.id)
            ).scalars().all()

            keywords = session.execute(
                select(Keyword.keyword)
                .where(Keyword.article_id == article.id)
            ).scalars().all()

            normalized = combine_search_fields(
                title=article.title or "",
                abstract=article.abstract or "",
                keywords=keywords,
                authors=authors,
                category=article.category or "",
                subcategory=article.subcategory or "",
            )

            row = ArticleSearch(
                article_id=article.id,
                bucket=article.bucket,
                source=article.source,
                title_lemma=normalized["title_lemma"],
                abstract_lemma=normalized["abstract_lemma"],
                keywords_lemma=normalized["keywords_lemma"],
                authors_lemma=normalized["authors_lemma"],
                category_lemma=normalized["category_lemma"],
                subcategory_lemma=normalized["subcategory_lemma"],
                all_text_lemma=normalized["all_text_lemma"],
            )
            session.merge(row)

        session.commit()

    with engine.begin() as conn:
        conn.execute(text("INSERT INTO article_search_fts(article_search_fts) VALUES ('rebuild')"))