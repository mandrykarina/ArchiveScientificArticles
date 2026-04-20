from sqlalchemy import Column, Integer, String, Text, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import relationship

from app.db import Base


class Article(Base):
    __tablename__ = "articles"

    id = Column(Integer, primary_key=True)
    source = Column(String(32), nullable=False)
    bucket = Column(String(8), nullable=False)
    external_id = Column(String(255), nullable=False)

    title = Column(Text, nullable=False)
    abstract = Column(Text)
    url = Column(Text)
    pdf_url = Column(Text)
    doi = Column(String(255))

    category = Column(String(255))
    subcategory = Column(String(255))

    published_at = Column(String(64))
    updated_at = Column(String(64))

    journal_title = Column(Text)
    citation = Column(Text)
    raw_json = Column(Text, nullable=False)

    authors = relationship("ArticleAuthor", back_populates="article", cascade="all, delete-orphan")
    keywords = relationship("Keyword", back_populates="article", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("source", "external_id", name="uq_source_external_id"),
        Index("idx_articles_bucket", "bucket"),
        Index("idx_articles_source", "source"),
        Index("idx_articles_category", "category"),
        Index("idx_articles_subcategory", "subcategory"),
    )


class Author(Base):
    __tablename__ = "authors"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False, unique=True)

    articles = relationship("ArticleAuthor", back_populates="author")


class ArticleAuthor(Base):
    __tablename__ = "article_authors"

    article_id = Column(Integer, ForeignKey("articles.id", ondelete="CASCADE"), primary_key=True)
    author_id = Column(Integer, ForeignKey("authors.id", ondelete="CASCADE"), primary_key=True)

    article = relationship("Article", back_populates="authors")
    author = relationship("Author", back_populates="articles")


class Keyword(Base):
    __tablename__ = "keywords"

    id = Column(Integer, primary_key=True)
    article_id = Column(Integer, ForeignKey("articles.id", ondelete="CASCADE"), nullable=False)
    keyword = Column(String(255), nullable=False)

    article = relationship("Article", back_populates="keywords")

class ArticleSearch(Base):
    __tablename__ = "article_search"

    article_id = Column(Integer, ForeignKey("articles.id", ondelete="CASCADE"), primary_key=True)
    bucket = Column(String(8), nullable=False)
    source = Column(String(32), nullable=False)

    title_lemma = Column(Text)
    abstract_lemma = Column(Text)
    keywords_lemma = Column(Text)
    authors_lemma = Column(Text)
    category_lemma = Column(Text)
    subcategory_lemma = Column(Text)
    all_text_lemma = Column(Text, nullable=False)