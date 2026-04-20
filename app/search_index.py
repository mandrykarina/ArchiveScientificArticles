import re
from typing import Iterable

import pymorphy3

morph = pymorphy3.MorphAnalyzer()

RU_WORD_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9\-]+", re.UNICODE)


def tokenize(text: str) -> list[str]:
    if not text:
        return []
    return RU_WORD_RE.findall(text.lower())


def is_cyrillic_token(token: str) -> bool:
    return bool(re.search(r"[а-яё]", token, re.IGNORECASE))


def normalize_token(token: str) -> str:
    token = token.strip().lower()
    if not token:
        return ""

    if is_cyrillic_token(token):
        try:
            parsed = morph.parse(token)
            if parsed:
                return parsed[0].normal_form
        except Exception:
            return token

    return token


def normalize_text(text: str) -> str:
    tokens = tokenize(text)
    normalized = [normalize_token(t) for t in tokens if t]
    return " ".join(t for t in normalized if t)


def combine_search_fields(
    title: str = "",
    abstract: str = "",
    keywords: Iterable[str] = (),
    authors: Iterable[str] = (),
    category: str = "",
    subcategory: str = "",
) -> dict:
    keywords_text = " ".join(k for k in keywords if k)
    authors_text = " ".join(a for a in authors if a)

    title_lemma = normalize_text(title)
    abstract_lemma = normalize_text(abstract)
    keywords_lemma = normalize_text(keywords_text)
    authors_lemma = normalize_text(authors_text)
    category_lemma = normalize_text(category)
    subcategory_lemma = normalize_text(subcategory)

    all_text_lemma = " ".join(
        part for part in [
            title_lemma,
            keywords_lemma,
            authors_lemma,
            category_lemma,
            subcategory_lemma,
            abstract_lemma,
        ] if part
    )

    return {
        "title_lemma": title_lemma,
        "abstract_lemma": abstract_lemma,
        "keywords_lemma": keywords_lemma,
        "authors_lemma": authors_lemma,
        "category_lemma": category_lemma,
        "subcategory_lemma": subcategory_lemma,
        "all_text_lemma": all_text_lemma,
    }


def normalize_query(query: str) -> str:
    return normalize_text(query)