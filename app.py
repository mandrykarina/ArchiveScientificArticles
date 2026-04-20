from pathlib import Path
from flask import Flask, render_template, request, abort

from app.repository import (
    get_categories,
    get_subcategories,
    get_articles,
    search_articles,
    search_articles_scoped,
    get_article_by_id,
    get_authors,
    get_keywords,
)

BASE_DIR = Path(__file__).resolve().parent

app = Flask(
    __name__,
    template_folder=str(BASE_DIR / "templates"),
    static_folder=str(BASE_DIR / "static"),
)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/<bucket>")
def bucket_view(bucket):
    if bucket not in {"rus", "eng", "chn"}:
        abort(404)

    category = (request.args.get("category") or "").strip()
    subcategory = (request.args.get("subcategory") or "").strip()
    local_query = (request.args.get("local_q") or "").strip()

    categories = get_categories(bucket)
    subcategories = get_subcategories(bucket, category) if category else []

    if subcategory and subcategory not in subcategories:
        subcategory = ""

    if local_query:
        articles = search_articles_scoped(
            query=local_query,
            bucket=bucket,
            category=category or None,
            subcategory=subcategory or None,
        )
    else:
        articles = get_articles(
            bucket=bucket,
            category=category or None,
            subcategory=subcategory or None
        )

    bucket_titles = {
        "rus": "Rus",
        "eng": "Eng",
        "chn": "Chn",
    }

    return render_template(
        "bucket.html",
        bucket=bucket,
        bucket_title=bucket_titles[bucket],
        categories=categories,
        subcategories=subcategories,
        selected_category=category,
        selected_subcategory=subcategory,
        local_query=local_query,
        articles=articles,
    )


@app.route("/search")
def search_view():
    query = (request.args.get("q") or "").strip()
    bucket = (request.args.get("bucket") or "").strip()

    if bucket not in {"", "rus", "eng", "chn"}:
        abort(404)

    results = search_articles(bucket=bucket or None, query=query) if query else []

    return render_template(
        "search.html",
        query=query,
        bucket=bucket,
        results=results,
    )


@app.route("/article/<int:article_id>")
def article_view(article_id: int):
    article = get_article_by_id(article_id)
    if article is None:
        abort(404)

    authors = get_authors(article_id)
    keywords = get_keywords(article_id)

    return render_template(
        "article.html",
        article=article,
        authors=authors,
        keywords=keywords,
    )


if __name__ == "__main__":
    app.run(debug=True)