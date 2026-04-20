from app.db import Base, engine
from app.importer import import_all_processed
from app.search_builder import rebuild_search_index

def main():
    Base.metadata.create_all(bind=engine)
    import_all_processed()
    rebuild_search_index()
    print("🎉 База данных и поисковый индекс собраны")

if __name__ == "__main__":
    main()