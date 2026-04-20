"""
arXiv Parser Library
Чистая реализация с интеллектуальным планировщиком мониторинга.
"""
import json
import time
import arxiv
from datetime import datetime, timedelta, time
from typing import List, Optional, Tuple, Dict, Any

# Конфигурация расписания arXiv
ARXIV_PUBLICATION_UTC_HOUR = 1      # 01:00 UTC = 04:00 MSK
ARXIV_PUBLICATION_UTC_MINUTE = 30   # Время запуска: через 30 мин после публикации
DAYS_WITHOUT_PUBLICATIONS = [4, 5]  # Пятница (4) и Суббота (5)
ARXIV_API_MAX_RESULTS = 30000       # Максимальный лимит API arXiv

class ArxivParser:
    """Парсер arXiv с интеллектуальным планировщиком мониторинга."""

    # Полный словарь для основных категорий (архивов)
    ARXIV_ARCHIVES = {
        "cs": "Computer Science",
        "econ": "Economics",
        "eess": "Electrical Engineering and Systems Science",
        "math": "Mathematics",
        "astro-ph": "Astrophysics",
        "cond-mat": "Condensed Matter",
        "gr-qc": "General Relativity and Quantum Cosmology",
        "hep-ex": "High Energy Physics - Experiment",
        "hep-lat": "High Energy Physics - Lattice",
        "hep-ph": "High Energy Physics - Phenomenology",
        "hep-th": "High Energy Physics - Theory",
        "math-ph": "Mathematical Physics",
        "nlin": "Nonlinear Sciences",
        "nucl-ex": "Nuclear Experiment",
        "nucl-th": "Nuclear Theory",
        "physics": "Physics",
        "quant-ph": "Quantum Physics",
        "q-bio": "Quantitative Biology",
        "q-fin": "Quantitative Finance",
        "stat": "Statistics",
    }

    # Полный словарь для подкатегорий (все коды из таксономии arXiv)
    ARXIV_SUBCATEGORIES = {
        # Computer Science
        "cs.AI": "Artificial Intelligence",
        "cs.AR": "Hardware Architecture",
        "cs.CC": "Computational Complexity",
        "cs.CE": "Computational Engineering, Finance, and Science",
        "cs.CG": "Computational Geometry",
        "cs.CL": "Computation and Language",
        "cs.CR": "Cryptography and Security",
        "cs.CV": "Computer Vision and Pattern Recognition",
        "cs.CY": "Computers and Society",
        "cs.DB": "Databases",
        "cs.DC": "Distributed, Parallel, and Cluster Computing",
        "cs.DL": "Digital Libraries",
        "cs.DM": "Discrete Mathematics",
        "cs.DS": "Data Structures and Algorithms",
        "cs.ET": "Emerging Technologies",
        "cs.FL": "Formal Languages and Automata Theory",
        "cs.GL": "General Literature",
        "cs.GR": "Graphics",
        "cs.GT": "Computer Science and Game Theory",
        "cs.HC": "Human-Computer Interaction",
        "cs.IR": "Information Retrieval",
        "cs.IT": "Information Theory",
        "cs.LG": "Machine Learning",
        "cs.LO": "Logic in Computer Science",
        "cs.MA": "Multiagent Systems",
        "cs.MM": "Multimedia",
        "cs.MS": "Mathematical Software",
        "cs.NA": "Numerical Analysis",
        "cs.NE": "Neural and Evolutionary Computing",
        "cs.NI": "Networking and Internet Architecture",
        "cs.OH": "Other Computer Science",
        "cs.OS": "Operating Systems",
        "cs.PF": "Performance",
        "cs.PL": "Programming Languages",
        "cs.RO": "Robotics",
        "cs.SC": "Symbolic Computation",
        "cs.SD": "Sound",
        "cs.SE": "Software Engineering",
        "cs.SI": "Social and Information Networks",
        "cs.SY": "Systems and Control",
        # Economics
        "econ.EM": "Econometrics",
        "econ.GN": "General Economics",
        "econ.TH": "Theoretical Economics",
        # Electrical Engineering and Systems Science
        "eess.AS": "Audio and Speech Processing",
        "eess.IV": "Image and Video Processing",
        "eess.SP": "Signal Processing",
        "eess.SY": "Systems and Control",
        # Mathematics
        "math.AC": "Commutative Algebra",
        "math.AG": "Algebraic Geometry",
        "math.AP": "Analysis of PDEs",
        "math.AT": "Algebraic Topology",
        "math.CA": "Classical Analysis and ODEs",
        "math.CO": "Combinatorics",
        "math.CT": "Category Theory",
        "math.CV": "Complex Variables",
        "math.DG": "Differential Geometry",
        "math.DS": "Dynamical Systems",
        "math.FA": "Functional Analysis",
        "math.GM": "General Mathematics",
        "math.GN": "General Topology",
        "math.GR": "Group Theory",
        "math.GT": "Geometric Topology",
        "math.HO": "History and Overview",
        "math.IT": "Information Theory",
        "math.KT": "K-Theory and Homology",
        "math.LO": "Logic",
        "math.MG": "Metric Geometry",
        "math.MP": "Mathematical Physics",
        "math.NA": "Numerical Analysis",
        "math.NT": "Number Theory",
        "math.OA": "Operator Algebras",
        "math.OC": "Optimization and Control",
        "math.PR": "Probability",
        "math.QA": "Quantum Algebra",
        "math.RA": "Rings and Algebras",
        "math.RT": "Representation Theory",
        "math.SG": "Symplectic Geometry",
        "math.SP": "Spectral Theory",
        "math.ST": "Statistics Theory",
        # Astrophysics
        "astro-ph.CO": "Cosmology and Nongalactic Astrophysics",
        "astro-ph.EP": "Earth and Planetary Astrophysics",
        "astro-ph.GA": "Astrophysics of Galaxies",
        "astro-ph.HE": "High Energy Astrophysical Phenomena",
        "astro-ph.IM": "Instrumentation and Methods for Astrophysics",
        "astro-ph.SR": "Solar and Stellar Astrophysics",
        # Condensed Matter
        "cond-mat.dis-nn": "Disordered Systems and Neural Networks",
        "cond-mat.mes-hall": "Mesoscale and Nanoscale Physics",
        "cond-mat.mtrl-sci": "Materials Science",
        "cond-mat.other": "Other Condensed Matter",
        "cond-mat.quant-gas": "Quantum Gases",
        "cond-mat.soft": "Soft Condensed Matter",
        "cond-mat.stat-mech": "Statistical Mechanics",
        "cond-mat.str-el": "Strongly Correlated Electrons",
        "cond-mat.supr-con": "Superconductivity",
        # General Relativity and Quantum Cosmology
        "gr-qc": "General Relativity and Quantum Cosmology",
        # High Energy Physics
        "hep-ex": "High Energy Physics - Experiment",
        "hep-lat": "High Energy Physics - Lattice",
        "hep-ph": "High Energy Physics - Phenomenology",
        "hep-th": "High Energy Physics - Theory",
        # Mathematical Physics (alias)
        "math-ph": "Mathematical Physics",
        # Nonlinear Sciences
        "nlin.AO": "Adaptation and Self-Organizing Systems",
        "nlin.CD": "Chaotic Dynamics",
        "nlin.CG": "Cellular Automata and Lattice Gases",
        "nlin.PS": "Pattern Formation and Solitons",
        "nlin.SI": "Exactly Solvable and Integrable Systems",
        # Nuclear Experiment and Theory
        "nucl-ex": "Nuclear Experiment",
        "nucl-th": "Nuclear Theory",
        # Physics (general)
        "physics.acc-ph": "Accelerator Physics",
        "physics.ao-ph": "Atmospheric and Oceanic Physics",
        "physics.app-ph": "Applied Physics",
        "physics.atm-clus": "Atomic and Molecular Clusters",
        "physics.atom-ph": "Atomic Physics",
        "physics.bio-ph": "Biological Physics",
        "physics.chem-ph": "Chemical Physics",
        "physics.class-ph": "Classical Physics",
        "physics.comp-ph": "Computational Physics",
        "physics.data-an": "Data Analysis, Statistics and Probability",
        "physics.ed-ph": "Physics Education",
        "physics.flu-dyn": "Fluid Dynamics",
        "physics.gen-ph": "General Physics",
        "physics.geo-ph": "Geophysics",
        "physics.hist-ph": "History and Philosophy of Physics",
        "physics.ins-det": "Instrumentation and Detectors",
        "physics.med-ph": "Medical Physics",
        "physics.optics": "Optics",
        "physics.plasm-ph": "Plasma Physics",
        "physics.pop-ph": "Popular Physics",
        "physics.soc-ph": "Physics and Society",
        "physics.space-ph": "Space Physics",
        # Quantum Physics
        "quant-ph": "Quantum Physics",
        # Quantitative Biology
        "q-bio.BM": "Biomolecules",
        "q-bio.CB": "Cell Behavior",
        "q-bio.GN": "Genomics",
        "q-bio.MN": "Molecular Networks",
        "q-bio.NC": "Neurons and Cognition",
        "q-bio.OT": "Other Quantitative Biology",
        "q-bio.PE": "Populations and Evolution",
        "q-bio.QM": "Quantitative Methods",
        "q-bio.SC": "Subcellular Processes",
        "q-bio.TO": "Tissues and Organs",
        # Quantitative Finance
        "q-fin.CP": "Computational Finance",
        "q-fin.EC": "Economics",
        "q-fin.GN": "General Finance",
        "q-fin.MF": "Mathematical Finance",
        "q-fin.PM": "Portfolio Management",
        "q-fin.PR": "Pricing of Securities",
        "q-fin.RM": "Risk Management",
        "q-fin.ST": "Statistical Finance",
        "q-fin.TR": "Trading and Market Microstructure",
        # Statistics
        "stat.AP": "Applications",
        "stat.CO": "Computation",
        "stat.ME": "Methodology",
        "stat.ML": "Machine Learning",
        "stat.OT": "Other Statistics",
        "stat.TH": "Statistics Theory",
    }

    def __init__(self, delay_seconds: float = 3.0, num_retries: int = 3):
        self.client = arxiv.Client(
            delay_seconds=delay_seconds,
            num_retries=num_retries
        )

    @staticmethod
    def print_separator(char: str = "=", width: int = 80) -> None:
        """Печать разделительной линии."""
        print(char * width)

    def print_result(self, result: arxiv.Result, number: int) -> None:
        """Печать информации об одной статье с расшифровкой категорий."""
        print(f"\n{'-' * 80}")
        print(f"[{number}] ID: {result.get_short_id()}")
        print(f"    URL: {result.entry_id}")
        print(f"    Title: {result.title}")
        authors_str = ", ".join([author.name for author in result.authors])
        print(f"    Authors ({len(result.authors)}): {authors_str}")
        print(f"    Submitted: {result.published.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"    Updated: {result.updated.strftime('%Y-%m-%d %H:%M:%S UTC')}")

        # --- НОВЫЙ ВЫВОД КАТЕГОРИЙ С ПОЛНЫМИ СЛОВАРЯМИ ---
        category_code = result.primary_category
        if '.' in category_code:
            archive_code, subcategory_code = category_code.split('.', 1)
            archive_name = self.ARXIV_ARCHIVES.get(archive_code, archive_code)
            # Ищем полное название подкатегории
            subcategory_name = self.ARXIV_SUBCATEGORIES.get(category_code, subcategory_code)
            print(f"    Archive: {archive_name}")
            print(f"    Subcategory: {subcategory_name}")
        else:
            # Если категория без точки (например, старый формат или общий архив)
            category_name = self.ARXIV_ARCHIVES.get(category_code, category_code)
            print(f"    Category: {category_name}")

        print(f"    Summary ({len(result.summary)} chars):")
        summary_lines = result.summary.replace('\n', ' ').strip().split('. ')
        for i, line in enumerate(summary_lines[:3], 1):
            if line.strip():
                print(f"      {i}. {line.strip()}.")
        if result.comment:
            print(f"    Comment: {result.comment}")
        if result.journal_ref:
            print(f"    Journal: {result.journal_ref}")
        if result.doi:
            print(f"    DOI: {result.doi}")
        if result.pdf_url:
            print(f"    PDF: {result.pdf_url}")

    @staticmethod
    def check_parsing_window(now_utc: Optional[datetime] = None) -> Tuple[bool, int, str]:
        """
        Определяет возможность парсинга по расписанию arXiv.
        Возвращает: (можно_парсить, дней_для_парсинга, пояснение).
        """
        if now_utc is None:
            now_utc = datetime.utcnow()

        current_weekday = now_utc.weekday()
        current_time = now_utc.time()
        target_time = time(hour=ARXIV_PUBLICATION_UTC_HOUR, 
                          minute=ARXIV_PUBLICATION_UTC_MINUTE)
        #Надо подумать, вообще в пт и сб будем парсить или ну его, вообщем, функция подлежит при необходимости изменениям.
        # Проверяем дни без публикаций (пятница, суббота)
        if current_weekday in DAYS_WITHOUT_PUBLICATIONS:
            return False, 0, "Пятница/Суббота: день без публикаций arXiv"

        # Проверяем, наступило ли время запуска
        if current_time != target_time:
            return False, 0, f"Ожидание времени запуска ({target_time.strftime('%H:%M')} UTC)"

        # Определяем период для парсинга
        """
        if current_weekday == 0:    # Понедельник (пакет за сб, вс)
            return True, 2, "Понедельник: парсим пакет за выходные (сб, вс)"
        elif current_weekday == 1:  # Вторник
            return True, 1, "Вторник: ежедневный выпуск"
        elif current_weekday == 2:  # Среда
            return True, 1, "Среда: ежедневный выпуск"
        elif current_weekday == 3:  # Четверг
            return True, 1, "Четверг: ежедневный выпуск"
        elif current_weekday == 6:  # Воскресенье (пакет за чт, пт)
            return True, 2, "Воскресенье: парсим пакет за четверг-пятницу"
        """
        return True, 7, "Обычный парсинг"

    def _get_articles_for_date(self, target_date: datetime.date) -> List[arxiv.Result]:
        """Внутренний метод для получения статей за конкретную дату."""
        date_from = target_date.strftime("%Y%m%d0000")
        date_to = target_date.strftime("%Y%m%d2359")

        search = arxiv.Search(
            query=f"submittedDate:[{date_from} TO {date_to}]",
            max_results=ARXIV_API_MAX_RESULTS,
            sort_by=arxiv.SortCriterion.SubmittedDate,
            sort_order=arxiv.SortOrder.Descending
        )

        articles = []
        try:
            for result in self.client.results(search):
                articles.append(result)
        except Exception:
            pass
        
        return articles
        
    def _get_articles_for_period(self, days_to_parse: int) -> List[arxiv.Result]:
        """Собирает ВСЕ статьи за указанное количество предыдущих дней."""
        all_articles = []
        current_date = datetime.utcnow().date()
        
        for day_offset in range(1, days_to_parse + 1):
            target_date = current_date - timedelta(days=day_offset)
                      
            # Запрашиваем ВСЕ статьи за день
            day_articles = self._get_articles_for_date(target_date)
            all_articles.extend(day_articles)
            
            # Пауза между запросами за разные дни
            if day_offset < days_to_parse:
                time.sleep(3)  # 3 секунды паузы
        
        return all_articles

    def monitor_smart(self) -> List[arxiv.Result]:
        """Умный мониторинг на основе расписания arXiv."""
        can_parse, days_to_parse, explanation = self.check_parsing_window()
        if not can_parse or days_to_parse == 0:
            return []
        articles = self._get_articles_for_period(days_to_parse)
        return articles

    def get_latest(self, count: int = 100) -> List[arxiv.Result]:
        """Получение N последних статей по дате подачи (обходной режим)."""
        articles = []
        current_date = datetime.utcnow().date()
        days_back = 0

        while len(articles) < count and days_back < 365:
            date_from = current_date.strftime("%Y%m%d0000")
            date_to = current_date.strftime("%Y%m%d2359")

            search = arxiv.Search(
                query=f"submittedDate:[{date_from} TO {date_to}]",
                max_results=ARXIV_API_MAX_RESULTS,
                sort_by=arxiv.SortCriterion.SubmittedDate,
                sort_order=arxiv.SortOrder.Descending
            )

            try:
                for result in self.client.results(search):
                    articles.append(result)
                    if len(articles) >= count:
                        break
            except Exception:
                pass

            current_date -= timedelta(days=1)
            days_back += 1

        return articles[:count]
        
    def get_article_by_id(self, article_id: str) -> Optional[arxiv.Result]:
        """Получение полной информации об статье по её ID."""
        try:
            search = arxiv.Search(id_list=[article_id])
            results_iter = self.client.results(search)
            return next(results_iter)
        except StopIteration:
            return None
        except Exception:
            # Любая другая ошибка
            return None
            
    def print_statistics(self, articles: List[arxiv.Result], mode: str) -> None:
        """Вывод статистики по результатам парсинга."""
        self.print_separator()
        print("СТАТИСТИКА")
        self.print_separator()
        print(f"Режим: {mode}")
        print(f"Всего статей: {len(articles)}")

        if articles:
            earliest = min(articles, key=lambda x: x.published).published
            latest = max(articles, key=lambda x: x.updated).updated
            print(f"Диапазон подач: {earliest.strftime('%Y-%m-%d')} ... {latest.strftime('%Y-%m-%d')}")
            earliest_update = min(a.updated for a in articles)
            print(f"Диапазон обновлений: {earliest_update.strftime('%Y-%m-%d')} ... {latest.strftime('%Y-%m-%d')}")
    def article_to_dict(self, article: arxiv.Result) -> Dict[str, Any]:
        """
        Преобразует объект arxiv.Result в словарь, содержащий все поля,
        которые выводятся в print_result.
        """
        # Определяем архив и подкатегорию
        category_code = article.primary_category
        archive_name = category_code
        subcategory_name = None
        
        if '.' in category_code:
            archive_code, subcategory_code = category_code.split('.', 1)
            archive_name = self.ARXIV_ARCHIVES.get(archive_code, archive_code)
            subcategory_name = self.ARXIV_SUBCATEGORIES.get(category_code, subcategory_code)
        else:
            archive_name = self.ARXIV_ARCHIVES.get(category_code, category_code)
        
        # Формируем словарь
        result_dict = {
            "id": article.get_short_id(),
            "url": article.entry_id,
            "title": article.title,
            "authors": [author.name for author in article.authors],
            "submitted_date": article.published.strftime('%Y-%m-%d %H:%M:%S UTC'),
            "updated_date": article.updated.strftime('%Y-%m-%d %H:%M:%S UTC'),
            "archive": archive_name,
            "primary_category_code": category_code,
            "abstract": article.summary,
            "comment": article.comment if article.comment else None,
            "journal_ref": article.journal_ref if article.journal_ref else None,
            "doi": article.doi if article.doi else None,
            "pdf_url": article.pdf_url if article.pdf_url else None,
        }
        
        # Добавляем подкатегорию, если она есть
        if subcategory_name:
            result_dict["subcategory"] = subcategory_name
        
        return result_dict

    def export_articles_to_json(self, articles: List[arxiv.Result], filepath: str) -> None:
        """
        Экспортирует список статей в JSON-файл.
        Каждая статья представлена словарём, содержащим все поля из print_result.
        
        Args:
            articles: список объектов arxiv.Result
            filepath: путь к выходному JSON-файлу
        """
        data = [self.article_to_dict(article) for article in articles]
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Экспортировано {len(articles)} статей в {filepath}")