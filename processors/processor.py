import json
from pathlib import Path
from tqdm import tqdm
import yake
import torch
import re
import html
import os
import requests
from transformers import MarianTokenizer, MarianMTModel
from concurrent.futures import ThreadPoolExecutor
from langdetect import detect, DetectorFactory

try:
    import pymorphy3
    MORPH = pymorphy3.MorphAnalyzer()
except Exception:
    MORPH = None

DetectorFactory.seed = 0
os.environ["HF_HUB_DISABLE_SYMLINKS_WARNING"] = "1"

# ====================== НАСТРОЙКИ ======================
YAKE_TOP_KEYWORDS = 12
MAX_WORKERS = 4

# ====================== YANDEX GPT ======================
YANDEX_API_KEY = os.getenv("YANDEX_API_KEY")
YANDEX_FOLDER_ID = os.getenv("YANDEX_FOLDER_ID")

print("\n" + "=" * 90)
print("🔍 ПРОВЕРКА YANDEX GPT")
if YANDEX_API_KEY and YANDEX_FOLDER_ID:
    print("✅ YandexGPT подключён")
    print(f"   Folder ID: {YANDEX_FOLDER_ID[:8]}... (скрыто)")
else:
    print("❌ YANDEX_API_KEY или YANDEX_FOLDER_ID НЕ ЗАДАНЫ!")
    print("   → YandexGPT НЕ БУДЕТ ИСПОЛЬЗОВАТЬСЯ")
    print("   → Всё будет переводиться только через Marian fallback")
print("=" * 90 + "\n")


def yandex_gpt_translate(text: str) -> str:
    if not YANDEX_API_KEY or not YANDEX_FOLDER_ID:
        return text

    print(f"   [YandexGPT] → Отправка текста ({len(text)} символов)")

    url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
    headers = {
        "Authorization": f"Api-Key {YANDEX_API_KEY}",
        "Content-Type": "application/json"
    }

    prompt = f"""Переведи текст на естественный литературный русский язык.
Сделай перевод красивым, научным и читаемым. Сохрани точный смысл.

Текст:
{text}"""

    body = {
        "modelUri": f"gpt://{YANDEX_FOLDER_ID}/yandexgpt/latest",
        "completionOptions": {"stream": False, "temperature": 0.3, "maxTokens": 2000},
        "messages": [
            {"role": "system", "text": "Ты — профессиональный научный переводчик."},
            {"role": "user", "text": prompt}
        ]
    }

    try:
        resp = requests.post(url, json=body, headers=headers, timeout=25)
        print(f"   [YandexGPT] Ответ сервера: {resp.status_code}")

        if resp.status_code != 200:
            print(f"   [YandexGPT] ОШИБКА {resp.status_code}: {resp.text[:400]}")
            return text

        result = resp.json()
        translated = result["result"]["alternatives"][0]["message"]["text"]
        print("   [YandexGPT] ✓ Успешно переведено")
        return translated.strip()

    except Exception as e:
        print(f"   [YandexGPT] КРИТИЧЕСКАЯ ОШИБКА: {e}")
        return text


# ====================== MARIAN (fallback) ======================
tokenizer_en_ru = None
model_en_ru = None
tokenizer_zh_en = None
model_zh_en = None


def init_models():
    global tokenizer_en_ru, model_en_ru, tokenizer_zh_en, model_zh_en
    print("🔄 Загружаем Marian (fallback)...")

    try:
        tokenizer_en_ru = MarianTokenizer.from_pretrained("Helsinki-NLP/opus-mt-en-ru")
        model_en_ru = MarianMTModel.from_pretrained("Helsinki-NLP/opus-mt-en-ru")
        model_en_ru.eval()
        print("   ✓ opus-mt-en-ru загружен")
    except Exception as e:
        print(f"   ❌ Не удалось загрузить en-ru: {e}")

    try:
        tokenizer_zh_en = MarianTokenizer.from_pretrained("Helsinki-NLP/opus-mt-zh-en")
        model_zh_en = MarianMTModel.from_pretrained("Helsinki-NLP/opus-mt-zh-en")
        model_zh_en.eval()
        print("   ✓ opus-mt-zh-en загружен")
    except Exception as e:
        print(f"   ⚠ Не удалось загрузить zh-en: {e} (будет использоваться только en-ru)")

    print("✅ Marian инициализирован\n")


# ====================== ОЧИСТКА И НОРМАЛИЗАЦИЯ ======================
KEYWORD_GARBAGE_PATTERNS = [
    r'пожалуйста[, ]*предостав[^\n]*',
    r'текст не содерж[^\n]*',
    r'более точный перевод\s*[:\-]?\s*',
    r'альтернативный перевод\s*[:\-]?\s*',
    r'вариант перевода\s*[:\-]?\s*',
    r'или\s*:\s*',
    r'перевод\s*:\s*',
    r'^слова?\s*:\s*',
    r'^keyword[s]?\s*:\s*',
    r'^ключевые слова\s*:\s*',
]

RUSSIAN_STOPWORDS_FOR_KEYWORDS = {
    "и", "или", "для", "в", "на", "по", "с", "со", "из", "к", "ко", "у", "о", "об",
    "от", "до", "под", "над", "при", "без", "через", "между", "не", "это", "тот",
    "та", "те", "данный", "данная", "данное", "данные", "иной", "другой", "такой",
    "основа", "подход", "система", "метод", "модель", "анализ", "результат",
    "работа", "исследование", "статья", "данные", "задача", "задачи"
}

BAD_KEYWORD_FRAGMENTS = [
    "пожалуйста",
    "предостав",
    "полный текст",
    "не содерж",
    "более точный перевод",
    "вариант перевода",
    "или:",
    "перевод:",
    "в данной работе",
    "в данной статье",
    "мы предлагаем",
    "мы представляем",
    "в этой статье",
    "данной работе",
    "данной статье",
]

BAD_KEYWORD_STARTS = [
    "в данной",
    "в этой",
    "мы ",
    "наш ",
    "авторы ",
    "данное ",
    "исследование ",
]

BAD_SINGLE_WORDS = {
    "основа", "подход", "система", "метод", "модель", "анализ", "результат",
    "работа", "исследование", "статья", "данные", "задача", "задачи",
    "одномерный", "двумерный", "трёхмерный", "одномерном", "двумерном",
    "трёхмерном", "случай", "пример", "использование", "применение",
    "создание", "управление", "структура", "функция", "инновация"
}


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = html.unescape(str(text))
    text = text.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    text = re.sub(r'&[a-zA-Z#0-9]+;', '', text)
    text = re.sub(r'\\[\w]+', '', text)
    text = re.sub(r'\$[^$]*\$', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def post_process_russian(text: str) -> str:
    if not text:
        return ""

    text = text.strip('"“”«»')
    replacements = {
        "В настоящем документе": "В данной статье",
        "В настоящей работе": "В данной работе",
        "Мы представляем": "Авторы предлагают",
        "Мы показываем": "Показано",
        "В результате": "В итоге",
        "На основе": "На основании",
        "С целью": "Для",
        "Было обнаружено": "Обнаружено",
        "Исследование показывает": "Исследование показало",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)

    text = text.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    text = re.sub(r'\s+([,.;:!?])', r'\1', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def remove_service_phrases(text: str) -> str:
    if not text:
        return ""

    text = clean_text(text)

    patterns = [
        r'более точный перевод\s*[:\-]?\s*',
        r'альтернативный перевод\s*[:\-]?\s*',
        r'вариант перевода\s*[:\-]?\s*',
        r'примечание\s*[:\-]?\s*',
    ]

    for pattern in patterns:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE)

    text = re.sub(r'\s+', ' ', text).strip()
    return text


def normalize_text_field(text: str) -> str:
    if not text:
        return ""
    text = clean_text(text)
    text = remove_service_phrases(text)
    text = post_process_russian(text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def normalize_short_label(text: str) -> str:
    text = normalize_text_field(text)
    if not text:
        return ""
    text = re.sub(r'[.。]+$', '', text).strip()
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def normalize_title(text: str) -> str:
    text = normalize_text_field(text)
    if not text:
        return ""
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r'\.(?=$)', '', text).strip()
    return text


def normalize_comment(text: str) -> str:
    text = normalize_text_field(text)
    if not text:
        return ""
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def has_cyrillic(text: str) -> bool:
    return bool(re.search(r'[А-Яа-яЁё]', text or ""))


def is_english_keyword(keyword: str) -> bool:
    if not keyword:
        return False
    keyword = str(keyword).strip()

    if has_cyrillic(keyword):
        return False

    latin = len(re.findall(r'[A-Za-z]', keyword))
    total_letters = len(re.findall(r'[A-Za-zА-Яа-яЁё]', keyword))
    return latin > 0 and (total_letters == 0 or latin / max(total_letters, 1) > 0.7)


def clean_keyword(keyword: str) -> str:
    if not keyword:
        return ""

    keyword = html.unescape(str(keyword))
    keyword = keyword.replace("\n", " ").replace("\r", " ").replace("\t", " ")

    for pattern in KEYWORD_GARBAGE_PATTERNS:
        keyword = re.sub(pattern, '', keyword, flags=re.IGNORECASE)

    keyword = re.sub(r'\([^)]*\)', '', keyword)
    keyword = keyword.replace("...", " ")
    keyword = keyword.replace("..", " ")
    keyword = keyword.replace("|", " ")
    keyword = keyword.replace("/", " / ")

    keyword = keyword.replace("—", " – ")
    keyword = keyword.replace("-", " - ")

    keyword = re.sub(r'\s+', ' ', keyword).strip()
    keyword = keyword.strip(" ,;:!?\t")
    keyword = re.sub(r'[.,;:!?]+$', '', keyword).strip()

    keyword = keyword.replace(" - ", " – ")
    keyword = re.sub(r'\s+', ' ', keyword).strip()

    if len(keyword) < 2:
        return ""

    lowered = keyword.casefold()
    if "предостав" in lowered and "текст" in lowered:
        return ""
    if "не содержит" in lowered or "не содерж" in lowered:
        return ""

    return keyword


def normalize_keyword_for_compare(keyword: str) -> str:
    if not keyword:
        return ""
    k = keyword.casefold()
    k = k.replace("ё", "е")
    k = k.replace("—", "-").replace("–", "-")
    k = re.sub(r'[^\w\s-]+', '', k)
    k = re.sub(r'\s+', ' ', k).strip()
    return k


def _should_preserve_token_as_is(token: str) -> bool:
    if not token:
        return True
    if re.fullmatch(r'[A-Z0-9][A-Z0-9\-+/.]*', token):
        return True
    if re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9\-+/.]*', token):
        return True
    if re.fullmatch(r'[A-ZА-ЯЁ]{2,}', token):
        return True
    return False


def _lemmatize_single_word(token: str) -> str:
    if not token:
        return token

    if _should_preserve_token_as_is(token):
        return token

    if MORPH is None:
        return token.lower()

    if not re.fullmatch(r'[А-Яа-яЁё]+', token):
        return token

    parsed = MORPH.parse(token)
    if not parsed:
        return token.lower()

    best = parsed[0]
    return best.normal_form


def normalize_keyword_phrase(keyword: str) -> str:
    """
    1 слово -> лемматизируем
    2+ слова -> не лемматизируем, только чистим
    """
    keyword = clean_keyword(keyword)
    if not keyword:
        return ""

    words = re.findall(r'[A-Za-zА-Яа-яЁё0-9+\-/.]+', keyword, flags=re.UNICODE)

    if len(words) <= 1:
        if not words:
            return ""
        single = words[0]
        normalized = _lemmatize_single_word(single)
        normalized = clean_keyword(normalized)
        return normalized

    keyword = re.sub(r'\s+', ' ', keyword).strip()
    keyword = re.sub(r'[.,;:!?]+$', '', keyword).strip()
    return keyword


def keyword_word_count(keyword: str) -> int:
    return len(re.findall(r'[A-Za-zА-Яа-яЁё0-9]+', keyword or ""))


def keyword_is_valid(keyword: str) -> bool:
    if not keyword:
        return False

    keyword = keyword.strip()
    norm = normalize_keyword_for_compare(keyword)
    if not norm:
        return False

    # Полностью выкидываем английские keywords
    if is_english_keyword(keyword):
        return False

    # Нужны только русские keywords
    if not has_cyrillic(keyword):
        return False

    wc = keyword_word_count(keyword)

    # Слишком длинные фразы почти всегда мусор
    if wc == 0 or wc > 6:
        return False

    lower_kw = keyword.casefold()

    if any(fragment in lower_kw for fragment in BAD_KEYWORD_FRAGMENTS):
        return False

    if any(lower_kw.startswith(start) for start in BAD_KEYWORD_STARTS):
        return False

    # Слишком общие однословные слова убираем
    if wc == 1:
        single = norm
        if single in RUSSIAN_STOPWORDS_FOR_KEYWORDS:
            return False
        if single in BAD_SINGLE_WORDS:
            return False
        if len(single) < 3:
            return False

    # Для многословных фраз отсекаем явные обрывки
    if wc >= 2:
        if re.search(r'\b(в|на|по|с|из|к|у|о|об|от|для|при|под|над)$', lower_kw):
            return False

        bad_endings = [
            "метода", "метод", "основе", "подхода", "подход", "работы",
            "исследования", "системы", "данных", "статьи"
        ]
        last_word_match = re.findall(r'[А-Яа-яЁё]+', lower_kw)
        if last_word_match:
            last_word = last_word_match[-1]
            if wc <= 2 and last_word in bad_endings:
                return False

    return True


def clean_keywords_list(keywords) -> list:
    if not keywords:
        return []

    cleaned = []
    seen = set()

    for kw in keywords:
        kw = normalize_keyword_phrase(kw)
        kw = clean_keyword(kw)

        if not kw or not keyword_is_valid(kw):
            continue

        norm = normalize_keyword_for_compare(kw)
        if not norm or norm in seen:
            continue

        seen.add(norm)
        cleaned.append(kw)

    return cleaned


def merge_keywords(base_keywords, extra_keywords) -> list:
    result = []
    seen = set()

    for group in [base_keywords or [], extra_keywords or []]:
        for kw in group:
            kw = normalize_keyword_phrase(kw)
            kw = clean_keyword(kw)

            if not kw or not keyword_is_valid(kw):
                continue

            norm = normalize_keyword_for_compare(kw)
            if not norm or norm in seen:
                continue

            seen.add(norm)
            result.append(kw)

    return result


def is_russian(text: str) -> bool:
    if not text:
        return True
    ru_chars = len(re.findall(r'[а-яА-ЯёЁ]', text))
    total = len(re.sub(r'\s+', '', text))
    return ru_chars / total > 0.25 if total > 0 else True


def detect_language(text: str) -> str:
    if not text or len(text.strip()) < 15:
        return "unknown"
    try:
        return detect(text.strip()[:400])
    except Exception:
        return "unknown"


# ====================== ПЕРЕВОД ======================
def translate_text(text: str) -> str:
    if not text or not str(text).strip():
        return ""

    cleaned = clean_text(str(text).strip())

    if is_russian(cleaned):
        return normalize_text_field(cleaned)

    if cleaned in CHINA_SUBJECT_FIX:
        return normalize_short_label(CHINA_SUBJECT_FIX[cleaned])

    print(f"   → Переводим: {cleaned[:70]}...")

    translated = yandex_gpt_translate(cleaned)
    if translated != cleaned:
        print("   ✓ YandexGPT успешно перевёл")
        return normalize_text_field(translated)

    print("   ⚠ YandexGPT не сработал → Marian")
    try:
        lang = detect_language(cleaned)
        if lang == "zh" and tokenizer_zh_en is not None and model_zh_en is not None:
            inputs = tokenizer_zh_en([cleaned], return_tensors="pt", padding=True, truncation=True, max_length=512)
            with torch.no_grad():
                en_tokens = model_zh_en.generate(**inputs, max_length=512)
            en_text = tokenizer_zh_en.batch_decode(en_tokens, skip_special_tokens=True)[0]

            inputs = tokenizer_en_ru([en_text], return_tensors="pt", padding=True, truncation=True, max_length=512)
            with torch.no_grad():
                ru_tokens = model_en_ru.generate(**inputs, max_length=512)
            result = tokenizer_en_ru.batch_decode(ru_tokens, skip_special_tokens=True)[0]
        else:
            inputs = tokenizer_en_ru([cleaned], return_tensors="pt", padding=True, truncation=True, max_length=512)
            with torch.no_grad():
                ru_tokens = model_en_ru.generate(**inputs, max_length=512)
            result = tokenizer_en_ru.batch_decode(ru_tokens, skip_special_tokens=True)[0]

        return normalize_text_field(result)
    except Exception as e:
        print(f"   ❌ Marian тоже упал: {e}")
        return normalize_text_field(cleaned)


def extract_keywords(text: str) -> list:
    if not text or len(str(text).strip()) < 20:
        return []
    extractor = yake.KeywordExtractor(lan="ru", n=3, top=YAKE_TOP_KEYWORDS)
    raw_keywords = [kw[0] for kw in extractor.extract_keywords(str(text))]
    return clean_keywords_list(raw_keywords)


# ====================== СЛОВАРИ ======================
ARXIV_SUBCATEGORY_MAP = {
    "quant-ph": "Квантовая физика",
    "cs.ai": "Искусственный интеллект",
    "cs.cl": "Обработка естественного языка",
    "cs.cv": "Компьютерное зрение и распознавание образов",
    "cs.lg": "Машинное обучение",
    "cs.ne": "Нейронные сети и глубокое обучение",
    "cs.ro": "Робототехника",
    "cs.hc": "Взаимодействие человека и компьютера",
    "cs.cy": "Компьютеры и общество",
    "physics.soc-ph": "Физика и общество",
    "physics.data-an": "Физика данных и анализ",
    "hep-ph": "Физика элементарных частиц и поля",
    "hep-th": "Теоретическая физика высоких энергий",
    "cond-mat": "Физика конденсированного состояния",
    "math.ag": "Алгебраическая геометрия",
    "math.co": "Комбинаторика",
    "math.oc": "Оптимизация и управление",
    "math.pr": "Теория вероятностей",
    "stat.ml": "Статистическое машинное обучение",
    "astro-ph.co": "Космология и внегалактическая астрофизика",
    "astro-ph.ep": "Астрофизика Земли и планет",
    "math.gr": "Теория групп",
    "math.ds": "Динамические системы",
    "math.st": "Теория статистики",
    "cs.it": "Теория информации",
    "cs.pl": "Языки программирования",
    "q-bio.cb": "Поведение клеток",
}

CHINA_SUBJECT_FIX = {
    "离散数学和组合数学": "Дискретная математика и комбинаторная математика",
    "应用心理学": "Прикладная психология",
    "认知心理学": "Когнитивная психология",
    "理论心理学": "Теоретическая психология",
    "物理学": "Физика",
    "天文学": "Астрономия",
    "地球科学": "Науки о Земле",
    "生物学": "Биология",
    "计算机科学": "Компьютерные науки",
    "数学": "Математика",
    "材料科学": "Материаловедение",
    "能源科学": "Энергетика",
    "信息科学与系统科学": "Информационные науки и системные науки",
    "力学": "Механика",
    "化学": "Химия",
    "心理学": "Психология",
    "农、林、牧、渔": "Сельское, лесное, животноводческое и рыбное хозяйство",
    "医学、药学": "Медицина и фармация",
    "工程与技术科学": "Инженерные и технические науки",
    "测绘科学技术": "Технологии картографирования и геодезии",
    "矿山工程技术": "Горное дело и технологии",
    "机械工程": "Машиностроение",
    "动力与电气工程": "Энергетика и электротехника",
    "核科学技术": "Ядерные технологии",
    "电子与通信技术": "Электроника и связь",
    "食品科学技术": "Пищевые технологии",
    "土木建筑工程": "Строительство и архитектура",
    "水利工程": "Гидротехника",
    "交通运输工程": "Транспортная инженерия",
    "航空、航天科学技术": "Авиация и космонавтика",
    "环境科学技术及资源科学技术": "Экологические технологии и технологии ресурсов",
    "安全科学技术": "Технологии безопасности",
    "管理学": "Менеджмент",
    "统计学": "Статистика",
    "语言学及应用语言学": "Лингвистика и прикладная лингвистика",
    "光学": "Оптика",
    "图书馆学、情报学": "Библиотековедение и информатика",
    "药物科学": "Фармацевтические науки",
    "地球物理和空间物理": "Геофизика и космическая физика",
    "冰冻圈科学领域研究": "Исследования криосферы",
    "护理学": "Сестринское дело",
    "法学": "Юриспруденция",
    "数字出版": "Цифровая публикация",
    "其他": "Другое",
    "社会心理学": "Социальная психология",
    "临床与咨询心理学": "Клиническая и консультационная психология",
    "核物理学": "Ядерная физика",
    "辐射物理与技术": "Радиационная физика и технологии",
    "粒子加速器": "Ускорители частиц",
}

CYBER_CATEGORY_MAP = {
    "Фундаментальная медицина": "Медицинские науки",
    "Клиническая медицина": "Медицинские науки",
    "Науки о здоровье": "Медицинские науки",
    "Биотехнологии в медицине": "Медицинские науки",
    "Медицина": "Медицинские науки",

    "Математика": "Естественные и точные науки",
    "Компьютерные и информационные науки": "Естественные и точные науки",
    "Физика": "Естественные и точные науки",
    "Химические науки": "Естественные и точные науки",
    "Науки о Земле и смежные экологические науки": "Естественные и точные науки",
    "Биологические науки": "Естественные и точные науки",

    "Строительство и архитектура": "Техника и технологии",
    "Электротехника, электронная техника, информационные технологии": "Техника и технологии",
    "Механика и машиностроение": "Техника и технологии",
    "Химические технологии": "Техника и технологии",
    "Технологии материалов": "Техника и технологии",
    "Медицинские технологии": "Техника и технологии",
    "Энергетика и рациональное природопользование": "Техника и технологии",
    "Экологические биотехнологии": "Техника и технологии",
    "Промышленные биотехнологии": "Техника и технологии",
    "Нанотехнологии": "Техника и технологии",
    "Техника и технологии": "Техника и технологии",

    "История и археология": "Гуманитарные науки",
    "Языкознание и литературоведение": "Гуманитарные науки",
    "Философия, этика, религиоведение": "Гуманитарные науки",
    "Искусствоведение": "Гуманитарные науки",

    "Сельское хозяйство, лесное хозяйство, рыбное хозяйство": "Сельскохозяйственные науки",
    "Животноводство и молочное дело": "Сельскохозяйственные науки",
    "Ветеринарные науки": "Сельскохозяйственные науки",
    "Агробиотехнологии": "Сельскохозяйственные науки",

    "Психологические науки": "Социальные науки",
    "Экономика и бизнес": "Социальные науки",
    "Науки об образовании": "Социальные науки",
    "Социологические науки": "Социальные науки",
    "Право": "Социальные науки",
    "Политологические науки": "Социальные науки",
    "Социальная и экономическая география": "Социальные науки",
    "СМИ (медиа) и массовые коммуникации": "Социальные науки",
}


# ====================== ПРОЦЕССОРЫ ======================
def process_arxiv(article: dict) -> dict:
    for field in ["title", "archive", "abstract", "subcategory", "comment"]:
        if article.get(field):
            article[field] = translate_text(article[field])

    if article.get("subcategory"):
        original_subcat = str(article["subcategory"]).strip()
        code = original_subcat.lower().strip().rstrip(".")
        article["subcategory"] = ARXIV_SUBCATEGORY_MAP.get(code, translate_text(original_subcat))
        article["subcategory"] = normalize_short_label(article["subcategory"])

    if article.get("abstract"):
        article["abstract"] = normalize_text_field(article["abstract"])

    if article.get("title"):
        article["title"] = normalize_title(article["title"])

    if article.get("archive"):
        article["archive"] = normalize_short_label(article["archive"])

    if article.get("comment"):
        article["comment"] = normalize_comment(article["comment"])

    combined = (article.get("title", "") + ". " + article.get("abstract", "")).strip()
    article["keywords"] = clean_keywords_list(extract_keywords(combined))
    return article


def process_cyberleninka(article: dict) -> dict:
    subcat = normalize_short_label(article.get("field_of_science", ""))
    article["field_of_science"] = subcat
    article["category"] = CYBER_CATEGORY_MAP.get(subcat, "Другое") if subcat else "Другое"

    for field in ["title", "abstract"]:
        if article.get(field):
            article[field] = translate_text(article[field])

    if article.get("title"):
        article["title"] = normalize_title(article["title"])

    if article.get("abstract"):
        article["abstract"] = normalize_text_field(article["abstract"])

    if article.get("keywords"):
        translated_keywords = [translate_text(k) for k in article["keywords"] if k]
        article["keywords"] = clean_keywords_list(translated_keywords)
    else:
        article["keywords"] = []

    return article


def process_chinaxiv(article: dict) -> dict:
    for field in ["title", "abstract", "domain", "subject"]:
        if article.get(field):
            article[field] = translate_text(article[field])

    if article.get("title"):
        article["title"] = normalize_title(article["title"])

    if article.get("abstract"):
        article["abstract"] = normalize_text_field(article["abstract"])

    if article.get("domain"):
        article["domain"] = normalize_short_label(article["domain"])

    if article.get("subject"):
        subject_raw = article["subject"]
        subject_raw = remove_service_phrases(subject_raw)
        article["subject"] = normalize_short_label(subject_raw)

    if article.get("subcategory"):
        article["subcategory"] = normalize_short_label(article["subcategory"])

    original_keywords = []
    if article.get("keywords"):
        translated_keywords = [translate_text(k) for k in article["keywords"] if k]
        original_keywords = clean_keywords_list(translated_keywords)

    combined = (article.get("title", "") + ". " + article.get("abstract", "")).strip()
    generated_keywords = extract_keywords(combined)

    article["keywords"] = merge_keywords(original_keywords, generated_keywords)
    return article


def process_file(file_path: Path):
    print(f"📂 Обрабатываем: {file_path.name}")
    with open(file_path, encoding="utf-8") as f:
        articles = json.load(f)

    if "arxiv" in file_path.name.lower():
        processor = process_arxiv
    elif "cyberleninka" in file_path.name.lower():
        processor = process_cyberleninka
    elif "chinaxiv" in file_path.name.lower():
        processor = process_chinaxiv
    else:
        return

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        processed = list(tqdm(executor.map(processor, articles), total=len(articles), desc="Обработка"))

    output_path = Path("processed") / (file_path.stem + "_processed.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(processed, f, ensure_ascii=False, indent=2)

    print(f"✅ Готово → {output_path.name}\n")


def main():
    init_models()
    files = [
        Path("raw/arxiv_articles.json"),
        Path("raw/cyberleninka_articles.json"),
        Path("raw/chinaxiv_articles.json")
    ]
    for file_path in files:
        if file_path.exists():
            process_file(file_path)
        else:
            print(f"⚠️ Файл {file_path.name} не найден")
    print("🎉 Обработка завершена!")


if __name__ == "__main__":
    main()