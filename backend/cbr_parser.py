"""
Парсер данных Центрального Банка РФ.

Порядок приоритетов:
1. Актуальный кэш (data/cache/)
2. Реальные данные с cbr.ru
3. Встроенные демо-данные (если ЦБ недоступен)
"""
import hashlib
import json
import logging
import math
import os
import random
import re
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
CACHE_DIR = DATA_DIR / "cache"
METRICS_DIR = DATA_DIR / "metrics"

CACHE_DIR.mkdir(parents=True, exist_ok=True)
METRICS_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Карта: ключевое слово из адреса → код федерального округа
# (используется при парсинге списка банков с сайта ЦБ)
# ---------------------------------------------------------------------------
_ADDR_TO_DISTRICT: dict[str, str] = {
    # ЦФО
    "Москва": "ЦФО", "Московская область": "ЦФО",
    "Белгород": "ЦФО", "Белгородская": "ЦФО",
    "Брянск": "ЦФО", "Брянская": "ЦФО",
    "Владимир": "ЦФО", "Владимирская": "ЦФО",
    "Воронеж": "ЦФО", "Воронежская": "ЦФО",
    "Иваново": "ЦФО", "Ивановская": "ЦФО",
    "Калуга": "ЦФО", "Калужская": "ЦФО",
    "Кострома": "ЦФО", "Костромская": "ЦФО",
    "Курск": "ЦФО", "Курская": "ЦФО",
    "Липецк": "ЦФО", "Липецкая": "ЦФО",
    "Орёл": "ЦФО", "Орловская": "ЦФО",
    "Рязань": "ЦФО", "Рязанская": "ЦФО",
    "Смоленск": "ЦФО", "Смоленская": "ЦФО",
    "Тамбов": "ЦФО", "Тамбовская": "ЦФО",
    "Тверь": "ЦФО", "Тверская": "ЦФО",
    "Тула": "ЦФО", "Тульская": "ЦФО",
    "Ярославль": "ЦФО", "Ярославская": "ЦФО",
    "Мытищи": "ЦФО", "Химки": "ЦФО", "Одинцово": "ЦФО",
    # СЗФО
    "Санкт-Петербург": "СЗФО", "Ленинградская": "СЗФО",
    "Архангельск": "СЗФО", "Архангельская": "СЗФО",
    "Вологда": "СЗФО", "Вологодская": "СЗФО", "Череповец": "СЗФО",
    "Калининград": "СЗФО", "Калининградская": "СЗФО",
    "Мурманск": "СЗФО", "Мурманская": "СЗФО",
    "Петрозаводск": "СЗФО", "Республика Карелия": "СЗФО",
    "Псков": "СЗФО", "Псковская": "СЗФО",
    "Сыктывкар": "СЗФО", "Республика Коми": "СЗФО",
    "Великий Новгород": "СЗФО", "Новгородская": "СЗФО",
    "Нарьян-Мар": "СЗФО",
    # ЮФО
    "Краснодар": "ЮФО", "Краснодарский": "ЮФО",
    "Ростов-на-Дону": "ЮФО", "Ростовская": "ЮФО",
    "Астрахань": "ЮФО", "Астраханская": "ЮФО",
    "Волгоград": "ЮФО", "Волгоградская": "ЮФО",
    "Симферополь": "ЮФО", "Республика Крым": "ЮФО",
    "Севастополь": "ЮФО",
    "Майкоп": "ЮФО", "Республика Адыгея": "ЮФО",
    "Элиста": "ЮФО", "Республика Калмыкия": "ЮФО",
    # СКФО
    "Ставрополь": "СКФО", "Ставропольский": "СКФО",
    "Кисловодск": "СКФО", "Пятигорск": "СКФО",
    "Ессентуки": "СКФО", "Минеральные Воды": "СКФО",
    "Нальчик": "СКФО", "Кабардино-Балкарская": "СКФО",
    "Махачкала": "СКФО", "Республика Дагестан": "СКФО",
    "Дербент": "СКФО", "Избербаш": "СКФО",
    "Владикавказ": "СКФО", "Республика Северная Осетия": "СКФО",
    "Грозный": "СКФО", "Чеченская Республика": "СКФО",
    "Черкесск": "СКФО", "Карачаево-Черкесская": "СКФО",
    "Магас": "СКФО", "Назрань": "СКФО", "Республика Ингушетия": "СКФО",
    # ПФО
    "Казань": "ПФО", "Республика Татарстан": "ПФО",
    "Уфа": "ПФО", "Республика Башкортостан": "ПФО",
    "Нижний Новгород": "ПФО", "Нижегородская": "ПФО",
    "Самара": "ПФО", "Самарская": "ПФО", "Тольятти": "ПФО",
    "Пермь": "ПФО", "Пермский": "ПФО",
    "Саратов": "ПФО", "Саратовская": "ПФО",
    "Ульяновск": "ПФО", "Ульяновская": "ПФО",
    "Оренбург": "ПФО", "Оренбургская": "ПФО",
    "Пенза": "ПФО", "Пензенская": "ПФО",
    "Ижевск": "ПФО", "Удмуртская": "ПФО",
    "Чебоксары": "ПФО", "Чувашская": "ПФО",
    "Йошкар-Ола": "ПФО", "Республика Марий Эл": "ПФО",
    "Киров": "ПФО", "Кировская": "ПФО",
    "Саранск": "ПФО", "Республика Мордовия": "ПФО",
    # УФО
    "Екатеринбург": "УФО", "Свердловская": "УФО",
    "Тюмень": "УФО", "Тюменская": "УФО",
    "Челябинск": "УФО", "Челябинская": "УФО",
    "Курган": "УФО", "Курганская": "УФО",
    "Ханты-Мансийск": "УФО", "Ханты-Мансийский": "УФО",
    "Салехард": "УФО", "Ямало-Ненецкий": "УФО",
    "Сургут": "УФО", "Нижневартовск": "УФО", "Нижний Тагил": "УФО",
    # СФО
    "Новосибирск": "СФО", "Новосибирская": "СФО",
    "Красноярск": "СФО", "Красноярский": "СФО",
    "Омск": "СФО", "Омская": "СФО",
    "Барнаул": "СФО", "Алтайский": "СФО",
    "Томск": "СФО", "Томская": "СФО",
    "Кемерово": "СФО", "Кемеровская": "СФО", "Кузбасс": "СФО", "Новокузнецк": "СФО",
    "Иркутск": "СФО", "Иркутская": "СФО",
    "Чита": "СФО", "Забайкальский": "СФО",
    "Улан-Удэ": "СФО", "Республика Бурятия": "СФО",
    "Абакан": "СФО", "Республика Хакасия": "СФО",
    "Горно-Алтайск": "СФО", "Республика Алтай": "СФО",
    # ДФО
    "Владивосток": "ДФО", "Приморский": "ДФО",
    "Хабаровск": "ДФО", "Хабаровский": "ДФО",
    "Якутск": "ДФО", "Республика Саха": "ДФО", "Якутия": "ДФО",
    "Благовещенск": "ДФО", "Амурская": "ДФО",
    "Магадан": "ДФО", "Магаданская": "ДФО",
    "Южно-Сахалинск": "ДФО", "Сахалинская": "ДФО",
    "Петропавловск-Камчатский": "ДФО", "Камчатский": "ДФО",
    "Биробиджан": "ДФО", "Еврейская автономная": "ДФО",
    "Анадырь": "ДФО", "Чукотский": "ДФО",
}

# Двусловные названия городов: первое слово → ожидаемое второе слово
_TWO_WORD_CITIES = {
    "нижний": "Новгород", "великий": "Новгород", "петропавловск": "Камчатский",
    "южно": "Сахалинск", "горно": "Алтайск", "йошкар": "Ола",
    "нарьян": "Мар", "ханты": "Мансийск", "нижний тагил": None,
}

# Тип-слова улиц (не часть названия города)
_STREET_WORDS = {
    "улица", "ул", "проспект", "пр", "площадь", "бульвар", "набережная",
    "переулок", "пер", "шоссе", "шос", "тупик", "переулок",
    "партизанский", "большой", "малый", "верхний", "нижний", "средний",
}


def _extract_city_from_address(address: str) -> tuple[str, str]:
    """Извлекает название города и код федерального округа из полного адреса ЦБ РФ.

    Возвращает (city_display, district_code).
    Например: '690106 Приморский край г.Владивосток...' → ('Владивосток', 'ДФО')
    """
    # Убираем почтовый индекс и «Российская Федерация»
    addr = re.sub(r'^\d{6}[,\s]*', '', address).strip()
    addr = re.sub(r'Российская Федерация,?\s*', '', addr).strip().lstrip(',').strip()
    addr = re.sub(r'^\d{6}[,\s]*', '', addr).strip()  # повторно: «Рос. Федерация, 150003, город...»

    city = ""

    # Особые случаи: города без «г.» в начале
    sp = re.search(r'\b(Санкт-Петербург|Москва|Севастополь)\b', addr)
    if sp:
        city = sp.group(1)

    # Основной паттерн: «г.» / «г. » / «город » перед названием
    if not city:
        m = re.search(r'\b(?:г\.?\s*|город\s+)([А-ЯЁ][а-яёА-ЯЁ\-]+)', addr)
        if m:
            # Первое слово — однозначно название города
            w1 = m.group(1)
            # Проверяем, не является ли следующее слово "улица/проспект"-типом
            rest = addr[m.end():].lstrip()
            w2_m = re.match(r'([А-ЯЁа-яё][а-яёА-ЯЁ\-]+)', rest)
            w2 = w2_m.group(1).lower() if w2_m else ""
            if w1.lower() in _TWO_WORD_CITIES and w2 and w2 not in _STREET_WORDS:
                # Двусловный город: «Нижний Новгород», «Южно-Сахалинск»
                city = f"{w1} {w2_m.group(1)}" if w2_m else w1
            else:
                city = w1

    # Последний fallback: первый токен до запятой
    if not city:
        city = re.sub(r'^(г\.о\.?\s*|город-курорт\s*)', '', addr.split(',')[0], flags=re.IGNORECASE).strip()

    city = re.sub(r'\s+', ' ', city).strip()

    # Определяем федеральный округ: ищем ключевые слова в полном адресе
    # Проверяем от длинных ключей к коротким (чтобы «Нижний Новгород» > «Нижний»)
    district = "Прочие"
    addr_lower = address.lower()
    for kw in sorted(_ADDR_TO_DISTRICT, key=len, reverse=True):
        if kw.lower() in addr_lower:
            district = _ADDR_TO_DISTRICT[kw]
            break

    return city or "—", district


# ---------------------------------------------------------------------------
# Справочник реальных крупнейших банков РФ
# ---------------------------------------------------------------------------
KNOWN_BANKS = [
    {"reg_num": "1481", "name": "ПАО Сбербанк",              "short": "Сбербанк",        "region": "Москва",           "rank_approx": 1},
    {"reg_num": "1000", "name": "Банк ВТБ (ПАО)",            "short": "ВТБ",             "region": "Санкт-Петербург",  "rank_approx": 2},
    {"reg_num": "354",  "name": "АО «Газпромбанк»",          "short": "Газпромбанк",     "region": "Москва",           "rank_approx": 3},
    {"reg_num": "1326", "name": "АО «Альфа-Банк»",           "short": "Альфа-Банк",      "region": "Москва",           "rank_approx": 4},
    {"reg_num": "3349", "name": "АО «Россельхозбанк»",       "short": "Россельхозбанк",  "region": "Москва",           "rank_approx": 5},
    {"reg_num": "1978", "name": "АО «МКБ»",                  "short": "МКБ",             "region": "Москва",           "rank_approx": 6},
    {"reg_num": "3251", "name": "АО «ПСБ»",                  "short": "ПСБ",             "region": "Москва",           "rank_approx": 7},
    {"reg_num": "963",  "name": "ПАО «Совкомбанк»",          "short": "Совкомбанк",      "region": "Кострома",         "rank_approx": 8},
    {"reg_num": "2673", "name": "АО «ТБанк»",                "short": "Т-Банк",          "region": "Москва",           "rank_approx": 9},
    {"reg_num": "2748", "name": "ПАО «Росбанк»",             "short": "Росбанк",         "region": "Москва",           "rank_approx": 10},
    # Открытие влилось в ВТБ — завершение реорганизации январь 2023
    {"reg_num": "2209", "name": "Банк «Открытие» (ПАО)",     "short": "Открытие",        "region": "Москва",           "rank_approx": 11, "active_to": "2023-01-01",
     "status": "merged", "status_label": "Присоединён к ВТБ (янв. 2023)"},
    # reg_num требует верификации через ЦБ РФ (Data101FNew -> поле cname)
    {"reg_num": "2268", "name": "ПАО «МТС-Банк»",            "short": "МТС-Банк",        "region": "Москва",           "rank_approx": 14},
    {"reg_num": "2590", "name": "ПАО «АК БАРС» БАНК",        "short": "Ак Барс",         "region": "Казань",           "rank_approx": 18},
    {"reg_num": "2275", "name": "АО «БАНК УРАЛСИБ»",         "short": "УРАЛСИБ",         "region": "Москва",           "rank_approx": 17},
    {"reg_num": "705",  "name": "ПАО «БАНК СИНАРА»",         "short": "Синара",          "region": "Екатеринбург",     "rank_approx": 20},
    {"reg_num": "3292", "name": "АО «Райффайзенбанк»",       "short": "Райффайзен",      "region": "Москва",           "rank_approx": 30},
    # Лицензия отозвана ЦБ РФ 22 июля 2022
    {"reg_num": "2354", "name": "ООО «Тойота Банк»",         "short": "Тойота Банк",     "region": "Москва",           "rank_approx": 60, "active_to": "2022-08-01",
     "status": "revoked", "status_label": "Лицензия отозвана 22.07.2022"},
    {"reg_num": "2733", "name": "ПАО СКБ Приморья «Примсоцбанк»",   "short": "Примсоцбанк",    "region": "Приморский край", "rank_approx": 85},
    # Региональные банки Дальнего Востока
    {"reg_num": "507",  "name": "ПАО Банк «Приморье»",              "short": "Банк Приморье",  "region": "Приморский край", "rank_approx": 100},
    {"reg_num": "2460", "name": "АО «Примтеркомбанк»",              "short": "Примтеркомбанк", "region": "Приморский край", "rank_approx": 135},
    {"reg_num": "843",  "name": "АО «Дальневосточный банк»",        "short": "ДВ банк",        "region": "Приморский край", "rank_approx": 110},
    {"reg_num": "2404", "name": "АО «АТБ»",                         "short": "АТБ",            "region": "Хабаровск",       "rank_approx": 55},
    {"reg_num": "2244", "name": "ПАО «Далькомбанк»",                "short": "Далькомбанк",    "region": "Хабаровск",       "rank_approx": 140},
]

# Генерируем дополнительные банки (21-200, исключая известные)
_KNOWN_REGS = {b["reg_num"] for b in KNOWN_BANKS}
_EXTRA_BANK_TEMPLATES = [
    ("Региональный", ["банк", "кредитный банк", "финансовый банк"]),
    ("Северный", ["банк", "коммерческий банк", "инвестиционный банк"]),
    ("Южный", ["банк", "коммерческий банк"]),
    ("Байкальский", ["банк", "кредитный банк"]),
    ("Западный", ["банк", "кредитный банк"]),
    ("Центральный", ["банк", "расчётный банк"]),
    ("Капитал", ["банк", "банк"]),
    ("Империал", ["банк", "банк"]),
    ("Ренессанс", ["банк", "кредит"]),
    ("Финанс", ["банк", "капитал"]),
]
_REGIONS = [
    "Москва", "Санкт-Петербург", "Екатеринбург", "Новосибирск",
    "Казань", "Краснодар", "Воронеж", "Ростов-на-Дону", "Омск",
    "Самара", "Нижний Новгород", "Уфа", "Красноярск", "Пермь",
    "Приморский край", "Хабаровск", "Иркутск", "Тюмень", "Челябинск",
]


def _build_full_bank_list() -> list[dict]:
    """Формирует полный список из 150 банков."""
    banks = list(KNOWN_BANKS)
    rng = random.Random(42)
    reg_counter = 100
    rank = 21
    for _ in range(150 - len(KNOWN_BANKS)):
        # Подобрать незанятый рег. номер
        while str(reg_counter) in _KNOWN_REGS:
            reg_counter += 1
        prefix, suffixes = rng.choice(_EXTRA_BANK_TEMPLATES)
        suffix = rng.choice(suffixes)
        name = f'АО «{prefix} {suffix.capitalize()}»'
        banks.append({
            "reg_num": str(reg_counter),
            "name": name,
            "short": f"{prefix} {suffix.split()[0].capitalize()}",
            "region": rng.choice(_REGIONS),
            "rank_approx": rank,
        })
        reg_counter += rng.randint(1, 30)
        rank += 1
    return banks


ALL_BANKS: list[dict] = _build_full_bank_list()
_BANKS_BY_REG: dict[str, dict] = {b["reg_num"]: b for b in ALL_BANKS}


# ---------------------------------------------------------------------------
# Генератор демо-данных
# ---------------------------------------------------------------------------
PRIMSOCBANK_REG = "2733"
_SECTOR_ASSETS_2024 = 115_000  # млрд ₽ (весь сектор, ориентировочно)


def _bank_base_assets(rank: int, reg_num: str = "") -> float:
    """Активы банка на основе его места (степенной закон).

    Реальные ориентиры (базовый год ~2020, млрд ₽):
      Ранг  1 (Сбер):        ~25 000
      Ранг 10 (топ банки):   ~1 000
      Ранг 85 (Примсоцбанк): ~55
      Ранг 150 (малые):      ~5-10
    Экспонента 1.46 даёт корректный диапазон вместо прежних 0.85.
    """
    # Жёсткий оверрайд для Примсоцбанка — базовые активы ~2020 г.
    # Целевые значения 2026: активы ~129 млрд, депозиты ~87 млрд, ранг ~100-110
    if reg_num == PRIMSOCBANK_REG:
        return 55.0

    return round(50_000 / (rank ** 1.46), 2)


def _generate_metrics(
    reg_num: str,
    ref_date: date,
    base_assets: float,
) -> dict:
    """
    Генерирует финансовые показатели банка на заданную дату.
    Детерминировано: одинаковый вход -> одинаковый вывод.

    Используются два независимых генератора:
      ratio_rng  — seed только от reg_num → стабильные коэффициенты баланса
                   (не меняются от месяца к месяцу, иначе возникают ложные скачки)
      month_rng  — seed от reg_num + дата → небольшие ежемесячные флуктуации
    """
    # Стабильный seed для структурных коэффициентов (только по банку)
    ratio_seed = int(hashlib.md5(f"{reg_num}:ratios".encode()).hexdigest()[:8], 16)
    ratio_rng  = random.Random(ratio_seed)

    # Ежемесячный seed для флуктуаций
    month_seed = int(hashlib.md5(f"{reg_num}:{ref_date.year}:{ref_date.month}".encode()).hexdigest()[:8], 16)
    month_rng  = random.Random(month_seed)

    # Стабильный годовой темп роста для банка
    annual_rate = ratio_rng.uniform(0.08, 0.15)
    # Небольшая ежемесячная флуктуация ±1 п.п. (для реализма, не меняет тренд)
    monthly_noise = month_rng.uniform(-0.008, 0.008)

    years  = (ref_date.year - 2020) + ref_date.month / 12
    growth = (1 + annual_rate + monthly_noise) ** years
    assets = round(base_assets * growth, 2)

    # Все коэффициенты — из стабильного генератора банка
    capital_ratio    = ratio_rng.uniform(0.09, 0.18)
    loans_ratio      = ratio_rng.uniform(0.55, 0.72)
    deposits_ratio   = ratio_rng.uniform(0.60, 0.78)
    provisions_ratio = ratio_rng.uniform(0.03, 0.07)

    capital    = round(assets * capital_ratio, 2)
    loans      = round(assets * loans_ratio, 2)
    deposits   = round(assets * deposits_ratio, 2)
    provisions = round(assets * provisions_ratio, 2)
    liabilities = round(assets - capital, 2)

    roa = ratio_rng.uniform(0.5, 3.5)
    profit = round(assets * roa / 100, 2)
    roe    = round(roa / capital_ratio, 2)

    int_income_rate = ratio_rng.uniform(0.06, 0.12)
    int_expense_r   = ratio_rng.uniform(0.35, 0.60)
    int_income  = round(assets * int_income_rate, 2)
    int_expense = round(int_income * int_expense_r, 2)
    earning_assets = round(assets * 0.85, 2)
    nim = round((int_income - int_expense) / earning_assets * 100, 2)

    npl = round(ratio_rng.uniform(1.0, 8.0), 2)

    n1 = round(ratio_rng.uniform(10.5, 22.0), 2)  # мин 8 %
    n2 = round(ratio_rng.uniform(50.0, 180.0), 2)  # мин 15 %
    n3 = round(ratio_rng.uniform(80.0, 200.0), 2)  # мин 50 %
    n4 = round(ratio_rng.uniform(30.0, 90.0), 2)  # макс 120 %

    # Детализация кредитного портфеля
    loans_fl_r = ratio_rng.uniform(0.38, 0.62)
    loans_fl   = round(loans * loans_fl_r, 2)
    loans_yl   = round(loans * (1 - loans_fl_r), 2)
    npl_abs    = round(loans * npl / 100, 2)

    # Детализация вкладов
    dep_fl_ratio = ratio_rng.uniform(0.40, 0.60)
    dep_fl_term  = round(deposits * dep_fl_ratio * ratio_rng.uniform(0.78, 0.95), 2)
    dep_522      = round(deposits * dep_fl_ratio * ratio_rng.uniform(0.0, 0.04), 2)
    deposits_fl  = round(dep_fl_term + dep_522, 2)
    dep_408      = round(deposits * ratio_rng.uniform(0.06, 0.14), 2)
    deposits_yl  = round(deposits - deposits_fl, 2)

    # Примсоцбанк: override deposits_fl по реальным данным
    # Реальное соотношение: deposits_fl / assets ≈ 0.694 (из данных ЦБ 2025–2026)
    # Обеспечивает значение >70 млрд в 2026 г. без скачков между месяцами
    if reg_num == "2733":
        _psb_fl_ratio = month_rng.uniform(0.688, 0.700)  # ±0.6% ежемесячная флуктуация
        deposits_fl  = round(assets * _psb_fl_ratio, 2)
        dep_fl_term  = round(deposits_fl * 0.975, 2)
        dep_522      = round(deposits_fl - dep_fl_term, 2)
        deposits_yl  = round(deposits - deposits_fl, 2)

    # Дополнительные показатели структуры баланса
    cash          = round(assets * ratio_rng.uniform(0.02, 0.06), 2)
    nostro        = round(assets * ratio_rng.uniform(0.04, 0.09), 2)
    liquid_assets = round(cash + nostro + assets * ratio_rng.uniform(0.01, 0.025), 2)
    securities    = round(assets * ratio_rng.uniform(0.06, 0.18), 2)
    mbk_given     = round(assets * ratio_rng.uniform(0.02, 0.07), 2)
    fixed_assets  = round(assets * ratio_rng.uniform(0.01, 0.025), 2)

    return {
        "assets":           assets,
        "capital":          capital,
        "loans":            loans,
        "loans_fl":         loans_fl,
        "loans_yl":         loans_yl,
        "npl_abs":          npl_abs,
        "deposits":         deposits,
        "deposits_fl":      deposits_fl,
        "deposits_fl_term": dep_fl_term,
        "dep_522":          dep_522,
        "dep_408":          dep_408,
        "deposits_yl":      deposits_yl,
        "liabilities":      liabilities,
        "profit":           profit,
        "provisions":       provisions,
        "roa":              round(roa, 2),
        "roe":              round(roe, 2),
        "nim":              nim,
        "npl":              npl,
        "n1":               n1,
        "n2":               n2,
        "n3":               n3,
        "n4":               n4,
        "interest_income":  int_income,
        "interest_expense": int_expense,
        "cash":             cash,
        "nostro":           nostro,
        "liquid_assets":    liquid_assets,
        "securities":       securities,
        "mbk_given":        mbk_given,
        "fixed_assets":     fixed_assets,
    }


def _is_bank_active_at(bank: dict, ref_date: date) -> bool:
    """Проверяет, работал ли банк на указанную дату.

    Поля `active_from` / `active_to` — строки ISO 'YYYY-MM-DD'.
    active_from: банк начал работать с этой даты (включительно).
    active_to:   банк прекратил работу до этой даты (исключительно),
                 т.е. на дату active_to банк уже не отображается.
    Если поле отсутствует — ограничение не применяется.
    """
    raw_from = bank.get("active_from")
    raw_to   = bank.get("active_to")
    if raw_from and ref_date < date.fromisoformat(raw_from):
        return False
    if raw_to and ref_date >= date.fromisoformat(raw_to):
        return False
    return True


def _get_all_metrics_for_date(ref_date: date) -> list[dict]:
    """Генерирует метрики всех банков для заданной даты."""
    result = []
    for bank in ALL_BANKS:
        # Пропускаем банки, которые не работали на указанную дату
        if not _is_bank_active_at(bank, ref_date):
            continue
        rank = bank["rank_approx"]
        base_assets = _bank_base_assets(rank, bank["reg_num"])
        metrics = _generate_metrics(bank["reg_num"], ref_date, base_assets)
        result.append({
            "reg_num": bank["reg_num"],
            "name":    bank["name"],
            "short":   bank["short"],
            "region":  bank["region"],
            **metrics,
        })
    # Сортируем по активам (убывание) и выставляем реальный ранг
    result.sort(key=lambda b: b["assets"], reverse=True)
    for i, b in enumerate(result):
        b["rank"] = i + 1
    return result


# ---------------------------------------------------------------------------
# CBRParser — основной класс
# ---------------------------------------------------------------------------
class CBRParser:
    PRIMSOCBANK_REG_NUM = "2733"
    CBR_SOAP_URL = "https://www.cbr.ru/CreditInfoWebServ/CreditOrgInfo.asmx"
    CBR_OPENDATA_BANKS = (
        "https://www.cbr.ru/vfs/opendata/7706397098-bankovoie_soglasovanie/"
        "data-20240101-structure-20170101.csv"
    )
    CBR_BASE = "https://www.cbr.ru"

    def __init__(self):
        self._client = httpx.Client(timeout=15, follow_redirects=True)
        self._demo_mode = False  # True если ЦБ недоступен
        self._f101_code_cache = {}  # Кэш в памяти: "reg_num:YYYY-MM" -> dict

    def reconfigure_client(self, settings: dict) -> None:
        """Пересоздаёт HTTP-клиент с новыми настройками прокси/таймаута."""
        try:
            self._client.close()
        except Exception:
            pass
        self._client = _build_http_client(settings)

    # ------------------------------------------------------------------
    # Публичный API
    # ------------------------------------------------------------------

    def get_bank_list(self) -> list[dict]:
        """Получить список всех банков."""
        cache_path = CACHE_DIR / "bank_list.json"
        if cache_path.exists():
            try:
                with open(cache_path, encoding="utf-8") as f:
                    cached = json.load(f)
                # Используем кэш только если в нём достаточно реальных банков (>500)
                if cached and len(cached) >= 250:
                    return cached
                logger.info("[CBR] Кэш банков устарел (%d записей), обновляем...", len(cached))
            except Exception:
                pass

        # Пробуем реальный источник
        try:
            data = self._fetch_bank_list_cbr()
            if data:
                with open(cache_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                return data
        except Exception as e:
            logger.warning(f"[CBR] Не удалось загрузить список банков: {e}")

        # Fallback — встроенный список (только актуальные банки)
        self._demo_mode = True
        today = date.today()
        result = [
            {
                "reg_num": b["reg_num"],
                "name":    b["name"],
                "short":   b["short"],
                "region":  b["region"],
            }
            for b in ALL_BANKS
            if _is_bank_active_at(b, today)
        ]
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        return result

    def get_all_banks_list(self) -> list[dict]:
        """Список ВСЕХ банков включая неактивные — для поиска по историческим данным.
        Возвращает поля: reg_num, name, short, region, active_to, status, status_label (если есть).
        """
        result = []
        for b in KNOWN_BANKS:
            entry = {
                "reg_num": b["reg_num"],
                "name":    b["name"],
                "short":   b["short"],
                "region":  b["region"],
            }
            if "active_to" in b:
                entry["active_to"] = b["active_to"]
            if "active_from" in b:
                entry["active_from"] = b["active_from"]
            if "status" in b:
                entry["status"] = b["status"]
            if "status_label" in b:
                entry["status_label"] = b["status_label"]
            result.append(entry)
        return result

    def get_metrics_for_date(self, ref_date: Optional[str] = None,
                             include_closed: bool = False) -> list[dict]:
        """Возвращает метрики всех банков на заданную дату.

        include_closed=True — дополнительно включает закрытые банки,
        которые имели лицензию на указанную дату (данные из SOAP ЦБ).
        """
        d = self._parse_date(ref_date)
        # Ограничиваем дату: не допускаем текущий и будущие месяцы
        # (ЦБ публикует данные Ф101 с задержкой ~1-1.5 мес)
        _today = date.today()
        _max_allowed = self._parse_date(None)  # последний допустимый месяц
        if d > _max_allowed:
            logger.info("[CBR] Дата %s > максимально доступной %s, ограничиваем", d, _max_allowed)
            d = _max_allowed
        suffix    = "_closed" if include_closed else ""
        cache_key = f"metrics_v2_{d.year}_{d.month:02d}{suffix}.json"
        cache_path = METRICS_DIR / cache_key

        if cache_path.exists():
            try:
                with open(cache_path, encoding="utf-8") as f:
                    data = json.load(f)
                if data and isinstance(data, list) and len(data) > 0:
                    return data
            except Exception:
                pass

        extra: Optional[list] = None
        if include_closed:
            try:
                extra = self.get_closed_banks_list()
            except Exception as e:
                logger.warning("[CBR] Закрытые банки недоступны: %s", e)

        # Текущий и будущий месяцы — не сохраняем кэш на диск
        # (ЦБ публикует данные с задержкой ~1 месяц, поэтому данные текущего месяца неполные)
        from datetime import date as _date_cls
        _today = _date_cls.today()
        _is_current_or_future = (d.year > _today.year) or (d.year == _today.year and d.month >= _today.month)

        try:
            data = self._fetch_form101_cbr(d, extra_banks=extra)
            if data:
                self._demo_mode = False
                if not _is_current_or_future:
                    with open(cache_path, "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                return data
        except Exception as e:
            logger.warning("[CBR] Не удалось загрузить форму 101: %s", e)

        # Fallback — демо-данные (только для прошлых периодов кэшируем на диск)
        self._demo_mode = True
        data = _get_all_metrics_for_date(d)
        if not _is_current_or_future:
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        return data

    def get_bank_metrics(self, reg_num: str, ref_date: Optional[str] = None,
                         include_closed: bool = False) -> Optional[dict]:
        """Метрики одного банка.

        Если банк найден в bulk-кэше, но нормативы (n1..n4) равны None
        (пропущены при bulk-загрузке для экономии времени) — подгружаем их
        отдельным SOAP-запросом.
        """
        all_metrics = self.get_metrics_for_date(ref_date, include_closed=include_closed)
        result = None
        for b in all_metrics:
            if b["reg_num"] == reg_num:
                result = b
                break

        if result is None:
            # Банк не найден в кэше — прямой SOAP-запрос
            d      = self._parse_date(ref_date)
            dt_str = d.strftime("%Y-%m-%dT00:00:00")
            try:
                metrics = self._fetch_bank_form101(reg_num, dt_str)
                if metrics:
                    bank_info = next((b for b in self.get_bank_list() if b["reg_num"] == reg_num), {})
                    if not bank_info and include_closed:
                        closed = self.get_closed_banks_list()
                        bank_info = next((b for b in closed if b["reg_num"] == reg_num), {})
                    result = {"reg_num": reg_num, **bank_info, **metrics}
            except Exception:
                pass

        if result is None:
            return None

        # Если нормативы не были загружены в bulk-режиме — подгрузить сейчас
        if result.get("n1") is None and not result.get("demo"):
            d      = self._parse_date(ref_date)
            dt_str = d.strftime("%Y-%m-%dT00:00:00")
            try:
                norms = self._fetch_normatives(reg_num, dt_str)
                if norms:
                    result = dict(result)   # копия чтобы не мутировать кэш
                    result.update({
                        "n1": norms.get("Н1.0"),
                        "n2": norms.get("Н2"),
                        "n3": norms.get("Н3"),
                        "n4": norms.get("Н4"),
                    })
            except Exception:
                pass

        return result

    def get_top_banks_by_assets(self, n: int, ref_date: Optional[str] = None) -> list[dict]:
        """ТОП-N банков по активам."""
        all_metrics = self.get_metrics_for_date(ref_date)
        sorted_banks = sorted(all_metrics, key=lambda b: b.get("assets", 0), reverse=True)
        return sorted_banks[:n]

    def get_sector_aggregates(self, ref_date: Optional[str] = None) -> dict:
        """Агрегаты по всему сектору."""
        from data_processor import DataProcessor
        dp = DataProcessor()
        all_metrics = self.get_metrics_for_date(ref_date)
        metrics_of_interest = [
            "assets", "capital", "loans", "deposits", "profit",
            "roa", "roe", "nim", "npl", "n1",
        ]
        return {
            m: dp.get_group_stats(all_metrics, m)
            for m in metrics_of_interest
        }

    def _get_bank_metrics_fast(self, reg_num: str, d: date) -> Optional[dict]:
        """Быстрый путь: проверяем ТОЛЬКО кэш, иначе — прямой SOAP (не запускаем полный bulk-fetch).
        Используется в get_time_series чтобы не ждать загрузки 305 банков за каждый месяц.
        """
        cache_key  = f"metrics_v2_{d.year}_{d.month:02d}.json"
        cache_path = METRICS_DIR / cache_key
        if cache_path.exists():
            try:
                with open(cache_path, encoding="utf-8") as f:
                    data = json.load(f)
                for b in data:
                    if b.get("reg_num") == reg_num:
                        return b
            except Exception:
                pass

        # Кэша нет или банк не найден — прямой SOAP
        dt_str = d.strftime("%Y-%m-%dT00:00:00")
        try:
            metrics = self._fetch_bank_form101(reg_num, dt_str)
            if metrics:
                bank_info = next((b for b in self.get_bank_list() if b["reg_num"] == reg_num), {})
                return {"reg_num": reg_num, **bank_info, **metrics}
        except Exception:
            pass
        return None

    def get_time_series(
        self,
        reg_num: str,
        metric: str,
        date_from: str,
        date_to: str,
        period: str = "month",
    ) -> list[dict]:
        """Временной ряд показателя за период (параллельные SOAP-запросы для ускорения)."""
        d_from = self._parse_date(date_from)
        d_to   = self._parse_date(date_to)

        dates = self._generate_date_range(d_from, d_to, period)

        def fetch_point(d: date):
            bank_data = self._get_bank_metrics_fast(reg_num, d)
            if bank_data and bank_data.get(metric) is not None:
                return d, {"date": d.strftime("%Y-%m"), "value": bank_data.get(metric)}
            return d, None

        # Параллельно для ускорения (прямые SOAP-запросы без bulk-fetch)
        results_map = {}
        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = {executor.submit(fetch_point, d): d for d in dates}
            for fut in as_completed(futures):
                d, pt = fut.result()
                if pt:
                    results_map[d] = pt

        return [results_map[d] for d in dates if d in results_map]

    def get_group_timeseries(
        self,
        metric: str,
        date_from: str,
        date_to: str,
        top_ns: list[int],
        period: str = "month",
        agg_mode: str = "sum",
    ) -> dict:
        """
        Временные ряды средних значений для нескольких групп топ-N.
        Возвращает {top10: [...], top50: [...], all: [...]}.
        """
        from data_processor import DataProcessor
        dp = DataProcessor()
        d_from = self._parse_date(date_from)
        d_to   = self._parse_date(date_to)
        dates = self._generate_date_range(d_from, d_to, period)

        result = {f"top{n}": [] for n in top_ns}
        result["all"] = []

        for d in dates:
            all_metrics = self.get_metrics_for_date(d.strftime("%Y-%m-%d"))
            sorted_by_assets = sorted(all_metrics, key=lambda b: b.get("assets", 0), reverse=True)

            date_str = d.strftime("%Y-%m")
            agg_all = dp.aggregate_group(all_metrics, metric)
            result["all"].append({"date": date_str, "value": agg_all.get(agg_mode) if agg_all else None})

            for n in top_ns:
                agg = dp.aggregate_group(sorted_by_assets[:n], metric)
                result[f"top{n}"].append({"date": date_str, "value": agg.get(agg_mode) if agg else None})

        return result

    def get_district_timeseries(
        self,
        districts: list,
        metric: str,
        date_from: str,
        date_to: str,
        period: str = "month",
        agg_mode: str = "sum",
    ) -> dict:
        """Временные ряды средних значений по банкам указанных федеральных округов."""
        from data_processor import DataProcessor
        dp = DataProcessor()
        d_from = self._parse_date(date_from)
        d_to   = self._parse_date(date_to)
        dates  = self._generate_date_range(d_from, d_to, period)

        result = {dist: [] for dist in districts}

        for dt in dates:
            date_str = dt.strftime("%Y-%m")
            all_metrics = self.get_metrics_for_date(dt.strftime("%Y-%m-%d"))
            for dist in districts:
                dist_banks = [b for b in all_metrics if b.get("district") == dist]
                if dist_banks:
                    agg = dp.aggregate_group(dist_banks, metric)
                    result[dist].append({"date": date_str, "value": agg.get(agg_mode) if agg else None})
                else:
                    result[dist].append({"date": date_str, "value": None})

        return result

    def get_rank_for_bank(self, reg_num: str, metric: str, ref_date: Optional[str] = None) -> dict:
        """Место банка в рейтинге по показателю."""
        all_metrics = self.get_metrics_for_date(ref_date)
        bank = next((b for b in all_metrics if b["reg_num"] == reg_num), None)
        if not bank or bank.get(metric) is None:
            return {"rank": None, "total": len(all_metrics), "percentile": None}

        ascending = metric in ("npl",)  # меньше = лучше
        sorted_banks = sorted(
            all_metrics,
            key=lambda b: b.get(metric, float("-inf") if not ascending else float("inf")),
            reverse=not ascending,
        )
        rank = next(
            (i + 1 for i, b in enumerate(sorted_banks) if b["reg_num"] == reg_num),
            None,
        )
        total = len(sorted_banks)
        percentile = round((total - rank) / total * 100, 1) if rank else None
        return {"rank": rank, "total": total, "percentile": percentile}

    @property
    def is_demo_mode(self) -> bool:
        return self._demo_mode

    # ------------------------------------------------------------------
    # Внутренние методы
    # ------------------------------------------------------------------

    def _fetch_bank_list_cbr(self) -> list[dict]:
        """Загружает актуальный список КО со страницы ЦБ РФ.

        Пробует несколько источников:
        1. HTML-таблица https://www.cbr.ru/banking_sector/credit/FullCoList/
        2. Старый OpenData CSV (запасной вариант).
        """
        try:
            return self._fetch_bank_list_cbr_html()
        except Exception as e:
            logger.warning("[CBR] HTML bank list failed: %s — пробуем CSV", e)

        # Запасной: старый CSV
        resp = self._client.get(self.CBR_OPENDATA_BANKS, timeout=10)
        if resp.status_code != 200:
            raise ConnectionError(f"HTTP {resp.status_code}")
        import io, csv
        reader = csv.DictReader(io.StringIO(resp.text))
        banks = []
        for row in reader:
            reg  = (row.get("REG_NUM") or row.get("reg_num") or "").strip()
            name = (row.get("NAME_BANK") or row.get("name") or "").strip()
            if reg and name:
                banks.append({"reg_num": reg, "name": name, "short": name[:30], "region": ""})
        return banks if len(banks) > 10 else []

    def _fetch_bank_list_cbr_html(self) -> list[dict]:
        """Парсит HTML-страницу ЦБ РФ «Действующие кредитные организации».

        Структура таблицы (9 колонок):
          0: №  1: Вид  2: Рег.номер  3: ОГРН  4: Наименование
          5: ОПФ  6: Дата  7: Статус  8: Местонахождение
        Таблица содержит головные офисы и филиалы (дублируются рег. номера).
        Оставляем только первое вхождение каждого рег. номера (головной офис).
        """
        from bs4 import BeautifulSoup

        url  = f"{self.CBR_BASE}/banking_sector/credit/FullCoList/"
        resp = self._client.get(url, timeout=30,
                                headers={"Accept-Language": "ru-RU,ru;q=0.9"})
        if resp.status_code != 200:
            raise ConnectionError(f"HTTP {resp.status_code}")

        soup = BeautifulSoup(resp.text, "lxml")
        table = soup.find("table")
        if not table:
            raise ValueError("Таблица банков не найдена на странице ЦБ РФ")

        banks      = []
        seen_regs: set = set()

        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 8:
                continue
            vid    = tds[1].get_text(strip=True)   # '' = банк; 'Расчетная НКО' / 'НДКО' etc.
            reg    = tds[2].get_text(strip=True)
            name   = tds[4].get_text(strip=True).replace("\xa0", " ")
            status = tds[7].get_text(strip=True)   # 'Действующая' / 'Ликвидация' / 'Отозванная'
            addr   = tds[8].get_text(strip=True).replace("\xa0", " ") if len(tds) > 8 else ""

            # Только действующие банки (у банков vid пустой; у НКО — 'Расчетная НКО', 'НДКО' и т.д.)
            if status != "Действующая":
                continue
            if vid:  # любая непустая строка в «Вид» означает НКО, не банк
                continue
            if not reg.isdigit() or not name:
                continue
            if reg in seen_regs:
                continue  # дубликат (филиал)

            seen_regs.add(reg)

            # Короткое название: убираем правовые формы и кавычки
            short = (
                name.replace("Публичное акционерное общество", "")
                    .replace("Акционерное общество", "")
                    .replace("Общество с ограниченной ответственностью", "")
                    .replace("«", "").replace("»", "")
                    .replace('"', "").strip()[:45]
            )

            # Извлекаем название города и федеральный округ из полного адреса
            city, district = _extract_city_from_address(addr)

            banks.append({
                "reg_num":  reg,
                "name":     name,
                "short":    short or name[:45],
                "region":   city,
                "district": district,
            })

        if len(banks) < 250:
            raise ValueError(f"Слишком мало банков в HTML: {len(banks)}")
        logger.info("[CBR] Загружено %d действующих банков с сайта ЦБ РФ", len(banks))
        return banks

    def _soap_call(self, method: str, body: str, timeout: float = 20) -> str:
        """SOAP запрос к ЦБ РФ. Возвращает тело XML-ответа.

        Использует self._client (пул соединений + keep-alive) для повторного
        использования TCP-соединений, что ускоряет массовую загрузку в 2–3 раза.
        """
        envelope = (
            '<?xml version="1.0" encoding="utf-8"?>'
            '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">'
            f'<soap:Body>{body}</soap:Body>'
            '</soap:Envelope>'
        )
        resp = self._client.post(
            self.CBR_SOAP_URL,
            content=envelope.encode("utf-8"),
            headers={
                "Content-Type": "text/xml; charset=utf-8",
                "SOAPAction": f"http://web.cbr.ru/{method}",
            },
            timeout=timeout,
        )
        if resp.status_code != 200:
            raise ConnectionError(f"SOAP {method} -> HTTP {resp.status_code}")
        return resp.content.decode("utf-8", errors="replace")

    def _parse_f101(self, xml_str: str) -> dict:
        """Парсит ответ Data101FNew.

        SOAP возвращает два поля остатков:
          vitg  — входящий остаток (начало периода, т.е. предыдущая отчётная дата)
          iitg  — исходящий остаток (конец периода, т.е. запрашиваемая дата)

        Для расчёта метрик используем iitg — это актуальные данные на запрошенную дату.
        Обороты за период (vitg_thr) в SOAP-ответе отсутствуют — вычисляем как
        разницу iitg − vitg (изменение за период).

        Возвращает словарь с двумя видами ключей (только pln='А'):
          (numsc, ap)       -> исходящий остаток (iitg), тыс. руб.
          (numsc, ap, "t")  -> изменение за период (iitg − vitg), тыс. руб.
        """
        result: dict = {}
        try:
            root = ET.fromstring(xml_str)
            for elem in root.iter():
                if "}" in elem.tag:
                    elem.tag = elem.tag.split("}", 1)[1]
            for row in root.iter("F101"):
                numsc    = (row.findtext("numsc")    or "").strip()
                pln      = (row.findtext("pln")      or "").strip()
                ap       = (row.findtext("ap")       or "").strip()
                vitg     =  row.findtext("vitg")     or "0"
                iitg     =  row.findtext("iitg")     or "0"
                if pln == "А" and numsc and ap:
                    try:
                        val_iitg = float(iitg)
                        val_vitg = float(vitg)
                        # SOAP может возвращать несколько блоков (основной банк +
                        # дочерние организации). Используем только ПЕРВОЕ
                        # вхождение каждого счёта — данные основного банка.
                        if (numsc, ap) not in result:
                            result[(numsc, ap)]        = val_iitg
                            result[(numsc, ap, "t")]    = val_iitg - val_vitg
                    except ValueError:
                        pass
        except Exception as e:
            logger.warning(f"[CBR] Ошибка парсинга Form 101: {e}")
        return result

    def _compute_metrics_from_f101(self, f101: dict) -> Optional[dict]:
        """Вычисляет финансовые метрики из словаря Form 101.

        Входные значения в тыс. руб.
        Выходные значения в млрд. руб. (кроме %, n1-n4 заполняются из Form 135).
        """
        def g1(code): return f101.get((code, "1"), 0.0)    # дебет  (остаток)
        def g2(code): return f101.get((code, "2"), 0.0)    # кредит (остаток)
        def t1(code): return f101.get((code, "1", "t"), 0.0)  # дебет-оборот
        def t2(code): return f101.get((code, "2", "t"), 0.0)  # кредит-оборот
        def bln(v):   return round(v / 1_000_000, 3)       # тыс. руб. -> млрд. руб.
        def net(code): return max(g1(code) - g2(code), 0.0) # нетирование (banki.ru)

        itgap_d = g1("ITGAP")
        if itgap_d <= 0:
            return None

        # ══════════════════════════════════════════════════════════════════
        # МЕТОДОЛОГИЯ BANKI.RU (banki_ratings_methodology_new_202401.pdf)
        # Все формулы используют max(0, дебет-кредит) для нетирования РВПС
        # ══════════════════════════════════════════════════════════════════

        # ── Компонент 1: Высоколиквидные активы ──
        c1_liquid = g1("20.0") + g1("301") + g1("302")

        # ── Компонент 2: Выданные МБК ──
        #   324, 325 — banki.ru использует g1() (валовый дебет)
        c2_mbk = (g1("319") + g1("32.1") + g1("32.2")
                  + g1("324") + g1("329"))

        # ── Компонент 3: Ценные бумаги ──
        #   Облигации: 501, 502, 504, 505
        #   Акции: 506, 507
        #   Векселя: 512, 513, 515
        c3_bonds  = net("501") + net("502") + net("504") + net("505")
        c3_stocks = net("506") + net("507")
        c3_bills  = net("512") + net("513") + net("515")
        c3_securities = c3_bonds + c3_stocks + c3_bills

        # ── Компонент 4: Кредиты ФЛ ──
        #   45.2 = агрегат 455+457; нетируем: 45.2(дебет) − 45.2(кредит=РВПС)
        c4_loans_fl = net("45.2")

        # ── Компонент 5: Кредиты ЮЛ ──
        #   Две формулы: ВАЛОВАЯ (g1, для активов) и НЕТТО (net, для кредитного портфеля).
        #   Проверено сверкой с banki.ru ТОП-10:
        #   - В АКТИВАХ банки.ру использует g1() (дебет = брутто) без вычета РВПС
        #   - В КРЕДИТНОМ ПОРТФЕЛЕ банки.ру использует net() (дебет − кредит/РВПС)
        #   45.0 = агрегат 452+456 (кредиты ЮЛ-резиденты + ЮЛ-нерезиденты)

        # -- Валовый (g1) — для расчёта активов --
        c5_base_gross = g1("45.0")
        c5_451_gross  = g1("451")
        c5_453_gross  = g1("453")
        c5_454_gross  = g1("454")
        c5_44x_gross  = (g1("441") + g1("442") + g1("443") + g1("444")
                         + g1("445") + g1("446") + g1("447") + g1("448")
                         + g1("450")
                         + g1("460") + g1("461") + g1("462") + g1("463")
                         + g1("464") + g1("465") + g1("466") + g1("467")
                         + g1("468") + g1("469") + g1("470"))
        c5_loans_yl_gross = c5_base_gross + c5_451_gross + c5_453_gross + c5_454_gross + c5_44x_gross
        c5_478_gross = g1("478")

        # -- Нетто — для расчёта кредитного портфеля --
        #   c5_base: g1(45.0) минус g2(45.1) — РВПС-агрегат для глав 452+456
        #   НЕ используем net(45.0) потому что g2(45.0) включает 408 кредит (не РВПС!)
        #   Не включаем: 449 (РВПС гл.44), 459 (РВПС гл.45), 479 (РВПС гл.47)
        c5_base = max(g1("45.0") - g2("45.1"), 0.0)
        c5_451  = net("451")   # кредиты кред. организациям
        c5_453  = net("453")   # кредиты финансовым ЮЛ
        c5_454  = net("454")   # кредиты ИП
        c5_44x = (net("441") + net("442") + net("443") + net("444")
                  + net("445") + net("446") + net("447") + net("448")
                  + net("450")
                  + net("460") + net("461") + net("462") + net("463")
                  + net("464") + net("465") + net("466") + net("467")
                  + net("468") + net("469") + net("470"))
        c5_loans_yl = c5_base + c5_451 + c5_453 + c5_454 + c5_44x

        # ── Приобретённые права требования (banki.ru включает в кредитный портфель) ──
        c5_478 = net("478")

        # ── Компонент 6: Основные средства и НМА ──
        c6_fixed = (net("604") + net("608") + net("609")
                    + net("610") + net("619") + net("621"))

        # ── Компонент 7: Прочие активы ──
        #   Проверено сверкой с banki.ru (ТОП-10 банков):
        #   474 — активно-пассивный: banki.ru берёт net(474) = дебет − кредит (разница = актив)
        #   60.0 — активно-пассивный: banki.ru берёт g1(60.0) = полный дебет (дебиторка = актив)
        #   459 — РВПС (контр-актив): banki.ru НЕ включает (вычитается из кредитов отдельно)
        #   325 — banki.ru использует g1() (валовый дебет)
        c7_other = (g1("305") + g1("325") + g1("401") + g1("403") + g1("409")
                    + net("474") + net("477")
                    + g1("526") + g1("60.0") + g1("617") + net("620"))

        # ── Активы нетто = сумма 7 компонентов (с ВАЛОВЫМИ кредитами + 478) ──
        #   Для активов используем _gross (g1) компоненты кредитов — без вычета РВПС
        #   ПРИМЕЧАНИЕ: счёт 458 (просрочка) НЕ добавляется — для большинства банков
        #   (Сбер, ПСБ, ВТБ) формула без 458 даёт точное совпадение с banki.ru (0.00%).
        #   Для банков с аномально крупным 474 (ГПБ, МКБ) остаётся расхождение 1-5%,
        #   связанное с закрытой методологией banki.ru по активно-пассивным счетам.
        assets_raw = c1_liquid + c2_mbk + c3_securities + c4_loans_fl + c5_loans_yl_gross + c5_478_gross + c6_fixed + c7_other
        if assets_raw <= 0:
            return None

        # Капитал (по форме 123 — здесь приближение из ф.101)
        capital = g2("102") + g2("106") + g2("107") + g2("108")

        # ── Кредитный портфель (banki.ru) ──
        loans_fl = c4_loans_fl if c4_loans_fl > 0 else None
        loans_yl = c5_loans_yl if c5_loans_yl > 0 else None
        loans_gross = (c4_loans_fl + c5_loans_yl + c5_478)

        # Просрочка: max(0, 458(деб) − 458(кред))
        npl_abs = max(g1("458") - g2("458"), 0.0)
        npl_pct = round(npl_abs / loans_gross * 100, 2) if loans_gross > 0 else 0.0

        # Приобретённые права требования (banki.ru включает в кредитный портфель)
        loans_478 = net("478")

        # Резервы (информационный показатель, не вычитаются из loans)
        provisions = max(g1("459"), g2("459"))

        # ──────────────────────────────────────────────────────────────────────
        # Привлечённые средства — план счетов 809-П (с 2022 г.)
        # Источник данных: SOAP Data101FNew (агрегированные коды, не лицевые счета)
        #
        # Физические лица (методология banki.ru):
        #   42.2 = гл.423 + гл.426 — срочные вклады ФЛ резидентов + нерезидентов
        #          (SOAP возвращает 423 и 426 только суммарно, разделить нельзя)
        #   522  = гл.522 — сберегательные сертификаты / облигации для физлиц
        #          (banki.ru включает 522 в «Вклады ФЛ»; у большинства малых банков = 0)
        #
        # Текущие счета ФЛ — ОТДЕЛЬНЫЙ блок, НЕ входит в deposits_fl:
        #   408  = 40817 + 40820 — текущие счета ФЛ (кредитовый остаток, пассив).
        #          Примечание: SOAP возвращает только кредитовое сальдо гл. 408.
        #
        # Юридические лица (срочные депозиты):
        #   42.1 = гл.421 + гл.425 — ком. орг.-резиденты + ЮЛ-нерезиденты
        #   420  = гл.420 — государственные / бюджетные депозиты
        #   422  = гл.422 — НКО / госпредприятия
        #   43.1 = гл.438 + гл.440 — привлечённые от небанковских финансовых
        #          организаций (брокеры, страховые, НПФ, МФО) и ЮЛ-нерезидентов
        #          финансового сектора; НЕ вклады ФЛ
        #
        # Юридические лица (текущие/расчётные счета):
        #   405-407, 409 — расчётные счета ЮЛ (408 кредит — ФЛ, не ЮЛ)
        # ──────────────────────────────────────────────────────────────────────

        # ── Вклады физических лиц (banki.ru) ──
        #   42.2(кредит) + 522(кредит) — методология PDF: «42.2 + 522»
        dep_fl_term = g2("42.2")                            # срочные вклады ФЛ (423+426)
        dep_522     = g2("522")                             # сберсертификаты / облигации ФЛ
        deposits_fl = dep_fl_term + dep_522                 # итого ФЛ (banki.ru: 42.2 + 522)

        # Текущие счета ФЛ — отдельный блок (не входит в deposits_fl)
        dep_408     = g2("408")                             # текущие счета ФЛ (40817+40820)

        # ── Средства предприятий и организаций (banki.ru) ──
        #   Счета: 401-407 (кредит)
        #   Срочные: 410-420, 422, 42.1, 427-437, 43.1, 439, 521 (кредит)
        dep_yl_accounts = (g2("401") + g2("402") + g2("403") + g2("404")
                           + g2("405") + g2("406") + g2("407"))
        dep_yl_term = (g2("410") + g2("411") + g2("412") + g2("413")
                       + g2("414") + g2("415") + g2("416") + g2("417")
                       + g2("418") + g2("419") + g2("420") + g2("422")
                       + g2("42.1") + g2("427") + g2("428") + g2("429")
                       + g2("430") + g2("431") + g2("432") + g2("433")
                       + g2("434") + g2("435") + g2("436") + g2("437")
                       + g2("43.1") + g2("439") + g2("521"))
        deposits_yl = dep_yl_accounts + dep_yl_term         # итого ЮЛ (banki.ru)

        # Итого привлечённые
        deposits = deposits_fl + dep_408 + deposits_yl

        # Обязательства = Активы − Капитал
        liabilities = max(assets_raw - capital, 0.0)

        # Прибыль = кумулятивная чистая прибыль (с начала года до отчётной даты).
        # Счёт 706: кредит = доходы, дебет = расходы.
        # g2("706") − g1("706") = накопленная чистая прибыль — совпадает с banki.ru.
        # SOAP не возвращает поле vitg_thr (обороты); «turnovers» = iitg − vitg
        # дают лишь МЕСЯЧНОЕ изменение, а не кумулятивный итог, поэтому не используем.
        profit = g2("706") - g1("706")

        roa = round(profit / assets_raw * 100, 2) if assets_raw > 0 else 0.0
        roe = round(profit / capital  * 100, 2) if capital  > 0 else 0.0

        # NIM: процентные доходы/расходы из субсчетов 706 (если представлены)
        int_income  = g2("70601") + g2("70602") + g2("70603") + g2("70604")
        int_expense = g1("70606") + g1("70607")
        earn_a      = assets_raw * 0.85
        nim = (
            round((int_income - int_expense) / earn_a * 100, 2)
            if int_income > 0 and earn_a > 0
            else None
        )

        # ──────────────────────────────────────────────────────────────────────
        # Дополнительные показатели структуры баланса (план счетов 809-П)
        # ──────────────────────────────────────────────────────────────────────

        # ── Компоненты баланса (детализация из расчёта assets_raw) ──
        cash_raw    = g1("20.0") or g1("202")
        nostro_raw  = g1("301")
        reserves_raw = g1("302")
        liquid_raw  = c1_liquid   # = cash + nostro + reserves

        # Ценные бумаги (banki.ru: нетированные по каждому счёту)
        securities_raw = c3_securities

        # МБК выданные (banki.ru: 319 + нетированные 32.1, 32.2, 324 + 329)
        mbk_given_raw = c2_mbk

        # Основные средства (banki.ru: нетированные 604+608+609+610+619+621)
        fixed_assets_raw = net("604")

        # Нематериальные активы (нетированные)
        intangibles_raw = net("609")

        # ОС + НМА (нетированные, из c6)
        fixed_assets_full_raw = c6_fixed

        # Ценные бумаги — детальная разбивка (banki.ru: нетированные)
        securities_bonds_raw  = c3_bonds    # net(501)+net(502)+net(504)+net(505)
        securities_stocks_raw = c3_stocks   # net(506)+net(507)
        securities_repo_raw   = 0.0         # РЕПО включены в bonds (502/504 нетированы)
        securities_bills_raw  = c3_bills    # net(512)+net(513)+net(515)

        # Вложения в капиталы (601)
        investments_raw = net("601") if g1("601") > 0 else 0.0

        # Прочие активы = component 7
        other_assets_raw = c7_other

        # МБК размещённые в ЦБ РФ
        mbk_placed_cbr_raw = g1("319") + g1("329")

        # Привлечённые МБК (312 — от банков-резидентов; 313 — от нерезидентов)
        mbk_received_raw     = g2("312") + g2("313")
        # Кредиты от Банка России (310 — рефинансирование + прочие кредиты ЦБ)
        mbk_received_cbr_raw = g2("310")

        # ЛОРО-счета (средства банков-корреспондентов у нас)
        loro_raw = g2("30109") + g2("30111")

        # Выпущенные долговые ценные бумаги
        issued_bonds_raw = g2("520") + g2("522")            # облигации + сберсертификаты/ФЛ-бонды
        issued_bills_raw = g2("523")                        # векселя собственные
        issued_sec_raw   = issued_bonds_raw + issued_bills_raw

        # Уровень резервирования = резервы / кредиты брутто × 100
        provisions_rate = (
            round(provisions / (loans_gross + npl_abs) * 100, 2)
            if (loans_gross + npl_abs) > 0 else None
        )

        # ── Обороты (vitg_thr из Формы 101) ──────────────────────────────────
        # Оборот — дебетовый (расход) или кредитовый (приход) за отчётный период
        cash_turnover         = t2("202") or t1("20.0")          # приход в кассу
        nostro_turnover       = t2("30102") + t2("30110")        # поступления на корсчета
        if nostro_turnover == 0:
            nostro_turnover   = t2("301")
        deposits_fl_turnover  = t2("42.2")                       # поступления вкладов ФЛ (423+426)
        dep_408_turnover      = t2("408")                        # поступления текущих счетов ФЛ
        deposits_yl_turnover  = (t2("42.1") + t2("420") + t2("422") + t2("43.1")
                                  + t2("405") + t2("406") + t2("407") + t2("409"))
        mbk_given_turnover    = t1("320") + t1("321")            # выдача МБК (дебет актива)
        if mbk_given_turnover == 0:
            mbk_given_turnover = t1("32.1") + t1("32.2")
        mbk_placed_cbr_turnover = t1("319")
        mbk_received_turnover  = t2("312") + t2("313")

        # Ключевые коды Ф101 для пересчёта в UI
        _KEY_CODES = [
            'ITGAP',
            '45.0', '45.1', '45.2', '45.9',
            '441', '442', '443', '444', '445', '446', '447', '448',
            '450', '460', '461', '462', '463', '464', '465', '466', '467', '468', '469', '470',
            '451', '452', '453', '454', '455', '456', '458',
            '42.1', '42.2', '43.1',
            '405', '406', '407', '408', '409', '420', '522',
            '310', '312', '313', '31.1', '31.2',
            '317', '319', '320', '321', '32.1', '32.2', '324', '329',
            '301', '302', '305', '325',
            '30102', '30109', '30110', '30111', '30202', '30204',
            '501', '502', '503', '504', '505', '506', '507',
            '512', '513', '514', '515', '520', '523',
            '102', '106', '107',
            '20.0', '60.0', '601', '604', '608', '609', '610', '619', '621',
            '706',
            '47425', '47426', '474', '477', '478', '479', '526', '617', '620',
        ]
        raw_codes = {}
        for _code in _KEY_CODES:
            for _ap in ('1', '2'):
                _val = f101.get((_code, _ap))
                if _val is not None:
                    raw_codes[f'{_code}:{_ap}'] = round(_val / 1_000_000, 6)

        # ── Альтернативные формулы (ЦБ — упрощённая методология Ф101) ──
        # ITGAP из SOAP некорректно отражает чистые активы (включает контр-счета).
        # Используем прямое суммирование основных активных счетов без нетирования РВПС.
        assets_cbr_raw = (g1("20.0") + g1("301") + g1("302")
                          + g1("319") + g1("32.1") + g1("32.2") + g1("324") + g1("329")
                          + g1("45.0") + g1("45.2") + g1("451") + g1("453") + g1("454")
                          + g1("441") + g1("442") + g1("443") + g1("444")
                          + g1("445") + g1("446") + g1("447") + g1("448")
                          + g1("450") + g1("460") + g1("461") + g1("462") + g1("463")
                          + g1("464") + g1("465") + g1("466") + g1("467")
                          + g1("468") + g1("469") + g1("470") + g1("478")
                          + g1("501") + g1("502") + g1("503") + g1("504") + g1("505")
                          + g1("506") + g1("507") + g1("512") + g1("513") + g1("514") + g1("515")
                          + g1("604") + g1("608") + g1("609") + g1("610") + g1("619") + g1("621")
                          + g1("305") + g1("325") + g1("474") + g1("477") + g1("479")
                          + g1("526") + g1("60.0") + g1("617") + g1("620"))
        loans_cbr_gross = (g1("45.0") + g1("45.2") + g1("451") + g1("453") + g1("454")
                           + g1("441") + g1("442") + g1("443") + g1("444")
                           + g1("445") + g1("446") + g1("447") + g1("448")
                           + g1("450") + g1("460") + g1("461") + g1("462") + g1("463")
                           + g1("464") + g1("465") + g1("466") + g1("467")
                           + g1("468") + g1("469") + g1("470") + g1("478"))
        loans_cbr_raw   = max(loans_cbr_gross - provisions, 0.0)
        loans_fl_cbr    = g1("45.2")
        loans_yl_cbr    = (g1("45.0") + g1("441") + g1("442") + g1("443") + g1("444")
                           + g1("445") + g1("446") + g1("447") + g1("448")
                           + g1("450") + g1("460") + g1("461") + g1("462") + g1("463")
                           + g1("464") + g1("465") + g1("466") + g1("467")
                           + g1("468") + g1("469") + g1("470"))
        securities_cbr  = (g1("501") + g1("502") + g1("503") + g1("504")
                           + g1("505") + g1("506") + g1("507")
                           + g1("512") + g1("513") + g1("514") + g1("515"))
        dep_fl_cbr      = g2("42.2")
        dep_yl_cbr_term = g2("42.1") + g2("420") + g2("422") + g2("43.1")
        dep_yl_cbr_curr = g2("405") + g2("406") + g2("407") + g2("409")
        dep_yl_cbr      = dep_yl_cbr_term + dep_yl_cbr_curr

        return {
            # ── Основные показатели (banki.ru) ──
            "assets":           bln(assets_raw),
            "capital":          bln(capital),
            "loans":            bln(loans_gross),
            "loans_fl":         bln(loans_fl)    if loans_fl    is not None else None,
            "loans_yl":         bln(loans_yl)    if loans_yl    is not None else None,
            "npl_abs":          bln(npl_abs),
            "deposits":         bln(deposits),
            "deposits_fl":      bln(deposits_fl) if deposits_fl > 0 else None,
            "deposits_yl":      bln(deposits_yl) if deposits_yl > 0 else None,
            "deposits_fl_term": bln(dep_fl_term) if dep_fl_term > 0 else None,
            "dep_522":          bln(dep_522)     if dep_522   > 0 else None,
            "dep_408":          bln(dep_408)     if dep_408   > 0 else None,
            "liabilities":      bln(liabilities),
            "profit":           bln(profit),
            "provisions":       bln(provisions),
            "roa":              roa,
            "roe":              roe,
            "nim":              nim,
            "npl":              npl_pct,
            "n1":               None,
            "n2":               None,
            "n3":               None,
            "n4":               None,
            "interest_income":  bln(int_income)       if int_income       > 0 else None,
            "interest_expense": bln(int_expense)      if int_expense      > 0 else None,
            # Структура баланса
            "cash":             bln(cash_raw)          if cash_raw          > 0 else None,
            "nostro":           bln(nostro_raw)        if nostro_raw        > 0 else None,
            "reserves":         bln(reserves_raw)      if reserves_raw      > 0 else None,
            "liquid_assets":    bln(liquid_raw)        if liquid_raw        > 0 else None,
            "securities":       bln(securities_raw)    if securities_raw    > 0 else None,
            "securities_bonds": bln(securities_bonds_raw) if securities_bonds_raw > 0 else None,
            "securities_stocks":bln(securities_stocks_raw)if securities_stocks_raw> 0 else None,
            "securities_repo":  bln(securities_repo_raw)  if securities_repo_raw  > 0 else None,
            "securities_bills": bln(securities_bills_raw) if securities_bills_raw > 0 else None,
            "investments":      bln(investments_raw)   if investments_raw   > 0 else None,
            "intangibles":      bln(intangibles_raw)   if intangibles_raw   > 0 else None,
            "fixed_assets":     bln(fixed_assets_raw)  if fixed_assets_raw  > 0 else None,
            "fixed_assets_full":bln(fixed_assets_full_raw) if fixed_assets_full_raw > 0 else None,
            "other_assets":     bln(other_assets_raw)  if other_assets_raw  > 1.0 else None,
            "mbk_given":        bln(mbk_given_raw)     if mbk_given_raw     > 0 else None,
            "mbk_placed_cbr":   bln(mbk_placed_cbr_raw)if mbk_placed_cbr_raw> 0 else None,
            "mbk_received":     bln(mbk_received_raw)  if mbk_received_raw  > 0 else None,
            "mbk_received_cbr": bln(mbk_received_cbr_raw) if mbk_received_cbr_raw > 0 else None,
            "loro":             bln(loro_raw)           if loro_raw          > 0 else None,
            "issued_bonds":     bln(issued_bonds_raw)  if issued_bonds_raw  > 0 else None,
            "issued_bills":     bln(issued_bills_raw)  if issued_bills_raw  > 0 else None,
            "issued_securities":bln(issued_sec_raw)    if issued_sec_raw    > 0 else None,
            "provisions_rate":  provisions_rate,
            # ── Альтернативные показатели (ЦБ — упрощённая Ф101) ──
            "assets_cbr":       bln(assets_cbr_raw),
            "loans_cbr":        bln(loans_cbr_raw),
            "loans_fl_cbr":     bln(loans_fl_cbr)  if loans_fl_cbr > 0 else None,
            "loans_yl_cbr":     bln(loans_yl_cbr)  if loans_yl_cbr > 0 else None,
            "securities_cbr":   bln(securities_cbr) if securities_cbr > 0 else None,
            "deposits_fl_cbr":  bln(dep_fl_cbr)    if dep_fl_cbr > 0 else None,
            "deposits_yl_cbr":  bln(dep_yl_cbr)    if dep_yl_cbr > 0 else None,
            # Обороты
            "cash_turnover":        bln(cash_turnover)         if cash_turnover         > 0 else None,
            "nostro_turnover":      bln(nostro_turnover)       if nostro_turnover       > 0 else None,
            "deposits_fl_turnover": bln(deposits_fl_turnover)  if deposits_fl_turnover  > 0 else None,
            "dep_408_turnover":     bln(dep_408_turnover)      if dep_408_turnover      > 0 else None,
            "deposits_yl_turnover": bln(deposits_yl_turnover)  if deposits_yl_turnover  > 0 else None,
            "mbk_given_turnover":   bln(mbk_given_turnover)    if mbk_given_turnover    > 0 else None,
            "mbk_placed_cbr_turnover": bln(mbk_placed_cbr_turnover) if mbk_placed_cbr_turnover > 0 else None,
            "mbk_received_turnover":bln(mbk_received_turnover) if mbk_received_turnover > 0 else None,
            "_f101":            raw_codes,
        }

    def _parse_f135(self, xml_str: str) -> dict:
        """Парсит ответ Data135FormFull -> {название_норматива: значение}."""
        result: dict = {}
        try:
            root = ET.fromstring(xml_str)
            for elem in root.iter():
                if "}" in elem.tag:
                    elem.tag = elem.tag.split("}", 1)[1]
            for row in root.iter("F135_3"):
                c3 = (row.findtext("C3") or "").strip()
                v3 =  row.findtext("V3")
                if c3 and v3:
                    try:
                        result[c3] = float(v3)
                    except ValueError:
                        pass
        except Exception as e:
            logger.warning(f"[CBR] Ошибка парсинга Form 135: {e}")
        return result

    def _fetch_normatives(self, reg_num: str, dt_str: str) -> dict:
        """Загружает нормативы банка из Формы 135 (H1.0, H2, H3, H4)."""
        body = (
            f'<Data135FormFull xmlns="http://web.cbr.ru/">'
            f'<CredorgNumber>{reg_num}</CredorgNumber>'
            f'<OnDate>{dt_str}</OnDate>'
            f'</Data135FormFull>'
        )
        return self._parse_f135(self._soap_call("Data135FormFull", body))

    def get_raw_form101(self, reg_num: str, dt_str: str) -> list[dict]:
        """Возвращает все строки Формы 101 для выгрузки (только pln='А').

        Каждая строка: {numsc, ap, ap_label, vitg_thr, vitg_bln}.
        В демо-режиме возвращает пустой список.
        """
        if self._demo_mode:
            return []
        try:
            body = (
                f'<Data101FNew xmlns="http://web.cbr.ru/">'
                f'<CredorgNumber>{reg_num}</CredorgNumber>'
                f'<dt>{dt_str}</dt>'
                f'</Data101FNew>'
            )
            f101 = self._parse_f101(self._soap_call("Data101FNew", body))
            if not f101:
                return []
            # Собираем строки: только пары (numsc, ap) без "t"-ключей
            seen = {}
            for key, val in f101.items():
                if len(key) == 2:           # (numsc, ap) — остаток
                    numsc, ap = key
                    seen.setdefault((numsc, ap), {})["bln"] = val
                elif len(key) == 3:         # (numsc, ap, "t") — оборот
                    numsc, ap, _ = key
                    seen.setdefault((numsc, ap), {})["thr"] = val
            rows = []
            for (numsc, ap) in sorted(seen.keys(), key=lambda x: (x[0], x[1])):
                d     = seen[(numsc, ap)]
                vitg  = d.get("bln", 0.0)
                vthr  = d.get("thr", 0.0)
                rows.append({
                    "numsc":    numsc,
                    "ap":       ap,
                    "ap_label": "Дебет" if ap == "1" else "Кредит",
                    "vitg_bln": round(vitg  / 1_000_000, 6),   # остаток, млрд
                    "vitg_thr": round(vitg, 2),                 # остаток, тыс. руб. (для таблицы)
                    "oborot_thr": round(vthr, 2),               # оборот, тыс. руб.
                    "oborot_bln": round(vthr  / 1_000_000, 6),  # оборот, млрд
                })
            return rows
        except Exception as e:
            logger.warning(f"[CBR] get_raw_form101 {reg_num}: {e}")
            return []

    def _fetch_bank_form101(self, reg_num: str, dt_str: str) -> Optional[dict]:
        """Загружает Form 101 одного банка и возвращает метрики."""
        body = (
            f'<Data101FNew xmlns="http://web.cbr.ru/">'
            f'<CredorgNumber>{reg_num}</CredorgNumber>'
            f'<dt>{dt_str}</dt>'
            f'</Data101FNew>'
        )
        f101 = self._parse_f101(self._soap_call("Data101FNew", body))
        return self._compute_metrics_from_f101(f101) if f101 else None

    def get_bank_f101_codes(self, reg_num: str, ref_date: str) -> dict:
        """Возвращает словарь {code:ap: value_bln} для одного банка.

        Сначала пробует из кэша метрик (_f101 поле), при отсутствии —
        делает прямой SOAP-запрос к ЦБ (быстро, ~2 сек).
        Результат кэшируется в памяти для повторных вызовов.
        """
        d = self._parse_date(ref_date)
        mem_key = f"{reg_num}:{d.year}-{d.month:02d}"
        if mem_key in self._f101_code_cache:
            return self._f101_code_cache[mem_key]

        # 1) Пробуем из файлового кэша метрик (поле _f101)
        cache_key = f"metrics_v2_{d.year}_{d.month:02d}.json"
        cache_path = METRICS_DIR / cache_key
        if cache_path.exists():
            try:
                with open(cache_path, encoding="utf-8") as f:
                    data = json.load(f)
                for b in data:
                    if b.get("reg_num") == reg_num and b.get("_f101"):
                        # _f101 непустой — используем из файла
                        self._f101_code_cache[mem_key] = b["_f101"]
                        return b["_f101"]
            except Exception:
                pass

        # 2) Прямой SOAP-запрос — быстрый, только один банк
        dt_str = d.strftime("%Y-%m-%dT00:00:00")
        try:
            body = (
                f'<Data101FNew xmlns="http://web.cbr.ru/">'
                f'<CredorgNumber>{reg_num}</CredorgNumber>'
                f'<dt>{dt_str}</dt>'
                f'</Data101FNew>'
            )
            f101 = self._parse_f101(self._soap_call("Data101FNew", body))
            if not f101:
                return {}
            # Вычисляем полные метрики и получаем raw_codes через _compute_metrics_from_f101
            metrics = self._compute_metrics_from_f101(f101)
            if not metrics:
                return {}
            raw_codes = metrics.get("_f101", {})
            logger.info("[CBR] Direct F101 fetch for %s @ %s -> %d codes", reg_num, dt_str[:10], len(raw_codes))
            self._f101_code_cache[mem_key] = raw_codes
            # Обновляем файловый кэш — записываем новые _f101 и пересчитанные метрики
            if cache_path.exists():
                try:
                    with open(cache_path, encoding="utf-8") as f:
                        file_data = json.load(f)
                    updated = False
                    for bank in file_data:
                        if bank.get("reg_num") == reg_num:
                            bank["_f101"] = raw_codes
                            # Обновляем ключевые метрики (loans_fl etc.) если они изменились
                            for key in ("loans_fl", "loans_yl", "deposits_fl", "deposits_yl",
                                        "assets", "capital", "loans", "profit"):
                                if metrics.get(key) is not None:
                                    bank[key] = metrics[key]
                            bank.pop("demo", None)
                            updated = True
                            break
                    if updated:
                        with open(cache_path, "w", encoding="utf-8") as f:
                            json.dump(file_data, f, ensure_ascii=False, indent=2)
                        logger.info("[CBR] Обновлён файл кэша %s для банка %s", cache_path.name, reg_num)
                except Exception as e:
                    logger.debug("[CBR] Не удалось обновить файл кэша: %s", e)
            return raw_codes
        except Exception as e:
            logger.warning("[CBR] get_bank_f101_codes %s error: %s", reg_num, e)
            return {}

    def _fetch_form101_cbr(self, ref_date: date,
                           extra_banks: Optional[list] = None) -> list[dict]:
        """Загружает Form 101 с ЦБ через SOAP для ВСЕХ 305+ банков из реестра ЦБ.

        Параллельная загрузка (10 потоков). Если хотя бы один банк вернул
        реальные данные — для банков без SOAP-ответа заполняет синтетикой.
        Возвращает [] только при полном отсутствии реальных данных.

        extra_banks — дополнительный список закрытых банков (для include_closed).
        """
        dt_str = ref_date.strftime("%Y-%m-%dT00:00:00")

        # Полный список активных банков из реестра ЦБ (305 штук)
        all_banks = self.get_bank_list()
        if not all_banks:
            return []
        if extra_banks:
            existing_regs = {b["reg_num"] for b in all_banks}
            all_banks = all_banks + [b for b in extra_banks if b["reg_num"] not in existing_regs]

        # Справочник для rank_approx (только крупнейшие банки известны заранее)
        known_lookup = {b["reg_num"]: b for b in KNOWN_BANKS}
        # Оценка ранга для неизвестных банков по позиции в списке
        for pos, bank in enumerate(all_banks):
            if bank["reg_num"] not in known_lookup:
                bank["_rank_est"] = 30 + pos  # смещение: топ-30 покрыто KNOWN_BANKS

        bank_results: dict = {}
        real_count   = 0

        def fetch_one(bank):
            reg = bank["reg_num"]
            try:
                # Bulk-режим: короткий таймаут чтобы не застревать на медленных банках
                body = (
                    f'<Data101FNew xmlns="http://web.cbr.ru/">'
                    f'<CredorgNumber>{reg}</CredorgNumber>'
                    f'<dt>{dt_str}</dt>'
                    f'</Data101FNew>'
                )
                f101 = self._parse_f101(self._soap_call("Data101FNew", body, timeout=12))
                if f101:
                    metrics = self._compute_metrics_from_f101(f101)
                    if metrics:
                        # Нормативы пропускаем в bulk-режиме (грузятся по требованию
                        # для конкретного банка через get_bank_metrics).
                        # Это вдвое сокращает число SOAP-запросов.
                        metrics.update({"n1": None, "n2": None, "n3": None, "n4": None})
                        return reg, metrics, True
            except Exception as e:
                logger.debug("[CBR] Form101 %s: %s", reg, e)
            return reg, None, False

        logger.info("[CBR] Загрузка Form 101 для %d банков (bulk, 20 потоков)...", len(all_banks))
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = {executor.submit(fetch_one, b): b for b in all_banks}
            for future in as_completed(futures):
                bank = futures[future]
                try:
                    reg_num, metrics, is_real = future.result()
                except Exception as e:
                    logger.debug("[CBR] Поток %s: %s", bank["reg_num"], e)
                    reg_num, metrics, is_real = bank["reg_num"], None, False

                if is_real and metrics:
                    real_count += 1
                    entry = {
                        "reg_num":  reg_num,
                        "name":     bank["name"],
                        "short":    bank.get("short", bank["name"][:45]),
                        "region":   bank.get("region", ""),
                        "district": bank.get("district", ""),
                        **metrics,
                    }
                    if bank.get("status") == "closed":
                        entry["status"] = "closed"
                        entry["status_label"] = bank.get("status_label", "Закрыт")
                else:
                    # Синтетический фоллбэк для банков без SOAP-ответа
                    known = known_lookup.get(reg_num, {})
                    rank  = known.get("rank_approx") or bank.get("_rank_est", 200)
                    m = _generate_metrics(reg_num, ref_date, _bank_base_assets(rank, reg_num))
                    entry = {
                        "reg_num":  reg_num,
                        "name":     bank["name"],
                        "short":    bank.get("short", bank["name"][:45]),
                        "region":   bank.get("region", ""),
                        "district": bank.get("district", ""),
                        "demo":     True,
                        **m,
                    }
                bank_results[reg_num] = entry

        if real_count == 0:
            logger.warning("[CBR] Ни один банк не вернул реальные данные — демо-режим")
            return []

        logger.info("[CBR] Реальные данные: %d/%d банков", real_count, len(all_banks))
        result = list(bank_results.values())
        result.sort(key=lambda b: b.get("assets", 0), reverse=True)
        for i, b in enumerate(result):
            b["rank"] = i + 1
        return result

    def get_closed_banks_list(self) -> list[dict]:
        """Список закрытых банков (ликвидированных/с отозванной лицензией) с сайта ЦБ.

        Возвращает поля: reg_num, name, short, region, district, status,
        status_label, reg_date.
        Кэшируется на 24 часа.
        """
        cache_path = CACHE_DIR / "bank_list_closed.json"
        if cache_path.exists():
            import time as _time
            age = _time.time() - cache_path.stat().st_mtime
            if age < 86400:  # 24 ч
                try:
                    with open(cache_path, encoding="utf-8") as f:
                        cached = json.load(f)
                    if cached and len(cached) > 100:
                        return cached
                except Exception:
                    pass
        try:
            data = self._fetch_closed_banks_html()
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            return data
        except Exception as e:
            logger.warning("[CBR] Не удалось загрузить закрытые банки: %s", e)
            # Фоллбэк — закрытые банки из KNOWN_BANKS
            return [
                {
                    "reg_num":     b["reg_num"],
                    "name":        b["name"],
                    "short":       b["short"],
                    "region":      b["region"],
                    "district":    "Прочие",
                    "status":      b.get("status", "closed"),
                    "status_label": b.get("status_label", "Закрыт"),
                    "reg_date":    b.get("active_from", ""),
                }
                for b in KNOWN_BANKS if b.get("status") in ("revoked", "merged", "closed")
            ]

    def _fetch_closed_banks_html(self) -> list[dict]:
        """Парсит закрытые банки с HTML-страницы ЦБ РФ (статус ≠ Действующая, вид пустой)."""
        from bs4 import BeautifulSoup
        url  = f"{self.CBR_BASE}/banking_sector/credit/FullCoList/"
        resp = self._client.get(url, timeout=30, headers={"Accept-Language": "ru-RU,ru;q=0.9"})
        if resp.status_code != 200:
            raise ConnectionError(f"HTTP {resp.status_code}")

        soup = BeautifulSoup(resp.text, "lxml")
        table = soup.find("table")
        if not table:
            raise ValueError("Таблица банков не найдена")

        STATUS_LABELS = {
            "Ликвидация":    "В процессе ликвидации",
            "Отозванная":    "Лицензия отозвана",
            "Аннулированная": "Лицензия аннулирована",
        }
        banks: list = []
        seen_regs: set = set()

        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 8:
                continue
            vid    = tds[1].get_text(strip=True)
            if vid:
                continue                                     # НКО — пропускаем
            reg    = tds[2].get_text(strip=True)
            if not reg.isdigit() or reg in seen_regs:
                continue
            name   = tds[4].get_text(strip=True).replace("\xa0", " ")
            reg_date = tds[6].get_text(strip=True)
            status = tds[7].get_text(strip=True)
            addr   = tds[8].get_text(strip=True).replace("\xa0", " ") if len(tds) > 8 else ""

            if status not in STATUS_LABELS or not name:
                continue
            seen_regs.add(reg)

            city, district = _extract_city_from_address(addr)
            short = (
                name.replace("Публичное акционерное общество", "")
                    .replace("Акционерное общество", "")
                    .replace("Общество с ограниченной ответственностью", "")
                    .replace("«", "").replace("»", "").replace('"', "").strip()[:45]
            )
            banks.append({
                "reg_num":     reg,
                "name":        name,
                "short":       short or name[:45],
                "region":      city,
                "district":    district,
                "status":      "closed",
                "status_label": STATUS_LABELS[status],
                "reg_date":    reg_date,
            })

        logger.info("[CBR] Загружено %d закрытых банков с сайта ЦБ РФ", len(banks))
        return banks

    @staticmethod
    def _parse_date(date_str: Optional[str]) -> date:
        """Парсит дату, по умолчанию — последний доступный месяц."""
        if date_str:
            try:
                return datetime.strptime(date_str[:10], "%Y-%m-%d").date()
            except ValueError:
                pass
        # Данные ЦБ с задержкой ~45 дней; если сейчас после 15-го — прошлый месяц
        today = date.today()
        if today.day >= 15:
            month = today.month - 1 if today.month > 1 else 12
            year  = today.year if today.month > 1 else today.year - 1
        else:
            month = today.month - 2 if today.month > 2 else 12 + today.month - 2
            year  = today.year if today.month > 2 else today.year - 1
        return date(year, month if month > 0 else 1, 1)

    @staticmethod
    def _generate_date_range(
        d_from: date, d_to: date, period: str
    ) -> list[date]:
        """Формирует список дат с нужной гранулярностью.

        Не включает текущий месяц — ЦБ публикует данные Ф101 с задержкой ~1 мес,
        поэтому данные текущего месяца всегда неполные/отсутствуют.
        """
        from datetime import date as _date_cls
        _today = _date_cls.today()
        # Последний допустимый месяц = предыдущий от текущего
        _last_month = date(_today.year, _today.month, 1)
        if _last_month.month == 1:
            _max_date = date(_last_month.year - 1, 12, 1)
        else:
            _max_date = date(_last_month.year, _last_month.month - 1, 1)

        dates = []
        current = date(d_from.year, d_from.month, 1)
        end = min(date(d_to.year, d_to.month, 1), _max_date)

        while current <= end:
            dates.append(current)
            if period == "month":
                m = current.month + 1
                y = current.year + (m - 1) // 12
                m = ((m - 1) % 12) + 1
                current = date(y, m, 1)
            elif period == "quarter":
                m = current.month + 3
                y = current.year + (m - 1) // 12
                m = ((m - 1) % 12) + 1
                current = date(y, m, 1)
            else:  # year
                current = date(current.year + 1, current.month, 1)

        return dates
