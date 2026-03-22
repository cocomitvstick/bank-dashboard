"""
FastAPI сервер — аналитический дашборд Примсоцбанка.
Запуск: python main.py
Документация API: http://localhost:8080/docs
"""
# ---------------------------------------------------------------------------
# Логирование — ДО любых импортов, чтобы поймать ошибки импорта
# ---------------------------------------------------------------------------
import logging
import os
import sys

_log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "server.log")
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(_log_file, encoding="utf-8"),
    ],
)
logger = logging.getLogger("main")

logger.info("=" * 60)
logger.info("  ПСБ Analytics — старт")
logger.info("  Python %s", sys.version)
logger.info("=" * 60)

# ---------------------------------------------------------------------------
# Стандартные библиотеки
# ---------------------------------------------------------------------------
logger.debug("Импорт стандартных библиотек...")
import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import Optional
logger.debug("OK: стандартные библиотеки загружены")

# ---------------------------------------------------------------------------
# Третьесторонние библиотеки
# ---------------------------------------------------------------------------
logger.debug("Импорт uvicorn...")
try:
    import uvicorn
    logger.debug("OK: uvicorn %s", uvicorn.__version__)
except ImportError as e:
    logger.critical("ОШИБКА: uvicorn не установлен — %s", e)
    logger.critical("Запустите: pip install uvicorn")
    sys.exit(1)

logger.debug("Импорт fastapi...")
try:
    import fastapi
    from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import JSONResponse
    from fastapi.staticfiles import StaticFiles
    logger.debug("OK: fastapi %s", fastapi.__version__)
except ImportError as e:
    logger.critical("ОШИБКА: fastapi не установлен — %s", e)
    logger.critical("Запустите: pip install fastapi")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Пути
# ---------------------------------------------------------------------------
BASE_DIR     = Path(__file__).parent
FRONTEND_DIR = BASE_DIR.parent / "frontend"
DATA_DIR     = BASE_DIR / "data"

SETTINGS_PATH = DATA_DIR / "settings.json"

DEFAULT_SETTINGS = {
    "connection_mode": "auto",   # "auto" | "direct" | "proxy"
    "proxy_url": "",             # "http://user:pass@host:port" или "socks5://host:port"
    "timeout": 15,
}

def load_settings() -> dict:
    """Загружает настройки из JSON. Возвращает дефолт если файл отсутствует."""
    if SETTINGS_PATH.exists():
        try:
            with open(SETTINGS_PATH, encoding="utf-8") as f:
                saved = json.load(f)
            return {**DEFAULT_SETTINGS, **saved}
        except Exception:
            pass
    return dict(DEFAULT_SETTINGS)

def save_settings(settings: dict) -> None:
    """Сохраняет настройки в JSON."""
    SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(SETTINGS_PATH, "w", encoding="utf-8") as f:
        json.dump(settings, f, ensure_ascii=False, indent=2)

def _build_http_client(settings: dict):
    """Строит httpx.Client с учётом настроек прокси."""
    import httpx
    mode    = settings.get("connection_mode", "auto")
    timeout = int(settings.get("timeout", 15))
    proxy_url = settings.get("proxy_url", "").strip()

    if mode == "direct":
        # trust_env=False — игнорировать HTTP_PROXY / HTTPS_PROXY (VPN, системный прокси)
        return httpx.Client(timeout=timeout, follow_redirects=True, trust_env=False)
    elif mode == "proxy" and proxy_url:
        try:
            # httpx 0.26–0.27: proxies dict
            return httpx.Client(
                proxies={"all://": proxy_url},
                timeout=timeout, follow_redirects=True, trust_env=False,
            )
        except TypeError:
            # httpx 0.28+: используем mounts
            transport = httpx.HTTPTransport(proxy=httpx.Proxy(proxy_url))
            return httpx.Client(
                transport=transport,
                timeout=timeout, follow_redirects=True, trust_env=False,
            )
    else:
        # auto — системные настройки (VPN/прокси работают)
        return httpx.Client(timeout=timeout, follow_redirects=True)

logger.info("BASE_DIR:     %s", BASE_DIR)
logger.info("FRONTEND_DIR: %s (существует: %s)", FRONTEND_DIR, FRONTEND_DIR.exists())
logger.info("DATA_DIR:     %s (существует: %s)", DATA_DIR, DATA_DIR.exists())

# Создаём нужные папки
for d in [DATA_DIR, DATA_DIR / "metrics", DATA_DIR / "cache"]:
    d.mkdir(parents=True, exist_ok=True)
    logger.debug("Папка: %s", d)

# ---------------------------------------------------------------------------
# Локальные модули
# ---------------------------------------------------------------------------
logger.debug("Импорт cbr_parser...")
try:
    from cbr_parser import CBRParser
    logger.debug("OK: cbr_parser загружен")
except Exception as e:
    logger.critical("ОШИБКА при импорте cbr_parser: %s", e, exc_info=True)
    sys.exit(1)

logger.debug("Импорт moex_parser...")
try:
    from moex_parser import MOEXParser
    logger.debug("OK: moex_parser загружен")
except Exception as e:
    logger.critical("ОШИБКА при импорте moex_parser: %s", e, exc_info=True)
    sys.exit(1)

logger.debug("Импорт data_processor...")
try:
    from data_processor import DataProcessor
    logger.debug("OK: data_processor загружен")
except Exception as e:
    logger.critical("ОШИБКА при импорте data_processor: %s", e, exc_info=True)
    sys.exit(1)

logger.debug("Импорт scheduler...")
try:
    from scheduler import start_scheduler, stop_scheduler
    logger.debug("OK: scheduler загружен")
except Exception as e:
    logger.critical("ОШИБКА при импорте scheduler: %s", e, exc_info=True)
    sys.exit(1)

# ---------------------------------------------------------------------------
# Инициализация объектов
# ---------------------------------------------------------------------------
logger.debug("Создание CBRParser...")
try:
    cbr = CBRParser()
    logger.info("OK: CBRParser создан, demo_mode=%s", cbr.is_demo_mode)
    _startup_settings = load_settings()
    cbr._client = _build_http_client(_startup_settings)
    logger.info("HTTP клиент: mode=%s, timeout=%s", _startup_settings.get("connection_mode"), _startup_settings.get("timeout"))
except Exception as e:
    logger.critical("ОШИБКА при создании CBRParser: %s", e, exc_info=True)
    sys.exit(1)

logger.debug("Создание MOEXParser...")
try:
    moex = MOEXParser()
    logger.debug("OK: MOEXParser создан")
except Exception as e:
    logger.critical("ОШИБКА при создании MOEXParser: %s", e, exc_info=True)
    sys.exit(1)

logger.debug("Создание DataProcessor...")
try:
    dp = DataProcessor()
    logger.debug("OK: DataProcessor создан")
except Exception as e:
    logger.critical("ОШИБКА при создании DataProcessor: %s", e, exc_info=True)
    sys.exit(1)

# ---------------------------------------------------------------------------
# FastAPI приложение
# ---------------------------------------------------------------------------
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(application: "FastAPI"):
    # startup
    logger.info("=" * 60)
    logger.info("  ПСБ Analytics API запущен")
    logger.info("  http://localhost:8000/app  — дашборд")
    logger.info("  http://localhost:8000/docs — Swagger")
    logger.info("=" * 60)
    try:
        start_scheduler(cbr)
        logger.info("OK: планировщик запущен")
    except Exception as e:
        logger.warning("Планировщик не запущен: %s", e)
    # Автоматически открываем браузер
    import threading, webbrowser, time
    def _open_browser():
        time.sleep(1.5)
        webbrowser.open("http://localhost:8000/app")
    threading.Thread(target=_open_browser, daemon=True).start()
    yield
    # shutdown
    try:
        stop_scheduler()
    except Exception:
        pass

logger.debug("Создание FastAPI app...")
app = FastAPI(
    title="ПСБ Analytics API",
    description="Аналитический дашборд банковского сектора РФ — Примсоцбанк",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
logger.debug("OK: FastAPI app создан, CORS настроен")

# Обслуживаем фронтенд статически
if FRONTEND_DIR.exists():
    try:
        app.mount("/app", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
        logger.info("OK: фронтенд смонтирован из %s", FRONTEND_DIR)
    except Exception as e:
        logger.warning("Не удалось смонтировать фронтенд: %s", e)
else:
    logger.warning("Папка frontend не найдена: %s", FRONTEND_DIR)

# ---------------------------------------------------------------------------
# Константы
# ---------------------------------------------------------------------------
PRIMSOCBANK_REG = "2733"

METRICS_META = {
    # ══════════════════════════════════════════════════════════════════════════
    # Методология Банки.ру — показатели совпадают с сайтом banki.ru/banks/ratings
    # ══════════════════════════════════════════════════════════════════════════

    # ── Масштаб баланса (Банки.ру) ───────────────────────────────────────────
    "assets":           {"label": "Активы нетто (Банки.ру)",              "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "capital":          {"label": "Капитал по Ф.123 (Банки.ру)",          "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "liabilities":      {"label": "Обязательства (Банки.ру)",             "unit": "млрд ₽", "ascending": False, "method": "banki"},

    # ── Кредитный портфель (Банки.ру) ────────────────────────────────────────
    "loans":            {"label": "Кредитный портфель (Банки.ру)",        "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "loans_fl":         {"label": "Кредиты физ. лицам (Банки.ру)",        "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "loans_yl":         {"label": "Кредиты юр. лицам (Банки.ру)",         "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "npl_abs":          {"label": "Просроченная задолженность",           "unit": "млрд ₽", "ascending": True,  "method": "banki"},
    "provisions":       {"label": "Резервы (РВПС)",                       "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "provisions_rate":  {"label": "Уровень резервирования",               "unit": "%",       "ascending": False, "method": "banki"},

    # ── Вклады и депозиты (Банки.ру) ─────────────────────────────────────────
    "deposits":         {"label": "Вклады и депозиты итого (Банки.ру)",    "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "deposits_fl":      {"label": "Вклады физ. лиц (Банки.ру)",           "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "deposits_fl_term": {"label": "Срочные вклады ФЛ (423+426)",          "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "dep_522":          {"label": "Сберсертификаты ФЛ (522)",             "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "dep_408":          {"label": "Текущие счета ФЛ (408)",               "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "deposits_fl_turnover": {"label": "Вклады ФЛ — оборот",              "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "dep_408_turnover": {"label": "Текущие счета ФЛ — оборот",            "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "deposits_yl":      {"label": "Средства юр. лиц (Банки.ру)",          "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "deposits_yl_turnover": {"label": "Средства ЮЛ — оборот",            "unit": "млрд ₽", "ascending": False, "method": "banki"},

    # ── МБК и прочие обязательства (Банки.ру) ────────────────────────────────
    "mbk_received":     {"label": "Привлечённые МБК (Банки.ру)",          "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "mbk_received_cbr": {"label": "Привлечённые от ЦБ РФ (310)",          "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "mbk_received_turnover": {"label": "Привлечённые МБК — оборот",       "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "loro":             {"label": "ЛОРО-счета (30109+30111)",              "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "issued_bonds":     {"label": "Выпущенные облигации (520)",            "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "issued_bills":     {"label": "Выпущенные векселя (523)",              "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "issued_securities":{"label": "Выпущенные ОиВ (520+523)",             "unit": "млрд ₽", "ascending": False, "method": "banki"},

    # ── Структура активов (Банки.ру) ─────────────────────────────────────────
    "liquid_assets":    {"label": "Высоколиквидные активы (Банки.ру)",     "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "cash":             {"label": "Денежные средства в кассе (Банки.ру)",  "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "cash_turnover":    {"label": "Касса — оборот",                        "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "nostro":           {"label": "НОСТРО-счета (Банки.ру)",               "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "nostro_turnover":  {"label": "НОСТРО — оборот",                       "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "reserves":         {"label": "Обязательные резервы (302)",            "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "mbk_given":        {"label": "Выданные МБК (Банки.ру)",               "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "mbk_given_turnover":{"label": "МБК выданные — оборот",               "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "mbk_placed_cbr":   {"label": "Размещённые МБК в ЦБ РФ (319)",        "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "mbk_placed_cbr_turnover": {"label": "МБК в ЦБ РФ — оборот",          "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "securities":       {"label": "Вложения в цен. бумаги (Банки.ру)",     "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "securities_bonds": {"label": "Вложения в облигации (Банки.ру)",       "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "securities_stocks":{"label": "Вложения в акции (506)",                "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "securities_repo":  {"label": "Бумаги в РЕПО (502+504)",               "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "securities_bills": {"label": "Вложения в векселя (512-515)",          "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "investments":      {"label": "Вложения в капиталы орг. (601)",        "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "fixed_assets":     {"label": "Основные средства (604)",               "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "intangibles":      {"label": "Нематериальные активы (609)",           "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "fixed_assets_full":{"label": "ОС и НМА (604+609)",                    "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "other_assets":     {"label": "Прочие активы (Банки.ру)",              "unit": "млрд ₽", "ascending": False, "method": "banki"},

    # ── Доходность и риски ───────────────────────────────────────────────────
    "profit":           {"label": "Чистая прибыль",                       "unit": "млрд ₽", "ascending": False, "method": "banki"},
    "interest_income":  {"label": "Процентные доходы",                     "unit": "млрд ₽", "ascending": False, "method": "common"},
    "interest_expense": {"label": "Процентные расходы",                    "unit": "млрд ₽", "ascending": True,  "method": "common"},
    "roa":              {"label": "ROA (рентабельность активов)",           "unit": "%",       "ascending": False, "method": "banki"},
    "roe":              {"label": "ROE (рентабельность капитала)",          "unit": "%",       "ascending": False, "method": "banki"},
    "nim":              {"label": "NIM (чистая процентная маржа)",          "unit": "%",       "ascending": False, "method": "common"},
    "npl":              {"label": "NPL (уровень просрочки)",               "unit": "%",       "ascending": True,  "method": "banki"},

    # ══════════════════════════════════════════════════════════════════════════
    # Методология ЦБ — расчёт по классической формуле ЦБ РФ (отличается от Банки.ру)
    # ══════════════════════════════════════════════════════════════════════════

    "assets_cbr":       {"label": "Активы нетто (ЦБ)",                    "unit": "млрд ₽", "ascending": False, "method": "cbr"},
    "loans_cbr":        {"label": "Кредитный портфель (ЦБ)",              "unit": "млрд ₽", "ascending": False, "method": "cbr"},
    "loans_fl_cbr":     {"label": "Кредиты физ. лицам (ЦБ)",              "unit": "млрд ₽", "ascending": False, "method": "cbr"},
    "loans_yl_cbr":     {"label": "Кредиты юр. лицам (ЦБ)",               "unit": "млрд ₽", "ascending": False, "method": "cbr"},
    "deposits_fl_cbr":  {"label": "Вклады физ. лиц (ЦБ)",                 "unit": "млрд ₽", "ascending": False, "method": "cbr"},
    "deposits_yl_cbr":  {"label": "Средства юр. лиц (ЦБ)",               "unit": "млрд ₽", "ascending": False, "method": "cbr"},
    "securities_cbr":   {"label": "Вложения в цен. бумаги (ЦБ)",          "unit": "млрд ₽", "ascending": False, "method": "cbr"},

    # ── Нормативы ЦБ (Форма 135) ────────────────────────────────────────────
    "n1":               {"label": "Норматив Н1.0 (ЦБ)",                   "unit": "%",       "ascending": False, "method": "cbr"},
    "n2":               {"label": "Норматив Н2 (ЦБ)",                     "unit": "%",       "ascending": False, "method": "cbr"},
    "n3":               {"label": "Норматив Н3 (ЦБ)",                     "unit": "%",       "ascending": False, "method": "cbr"},
    "n4":               {"label": "Норматив Н4 (ЦБ)",                     "unit": "%",       "ascending": False, "method": "cbr"},
}

_last_refresh: Optional[datetime] = None

# Статус фонового предзагрузчика (bulk-prefetch)
_prefetch_status: dict = {
    "running": False,
    "total":   0,
    "done":    0,
    "errors":  0,
    "started": None,
    "finished": None,
}
_prefetch_stop_flag: bool = False   # флаг запроса остановки


def _default_date() -> str:
    return CBRParser._parse_date(None).strftime("%Y-%m-%d")


def _group_n(group: str) -> Optional[int]:
    """Из строки 'top10' -> 10, 'all' -> None."""
    if group == "all":
        return None
    if group.startswith("top"):
        try:
            return int(group[3:])
        except ValueError:
            pass
    return None


# ---------------------------------------------------------------------------
# Эндпоинты
# ---------------------------------------------------------------------------

@app.get("/")
def root():
    return {"status": "ok", "message": "ПСБ Analytics API. Frontend: /app"}


@app.get("/api/status")
def api_status():
    """Статус данных: дата последнего обновления, режим работы."""
    d = _default_date()
    # Считаем сколько месяцев уже закэшировано (реальные данные, не demo-заглушки)
    metrics_dir = Path(__file__).parent / "data" / "metrics"
    cached_months = 0
    real_banks_count = 0
    demo_banks_count = 0
    if metrics_dir.exists():
        for f in metrics_dir.glob("metrics_v2_*.json"):
            if "_closed" in f.name:
                continue
            try:
                with open(f, encoding="utf-8") as fp:
                    data = json.load(fp)
                if isinstance(data, list) and len(data) > 0:
                    cached_months += 1
                    real_banks_count += sum(1 for b in data if not b.get("demo"))
                    demo_banks_count += sum(1 for b in data if b.get("demo"))
            except Exception:
                pass
    return {
        "last_update_date":    d,
        "demo_mode":           cbr.is_demo_mode,
        "last_refresh":        _last_refresh.isoformat() if _last_refresh else None,
        "primsocbank_reg":     PRIMSOCBANK_REG,
        "server_time":         datetime.now().isoformat(),
        "cached_months_count": cached_months,
        "real_banks_count":    real_banks_count,
        "demo_banks_count":    demo_banks_count,
    }


@app.get("/api/settings")
def get_settings():
    """Настройки подключения (прокси, таймаут)."""
    s = load_settings()
    # Не возвращаем proxy_url целиком — маскируем пароль если есть
    masked = dict(s)
    url = s.get("proxy_url", "")
    if "@" in url and ":" in url:
        # "http://user:PASS@host:port" -> "http://user:***@host:port"
        try:
            from urllib.parse import urlparse, urlunparse
            p = urlparse(url)
            if p.password:
                masked["proxy_url"] = url.replace(f":{p.password}@", ":***@")
        except Exception:
            pass
    return masked


@app.post("/api/settings")
def update_settings(body: dict):
    """Обновляет настройки подключения и применяет немедленно."""
    allowed = {"connection_mode", "proxy_url", "timeout"}
    new_settings = {k: v for k, v in body.items() if k in allowed}
    # Валидация
    if "connection_mode" in new_settings and new_settings["connection_mode"] not in ("auto", "direct", "proxy"):
        raise HTTPException(400, "connection_mode должен быть: auto | direct | proxy")
    if "timeout" in new_settings:
        try:
            new_settings["timeout"] = max(5, min(120, int(new_settings["timeout"])))
        except (ValueError, TypeError):
            raise HTTPException(400, "timeout должен быть числом от 5 до 120")

    current = load_settings()
    current.update(new_settings)
    save_settings(current)

    # Применяем немедленно — пересоздаём HTTP-клиент
    cbr._client = _build_http_client(current)
    logger.info("Настройки обновлены: %s", {k: v for k, v in current.items() if k != "proxy_url"})
    return {"ok": True, "settings": current}


@app.get("/api/metrics/list")
def metrics_list():
    """Справочник доступных показателей."""
    return [
        {"code": code, **meta}
        for code, meta in METRICS_META.items()
    ]


@app.get("/api/banks")
def get_banks():
    """Список активных банков (без метрик)."""
    return cbr.get_bank_list()


@app.get("/api/banks/all")
def get_all_banks():
    """Список ВСЕХ банков включая исторически неактивные.
    Содержит поля active_to, status, status_label для закрытых банков.
    Используется для поиска по историческим данным.
    """
    return cbr.get_all_banks_list()


@app.get("/api/banks/closed")
def get_closed_banks(q: str = Query(None, description="Поиск по названию")):
    """Список закрытых банков (с отозванной/аннулированной лицензией) с сайта ЦБ РФ.

    Поддерживает поиск по параметру q=<строка>.
    """
    banks = cbr.get_closed_banks_list()
    if q:
        ql = q.lower()
        banks = [b for b in banks if ql in b.get("name", "").lower()
                 or ql in b.get("short", "").lower()
                 or ql in b.get("reg_num", "")]
    return banks


@app.get("/api/prefetch/status")
def get_prefetch_status():
    """Статус фонового предзагрузчика данных."""
    return _prefetch_status


@app.post("/api/prefetch")
def start_prefetch(
    background_tasks: BackgroundTasks,
    date_from: str = Query(None, description="Начало периода YYYY-MM-DD"),
    date_to:   str = Query(None, description="Конец периода YYYY-MM-DD"),
    include_closed: bool = Query(False, description="Включить закрытые банки"),
):
    """Запускает фоновую загрузку Form 101 для всех банков за все периоды диапазона.

    Нужен для прогрева кэша перед анализом. Результат — кэш-файлы
    metrics_v2_YYYY_MM.json в папке data/metrics/.
    """
    from datetime import date as _date
    import threading

    if _prefetch_status["running"]:
        return {"status": "already_running", **_prefetch_status}

    d_from = CBRParser._parse_date(date_from)
    d_to   = CBRParser._parse_date(date_to) if date_to else CBRParser._parse_date(None)

    # Собираем список дат по месяцам
    months = []
    y, m = d_from.year, d_from.month
    while (y, m) <= (d_to.year, d_to.month):
        months.append(_date(y, m, 1))
        m += 1
        if m > 12:
            m = 1
            y += 1

    def _run():
        global _prefetch_status, _prefetch_stop_flag
        _prefetch_stop_flag = False
        _prefetch_status.update({"running": True, "total": len(months),
                                  "done": 0, "errors": 0,
                                  "started": datetime.now().isoformat(),
                                  "finished": None})
        suffix = "_closed" if include_closed else ""
        for dt in months:
            if _prefetch_stop_flag:
                logger.info("Prefetch остановлен пользователем на %d/%d",
                            _prefetch_status["done"], _prefetch_status["total"])
                break
            cache_path = Path(__file__).parent / "data" / "metrics" / \
                         f"metrics_v2_{dt.year}_{dt.month:02d}{suffix}.json"
            if cache_path.exists():
                _prefetch_status["done"] += 1
                continue
            try:
                cbr.get_metrics_for_date(dt.strftime("%Y-%m-%d"),
                                          include_closed=include_closed)
                _prefetch_status["done"] += 1
            except Exception as e:
                logger.warning("Prefetch %s/%s ошибка: %s", dt.year, dt.month, e)
                _prefetch_status["errors"] += 1
        _prefetch_status["running"]  = False
        _prefetch_status["finished"] = datetime.now().isoformat()
        logger.info("Prefetch завершён: %d/%d, ошибок %d",
                    _prefetch_status["done"], _prefetch_status["total"],
                    _prefetch_status["errors"])

    threading.Thread(target=_run, daemon=True).start()
    return {"status": "started", "months": len(months), **_prefetch_status}


@app.post("/api/prefetch/stop")
def stop_prefetch():
    """Останавливает текущую фоновую загрузку после завершения текущего месяца."""
    global _prefetch_stop_flag
    if not _prefetch_status.get("running"):
        return {"status": "not_running"}
    _prefetch_stop_flag = True
    return {"status": "stop_requested", "done": _prefetch_status["done"],
            "total": _prefetch_status["total"]}


# ---------------------------------------------------------------------------
# Новые эндпоинты: проверка полноты данных + дозагрузка
# ---------------------------------------------------------------------------

@app.get("/api/data/completeness")
def data_completeness():
    """Проверяет полноту загруженных данных по всем кэш-файлам.

    Возвращает список месяцев с количеством банков:
    - total: всего банков в файле
    - real: банков с реальными данными SOAP (demo=False или demo отсутствует)
    - demo: банков с синтетическими данными (demo=True)
    - no_norms: банков с n1=None (нормативы не загружены)
    - complete: True если real==total и no_norms==0
    """
    metrics_dir = Path(__file__).parent / "data" / "metrics"
    result = []
    if metrics_dir.exists():
        for f in sorted(metrics_dir.glob("metrics_v2_*.json")):
            if "_closed" in f.name:
                continue
            try:
                with open(f, encoding="utf-8") as fp:
                    data = json.load(fp)
                if not isinstance(data, list):
                    continue
                total    = len(data)
                demo_cnt = sum(1 for b in data if b.get("demo"))
                real_cnt = total - demo_cnt
                no_norms = sum(1 for b in data if b.get("n1") is None and not b.get("demo"))
                # Имя файла → месяц
                name_parts = f.stem.replace("metrics_v2_", "").split("_")
                year_m = f"{name_parts[0]}-{name_parts[1]}" if len(name_parts) >= 2 else f.stem
                result.append({
                    "month":    year_m,
                    "file":     f.name,
                    "total":    total,
                    "real":     real_cnt,
                    "demo":     demo_cnt,
                    "no_norms": no_norms,
                    "complete": demo_cnt == 0 and no_norms == 0,
                    "size_kb":  round(f.stat().st_size / 1024),
                })
            except Exception as e:
                logger.warning("completeness check %s: %s", f.name, e)
    return {"months": result, "metrics_dir": str(metrics_dir)}


# Глобальный статус операций дозагрузки
_repair_status: dict = {
    "running": False, "operation": None,
    "total": 0, "done": 0, "errors": 0,
    "started": None, "finished": None, "log": [],
}


@app.get("/api/data/repair-status")
def get_repair_status():
    """Статус операции дозагрузки данных."""
    return _repair_status


@app.post("/api/data/load-norms")
def load_norms(
    background_tasks: BackgroundTasks,
    date_from: str = Query(None, description="Начало периода YYYY-MM-DD"),
    date_to:   str = Query(None, description="Конец периода YYYY-MM-DD"),
):
    """Загружает нормативы (Н1/Н2/Н3/Н4) для банков у которых они отсутствуют.

    Полезно после переноса на оффлайн ПК — нормативы пропускаются при bulk-загрузке
    для экономии времени, но нужны для отображения показателей Н1/Н2 в дашборде.
    """
    import threading
    global _repair_status

    if _repair_status["running"]:
        return {"status": "already_running", **_repair_status}

    from datetime import date as _date
    metrics_dir = Path(__file__).parent / "data" / "metrics"
    d_from = CBRParser._parse_date(date_from)
    d_to   = CBRParser._parse_date(date_to) if date_to else CBRParser._parse_date(None)

    def _run():
        global _repair_status
        _repair_status = {"running": True, "operation": "load_norms",
                          "total": 0, "done": 0, "errors": 0,
                          "started": datetime.now().isoformat(), "finished": None, "log": []}
        try:
            # Сканируем кэш-файлы в диапазоне дат
            tasks = []  # (file_path, reg_num, dt_str)
            y, m = d_from.year, d_from.month
            while (_date(y, m, 1)) <= d_to:
                cache_path = metrics_dir / f"metrics_v2_{y}_{m:02d}.json"
                if cache_path.exists():
                    try:
                        with open(cache_path, encoding="utf-8") as fp:
                            data = json.load(fp)
                        dt_str = _date(y, m, 1).strftime("%Y-%m-%dT00:00:00")
                        for b in data:
                            if b.get("n1") is None and not b.get("demo"):
                                tasks.append((cache_path, data, b["reg_num"], dt_str))
                    except Exception:
                        pass
                m += 1
                if m > 12: m = 1; y += 1
            _repair_status["total"] = len(tasks)
            msg = f"Нормативов для загрузки: {len(tasks)}"
            _repair_status["log"].append(msg)
            logger.info("[Repair/norms] %s", msg)

            # Группируем по файлу — обновляем весь файл за раз
            from collections import defaultdict
            by_file: dict = defaultdict(lambda: {"path": None, "data": None, "regs": {}})
            for (cache_path, data, reg_num, dt_str) in tasks:
                key = str(cache_path)
                by_file[key]["path"] = cache_path
                by_file[key]["data"] = data
                by_file[key]["regs"][reg_num] = dt_str

            for key, entry in by_file.items():
                cache_path = entry["path"]
                data       = entry["data"]
                regs       = entry["regs"]
                modified   = False
                for reg_num, dt_str in regs.items():
                    try:
                        norms = cbr._fetch_normatives(reg_num, dt_str)
                        if norms:
                            for bank in data:
                                if bank["reg_num"] == reg_num:
                                    bank.update({
                                        "n1": norms.get("Н1.0"),
                                        "n2": norms.get("Н2"),
                                        "n3": norms.get("Н3"),
                                        "n4": norms.get("Н4"),
                                    })
                                    modified = True
                                    break
                            _repair_status["done"] += 1
                        else:
                            _repair_status["errors"] += 1
                    except Exception as e:
                        logger.debug("[Repair/norms] %s %s: %s", reg_num, dt_str[:7], e)
                        _repair_status["errors"] += 1
                if modified:
                    with open(cache_path, "w", encoding="utf-8") as fp:
                        json.dump(data, fp, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error("[Repair/norms] Ошибка: %s", e)
            _repair_status["errors"] += 1
        finally:
            _repair_status["running"]  = False
            _repair_status["finished"] = datetime.now().isoformat()
            _repair_status["log"].append(f"Готово: загружено {_repair_status['done']}, ошибок {_repair_status['errors']}")

    threading.Thread(target=_run, daemon=True).start()
    return {"status": "started", **_repair_status}


@app.post("/api/data/retry-demo")
def retry_demo_banks(
    background_tasks: BackgroundTasks,
    date_from: str = Query(None, description="Начало периода YYYY-MM-DD"),
    date_to:   str = Query(None, description="Конец периода YYYY-MM-DD"),
):
    """Повторяет SOAP-запросы для банков с синтетическими данными (demo=True).

    Используйте если некоторые банки не успели загрузиться за таймаут при bulk-загрузке.
    Таймаут увеличен до 25 сек — больше шансов получить ответ.
    """
    import threading
    global _repair_status

    if _repair_status["running"]:
        return {"status": "already_running", **_repair_status}

    from datetime import date as _date
    metrics_dir = Path(__file__).parent / "data" / "metrics"
    d_from = CBRParser._parse_date(date_from)
    d_to   = CBRParser._parse_date(date_to) if date_to else CBRParser._parse_date(None)

    def _run():
        global _repair_status
        _repair_status = {"running": True, "operation": "retry_demo",
                          "total": 0, "done": 0, "errors": 0,
                          "started": datetime.now().isoformat(), "finished": None, "log": []}
        try:
            tasks = []
            y, m = d_from.year, d_from.month
            while (_date(y, m, 1)) <= d_to:
                cache_path = metrics_dir / f"metrics_v2_{y}_{m:02d}.json"
                if cache_path.exists():
                    try:
                        with open(cache_path, encoding="utf-8") as fp:
                            data = json.load(fp)
                        dt_str = _date(y, m, 1).strftime("%Y-%m-%dT00:00:00")
                        demo_banks = [b for b in data if b.get("demo")]
                        if demo_banks:
                            tasks.append((cache_path, data, demo_banks, dt_str))
                    except Exception:
                        pass
                m += 1
                if m > 12: m = 1; y += 1

            total_demo = sum(len(t[2]) for t in tasks)
            _repair_status["total"] = total_demo
            msg = f"Синтетических записей для повтора: {total_demo} в {len(tasks)} месяцах"
            _repair_status["log"].append(msg)
            logger.info("[Repair/retry] %s", msg)

            for (cache_path, data, demo_banks, dt_str) in tasks:
                modified = False
                for bank in demo_banks:
                    reg_num = bank["reg_num"]
                    try:
                        # Увеличенный таймаут для повтора
                        body = (
                            f'<Data101FNew xmlns="http://web.cbr.ru/">'
                            f'<CredorgNumber>{reg_num}</CredorgNumber>'
                            f'<dt>{dt_str}</dt>'
                            f'</Data101FNew>'
                        )
                        xml_str = cbr._soap_call("Data101FNew", body, timeout=25)
                        f101    = cbr._parse_f101(xml_str)
                        if f101:
                            metrics = cbr._compute_metrics_from_f101(f101)
                            if metrics:
                                # Загружаем и нормативы (уже не bulk, можно)
                                try:
                                    norms = cbr._fetch_normatives(reg_num, dt_str)
                                    if norms:
                                        metrics.update({
                                            "n1": norms.get("Н1.0"),
                                            "n2": norms.get("Н2"),
                                            "n3": norms.get("Н3"),
                                            "n4": norms.get("Н4"),
                                        })
                                except Exception:
                                    pass
                                # Обновляем запись в данных
                                for i, b in enumerate(data):
                                    if b["reg_num"] == reg_num:
                                        data[i] = {**b, **metrics, "demo": False}
                                        modified = True
                                        break
                                _repair_status["done"] += 1
                                continue
                        _repair_status["errors"] += 1
                    except Exception as e:
                        logger.debug("[Repair/retry] %s %s: %s", reg_num, dt_str[:7], e)
                        _repair_status["errors"] += 1
                if modified:
                    with open(cache_path, "w", encoding="utf-8") as fp:
                        json.dump(data, fp, ensure_ascii=False, indent=2)
                    logger.info("[Repair/retry] Обновлён файл %s", cache_path.name)
        except Exception as e:
            logger.error("[Repair/retry] Ошибка: %s", e)
        finally:
            _repair_status["running"]  = False
            _repair_status["finished"] = datetime.now().isoformat()
            msg = f"Готово: обновлено {_repair_status['done']}, не удалось {_repair_status['errors']}"
            _repair_status["log"].append(msg)
            logger.info("[Repair/retry] %s", msg)

    threading.Thread(target=_run, daemon=True).start()
    return {"status": "started", **_repair_status}


@app.get("/api/banks/top")
def get_top_banks(
    n: int    = Query(50, ge=1, le=200, description="Количество банков"),
    date: str = Query(None, description="Дата YYYY-MM-DD"),
    metric: str = Query("assets", description="Показатель для сортировки"),
):
    """ТОП-N банков по выбранному показателю."""
    all_metrics = cbr.get_metrics_for_date(date)
    ascending = METRICS_META.get(metric, {}).get("ascending", False)
    sorted_banks = sorted(
        all_metrics,
        key=lambda b: b.get(metric) or (float("inf") if ascending else float("-inf")),
        reverse=not ascending,
    )
    result = sorted_banks[:n]
    all_by_assets = sorted(all_metrics, key=lambda b: b.get("assets", 0), reverse=True)
    rank_map = {b["reg_num"]: i + 1 for i, b in enumerate(all_by_assets)}
    for b in result:
        b["assets_rank"] = rank_map.get(b["reg_num"])
    return result


@app.get("/api/banks/{reg_num}/metrics")
def get_bank_metrics(
    reg_num: str,
    date: str = Query(None, description="Дата YYYY-MM-DD"),
    include_closed: bool = Query(False, description="Искать среди закрытых банков"),
):
    """Все показатели конкретного банка."""
    bank = cbr.get_bank_metrics(reg_num, date, include_closed=include_closed)
    if not bank:
        raise HTTPException(404, f"Банк {reg_num} не найден")

    all_metrics = cbr.get_metrics_for_date(date)
    all_by_assets = sorted(all_metrics, key=lambda b: b.get("assets", 0), reverse=True)
    assets_rank = next(
        (i + 1 for i, b in enumerate(all_by_assets) if b["reg_num"] == reg_num),
        None,
    )
    bank["assets_rank"] = assets_rank
    bank["total_banks"] = len(all_metrics)

    prev_d = CBRParser._parse_date(date)
    if prev_d.month == 1:
        prev_str = f"{prev_d.year - 1}-12-01"
    else:
        prev_str = f"{prev_d.year}-{prev_d.month - 1:02d}-01"
    prev_bank = cbr.get_bank_metrics(reg_num, prev_str)
    bank["delta"] = {}
    bank["prev_date"] = prev_str   # Дата периода сравнения для дельт
    if prev_bank:
        for m in METRICS_META:
            bank["delta"][m] = dp.calc_delta(bank.get(m), prev_bank.get(m))

    return bank


@app.get("/api/banks/{reg_num}/timeseries")
def get_bank_timeseries(
    reg_num: str,
    metric: str  = Query("assets"),
    date_from: str = Query("2020-01-01"),
    date_to: str   = Query(None),
    period: str    = Query("month", enum=["month", "quarter", "year"]),
):
    """Временной ряд показателя банка."""
    if date_to is None:
        date_to = _default_date()
    series = cbr.get_time_series(reg_num, metric, date_from, date_to, period)
    return {"reg_num": reg_num, "metric": metric, "period": period, "data": series}


@app.get("/api/compare")
def compare_banks(
    banks: str   = Query(None, description="Рег. номера через запятую: 2734,1481"),
    group: str   = Query(None, description="top10|top20|top30|top50|top100|all"),
    metric: str  = Query("assets"),
    date: str    = Query(None),
    date_from: str = Query("2020-01-01"),
    date_to: str   = Query(None),
    period: str    = Query("year", enum=["month", "quarter", "year"]),
    district: str  = Query(None, description="Федеральные округа через запятую: ЦФО,ПФО"),
    agg_mode: str  = Query("sum", description="Агрегация: sum|mean"),
):
    """Сравнение банков/групп."""
    if date_to is None:
        date_to = _default_date()

    result: dict = {}
    result["primsocbank"] = cbr.get_time_series(
        PRIMSOCBANK_REG, metric, date_from, date_to, period
    )

    groups_to_load: list[str] = []
    if group:
        # group может быть "top20" или "top20,top30,top100" — разбиваем
        groups_to_load = [g.strip() for g in group.split(",") if g.strip()]
    if not banks and not group:
        groups_to_load = ["top10", "top50", "all"]

    top_ns = [_group_n(g) for g in groups_to_load if _group_n(g) is not None]
    has_all = any(g == "all" for g in groups_to_load)

    all_top_ns = list(filter(None, top_ns))
    if all_top_ns or has_all:
        group_series = cbr.get_group_timeseries(
            metric, date_from, date_to,
            top_ns=all_top_ns if all_top_ns else [10, 50],
            period=period,
            agg_mode=agg_mode,
        )
        for g in groups_to_load:
            key = g if g == "all" else f"top{_group_n(g)}"
            if key in group_series:
                result[g] = group_series[key]

    if banks:
        for reg in banks.split(","):
            reg = reg.strip()
            if reg and reg != PRIMSOCBANK_REG:
                result[f"bank_{reg}"] = cbr.get_time_series(
                    reg, metric, date_from, date_to, period
                )

    response: dict = {
        "metric":    metric,
        "period":    period,
        "date_from": date_from,
        "date_to":   date_to,
        "series":    result,
    }

    if district:
        districts = [d.strip() for d in district.split(",") if d.strip()]
        if districts:
            district_series = cbr.get_district_timeseries(
                districts, metric, date_from, date_to, period, agg_mode=agg_mode
            )
            response["districts"] = district_series
        else:
            response["districts"] = {}

    return response


@app.get("/api/groups/averages")
def get_group_averages(
    metric: str  = Query("assets"),
    date: str    = Query(None),
    groups: str  = Query("top10,top50,all"),
):
    """Средние значения по группам банков на заданную дату."""
    all_metrics = cbr.get_metrics_for_date(date)
    sorted_by_assets = sorted(all_metrics, key=lambda b: b.get("assets", 0), reverse=True)

    result = {}
    for g in groups.split(","):
        g = g.strip()
        n = _group_n(g)
        subset = sorted_by_assets if n is None else sorted_by_assets[:n]
        agg = dp.aggregate_group(subset, metric)
        result[g] = agg

    psb = next((b for b in all_metrics if b["reg_num"] == PRIMSOCBANK_REG), None)
    result["primsocbank"] = {"mean": psb.get(metric) if psb else None, "count": 1}

    return {"metric": metric, "date": date or _default_date(), "groups": result}


@app.get("/api/radar/{reg_num}")
def get_radar_data(
    reg_num: str,
    date: str = Query(None),
    compare_group: str = Query("top10"),
):
    """Данные для радар-диаграммы: банк vs группа."""
    all_metrics = cbr.get_metrics_for_date(date)
    bank = next((b for b in all_metrics if b["reg_num"] == reg_num), None)
    if not bank:
        raise HTTPException(404, f"Банк {reg_num} не найден")

    sorted_by_assets = sorted(all_metrics, key=lambda b: b.get("assets", 0), reverse=True)
    n = _group_n(compare_group)
    group_subset = sorted_by_assets if n is None else sorted_by_assets[:n]

    radar_metrics = ["n1", "n2", "n3", "roe", "nim", "npl"]
    group_stats = {m: dp.get_group_stats(group_subset, m) for m in radar_metrics}

    bank_scores  = dp.normalize_for_radar(bank, group_stats)
    group_avgs   = {m: group_stats[m].get("mean", 0) for m in radar_metrics}
    group_scores = dp.normalize_for_radar(
        {m: group_stats[m].get("mean", 0) for m in radar_metrics},
        group_stats,
    )

    return {
        "metrics":      radar_metrics,
        "bank":         bank_scores,
        "bank_raw":     {m: bank.get(m) for m in radar_metrics},
        "group_label":  compare_group,
        "group_scores": group_scores,
        "group_raw":    group_avgs,
    }


@app.get("/api/rankings")
def get_rankings(
    metric: str  = Query("assets"),
    date: str    = Query(None),
    group: str   = Query("all"),
    limit: int   = Query(100, ge=1, le=500),
):
    """Рейтинговая таблица банков."""
    all_metrics = cbr.get_metrics_for_date(date)
    sorted_by_assets = sorted(all_metrics, key=lambda b: b.get("assets", 0), reverse=True)

    n = _group_n(group)
    subset = sorted_by_assets if n is None else sorted_by_assets[:n]

    ascending = METRICS_META.get(metric, {}).get("ascending", False)
    ranked = sorted(
        subset,
        key=lambda b: b.get(metric) or (float("inf") if ascending else float("-inf")),
        reverse=not ascending,
    )[:limit]

    ref_d = CBRParser._parse_date(date)
    if ref_d.month == 1:
        prev_str = f"{ref_d.year - 1}-12-01"
    else:
        prev_str = f"{ref_d.year}-{ref_d.month - 1:02d}-01"
    prev_all = cbr.get_metrics_for_date(prev_str)
    prev_map  = {b["reg_num"]: b for b in prev_all}

    result = []
    for i, bank in enumerate(ranked):
        prev = prev_map.get(bank["reg_num"])
        delta = dp.calc_delta(bank.get(metric), prev.get(metric) if prev else None)
        result.append({
            "rank":    i + 1,
            "reg_num": bank["reg_num"],
            "name":    bank.get("short", bank.get("name", "")),
            "region":  bank.get("region", ""),
            "value":   bank.get(metric),
            "delta":   delta,
            "is_primsocbank": bank["reg_num"] == PRIMSOCBANK_REG,
        })
    return {"metric": metric, "date": date or _default_date(), "banks": result}


@app.get("/api/banks/{reg_num}/form101")
def get_bank_form101(
    reg_num: str,
    dt:  str = Query(None, description="Дата YYYY-MM-DD"),
    fmt: str = Query("json", enum=["json", "csv"], description="Формат ответа"),
):
    """Выгрузка сырых данных Формы 101 банка — все агрегированные счета (pln='А').

    В демо-режиме возвращает пустой список (ЦБ РФ недоступен).
    Формат CSV: UTF-8-BOM (совместим с Excel).
    """
    import io, csv as csv_mod
    from fastapi.responses import StreamingResponse

    ref_date = CBRParser._parse_date(dt)
    dt_str   = ref_date.strftime("%Y-%m-%dT00:00:00")
    rows     = cbr.get_raw_form101(reg_num, dt_str)

    if fmt == "csv":
        output = io.StringIO()
        writer = csv_mod.DictWriter(
            output,
            fieldnames=["numsc", "ap_label", "vitg_thr", "vitg_bln"],
            extrasaction="ignore",
        )
        writer.writerow({
            "numsc":    "Код счёта",
            "ap_label": "Дебет/Кредит",
            "vitg_thr": "Значение, тыс. руб.",
            "vitg_bln": "Значение, млрд. руб.",
        })
        writer.writerows(rows)
        filename = f"form101_{reg_num}_{ref_date.strftime('%Y%m%d')}.csv"
        return StreamingResponse(
            io.BytesIO(output.getvalue().encode("utf-8-sig")),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    return {
        "reg_num":   reg_num,
        "date":      ref_date.strftime("%Y-%m-%d"),
        "demo_mode": cbr.is_demo_mode,
        "count":     len(rows),
        "columns":   [
            {"key": "numsc",    "label": "Код счёта"},
            {"key": "ap_label", "label": "Дебет/Кредит"},
            {"key": "vitg_thr", "label": "Значение, тыс. руб."},
            {"key": "vitg_bln", "label": "Значение, млрд. руб."},
        ],
        "rows": rows,
    }


@app.get("/api/banks/{reg_num}/soap-raw")
def get_soap_raw(
    reg_num: str,
    dt: str = Query(None, description="Дата YYYY-MM-DD"),
):
    """Сырые данные SOAP Ф101: все коды с дебетом/кредитом и расшифровка формул.

    Показывает ВСЕ коды Ф101 из SOAP ЦБ и как из них формируются итоговые метрики.
    Используйте для отладки и сверки с сайтом ЦБ.
    """
    if dt is None:
        dt = _default_date()

    # Получаем метрики банка (включая raw_codes в _f101)
    bank = cbr.get_bank_metrics(reg_num, dt)
    if not bank:
        raise HTTPException(404, f"Банк {reg_num} не найден")

    raw_codes = bank.get("_f101", {})

    # Названия счетов плана 809-П
    ACCOUNT_NAMES = {
        "ITGAP": "ИТОГО по активам (и пассивам)",
        "20.0": "Касса, денежные средства",
        "301": "Корреспондентские счета в ЦБ (НОСТРО)",
        "302": "Обязательные резервы в ЦБ (ФОР)",
        "305": "Расчёты с филиалами",
        "310": "Кредиты от Банка России",
        "312": "МБК привлечённые от резидентов",
        "313": "МБК привлечённые от нерезидентов",
        "319": "Депозиты в Банке России",
        "32.1": "МБК выданные резидентам (агр. 320+321)",
        "32.2": "МБК выданные нерезидентам (агр. 322+323)",
        "324": "Прочие размещённые МБК",
        "325": "Резервы по МБК",
        "329": "Прочие МБК",
        "42.1": "Депозиты ЮЛ (агр. 421+425)",
        "42.2": "Вклады ФЛ (агр. 423+426)",
        "43.1": "Привлечённые от НФО (агр. 438+440)",
        "45.0": "Кредиты ЮЛ (агр. 452+456)",
        "45.1": "РВПС по кредитам ЮЛ (агр. кредит)",
        "45.2": "Кредиты ФЛ (агр. 455+457)",
        "441": "Кредиты фед. бюджету",
        "442": "Кредиты субъектам РФ",
        "443": "Кредиты местным бюджетам",
        "444": "Кредиты гос. внебюджетным фондам",
        "445": "Кредиты гос. фин. организациям",
        "446": "Кредиты коммерч. организациям",
        "447": "Кредиты НКО",
        "448": "Кредиты ИП",
        "449": "РВПС по кредитам гл. 44",
        "450": "Кредиты финансовым организациям",
        "451": "Кредиты кредитным организациям",
        "452": "Кредиты ЮЛ-резидентам",
        "453": "Кредиты финансовым ЮЛ",
        "454": "Кредиты ИП (балансовый)",
        "455": "Кредиты ФЛ-резидентам",
        "456": "Кредиты ЮЛ-нерезидентам",
        "457": "Кредиты ФЛ-нерезидентам",
        "458": "Просроченная задолженность",
        "459": "РВПС по кредитам гл. 45",
        "460": "Проектное финансирование",
        "461": "Синдицированные кредиты",
        "470": "Прочие кредиты ЮЛ",
        "474": "Расчёты (активно-пассивный)",
        "477": "Требования по прочим операциям",
        "478": "Приобретённые права требования",
        "479": "РВПС по прочим (гл. 47)",
        "501": "Долговые ЦБ РФ (по справ. стоимости)",
        "502": "Долговые ЦБ РФ (через ОПУ)",
        "503": "Долговые ЦБ (по аморт. стоимости)",
        "504": "Долговые ЦБ иностранных (по справ.)",
        "505": "Долговые ЦБ иностранных (через ОПУ)",
        "506": "Долевые ЦБ (по справ. стоимости)",
        "507": "Долевые ЦБ (через ОПУ)",
        "512": "Векселя резидентов (по справ.)",
        "513": "Векселя резидентов (через ОПУ)",
        "515": "Векселя нерезидентов",
        "520": "Выпущенные облигации",
        "521": "Выпущенные ЮЛ-депозитные сертификаты",
        "522": "Сберегательные сертификаты / облигации ФЛ",
        "523": "Выпущенные векселя",
        "526": "Обязательства по аккредитивам",
        "60.0": "Расчёты с дебиторами/кредиторами",
        "601": "Участие в дочерних/зависимых АО",
        "604": "Здания и сооружения",
        "608": "Арендованное имущество",
        "609": "Нематериальные активы",
        "610": "Капитальные вложения",
        "617": "Средства обезличенных метал. счетов",
        "619": "Оборудование",
        "620": "Расходы будущих периодов",
        "621": "Прочие ОС",
        "706": "Финансовый результат (прибыль/убыток)",
        "102": "Уставный капитал",
        "106": "Добавочный капитал",
        "107": "Резервный фонд",
        "108": "Нераспределённая прибыль",
        "401": "Средства фед. бюджета",
        "403": "Прочие средства бюджетов",
        "405": "Расчётные счета ЮЛ (гос.)",
        "406": "Расчётные счета ЮЛ (негос.)",
        "407": "Счета НКО",
        "408": "Текущие счета ФЛ (40817+40820)",
        "409": "Средства в расчётах",
        "420": "Депозиты гос. организаций",
        "422": "Депозиты НКО / госпредприятий",
    }

    # Формируем таблицу: код, название, дебет, кредит
    codes_table = []
    seen_codes = set()
    for key, val in sorted(raw_codes.items()):
        parts = key.split(":")
        code = parts[0]
        ap = parts[1] if len(parts) > 1 else "?"
        if code not in seen_codes:
            seen_codes.add(code)
        codes_table.append({
            "code": code,
            "ap": "дебет" if ap == "1" else "кредит",
            "name": ACCOUNT_NAMES.get(code, ""),
            "value_bln": val,
            "value_thr": round(val * 1_000_000),
        })

    # Расшифровка формул: как из кодов получаются метрики
    formulas = {
        "assets (Банки.ру)": {
            "formula": "c1_liquid + c2_mbk + c3_securities + c4_loans_fl + c5_loans_yl + c5_478 + c6_fixed + c7_other",
            "components": {
                "c1_liquid": "g1(20.0) + g1(301) + g1(302)",
                "c2_mbk": "g1(319) + g1(32.1) + g1(32.2) + g1(324) + g1(329)",
                "c3_securities": "net(501) + net(502) + net(504) + net(505) + net(506) + net(507) + net(512) + net(513) + net(515)",
                "c4_loans_fl": "net(45.2)",
                "c5_loans_yl": "g1(45.0) + g1(451) + g1(453) + g1(454) + g1(441..448) + g1(450) + g1(460..470)",
                "c5_478": "g1(478)",
                "c6_fixed": "net(604) + net(608) + net(609) + net(610) + net(619) + net(621)",
                "c7_other": "g1(305) + g1(325) + g1(401) + g1(403) + g1(409) + net(474) + net(477) + g1(526) + g1(60.0) + g1(617) + net(620)",
            },
            "result": bank.get("assets"),
        },
        "loans (Банки.ру)": {
            "formula": "c4_loans_fl + c5_loans_yl + c5_478",
            "result": bank.get("loans"),
        },
        "loans_fl (Банки.ру)": {
            "formula": "net(45.2) = max(g1(45.2) - g2(45.2), 0)",
            "result": bank.get("loans_fl"),
        },
        "loans_yl (Банки.ру)": {
            "formula": "g1(45.0) + g1(451) + g1(453) + g1(454) + g1(441..448) + g1(450) + g1(460..470)",
            "result": bank.get("loans_yl"),
        },
        "deposits_fl (Банки.ру)": {
            "formula": "g2(42.2) + g2(522)",
            "result": bank.get("deposits_fl"),
        },
        "deposits_yl (Банки.ру)": {
            "formula": "g2(401..407) + g2(410..420) + g2(422) + g2(42.1) + g2(427..437) + g2(43.1) + g2(439) + g2(521)",
            "result": bank.get("deposits_yl"),
        },
        "profit": {
            "formula": "g2(706) - g1(706)",
            "result": bank.get("profit"),
        },
    }

    return {
        "reg_num": reg_num,
        "bank_name": bank.get("name", ""),
        "date": dt,
        "total_codes": len(codes_table),
        "note": "g1() = дебет (актив), g2() = кредит (пассив), net() = max(g1-g2, 0), bln = млрд руб",
        "codes": codes_table,
        "formulas": formulas,
        "metrics_summary": {
            "assets": bank.get("assets"),
            "assets_cbr": bank.get("assets_cbr"),
            "loans": bank.get("loans"),
            "loans_fl": bank.get("loans_fl"),
            "loans_yl": bank.get("loans_yl"),
            "deposits_fl": bank.get("deposits_fl"),
            "deposits_yl": bank.get("deposits_yl"),
            "capital": bank.get("capital"),
            "profit": bank.get("profit"),
            "securities": bank.get("securities"),
            "liquid_assets": bank.get("liquid_assets"),
        },
    }


@app.get("/api/banks/{reg_num}/f101-timeseries")
def get_f101_code_timeseries(
    reg_num: str,
    codes: str = Query(..., description="Коды через запятую: 45.2:1,455:1,458:1"),
    date_from: str = Query("2020-01-01"),
    date_to:   str = Query(None),
    period:    str = Query("year", enum=["month", "quarter", "year"]),
):
    """Временной ряд по отдельным кодам Ф101 — для пересчёта показателя в UI.

    Использует быстрый метод get_bank_f101_codes, который:
    1) Ищет _f101 в кэше метрик (мгновенно)
    2) При отсутствии — делает прямой SOAP-запрос только для одного банка (~2 сек)
    """
    if date_to is None:
        date_to = _default_date()
    requested = [c.strip() for c in codes.split(',') if c.strip()]
    d_from = CBRParser._parse_date(date_from)
    d_to   = CBRParser._parse_date(date_to)
    dates  = CBRParser._generate_date_range(d_from, d_to, period)

    series: dict[str, list] = {c: [] for c in requested}

    for d in dates:
        date_str = d.strftime("%Y-%m-%d")
        f101_codes = cbr.get_bank_f101_codes(reg_num, date_str)
        if not f101_codes:
            continue
        label = d.strftime("%Y-%m")
        for ck in requested:
            val = f101_codes.get(ck)
            if val is not None:
                series[ck].append({"date": label, "value": val})

    return {"reg_num": reg_num, "period": period, "series": series}


@app.get("/api/compare/f101-timeseries")
def get_group_f101_timeseries(
    codes:     str = Query(...,    description="Коды через запятую: 45.2:1,455:1"),
    group:     str = Query(None,   description="top10,top50,all"),
    district:  str = Query(None,   description="ЦФО,СЗФО,..."),
    date_from: str = Query("2020-01-01"),
    date_to:   str = Query(None),
    period:    str = Query("year", enum=["month", "quarter", "year"]),
    agg_mode:  str = Query("mean", enum=["mean", "sum"]),
):
    """Агрегированный временной ряд по кодам Ф101 для группы банков.
    Читает _f101 из файлового кэша метрик — нет дополнительных SOAP-вызовов.
    Возвращает {series: {"45.2:1": [{date, value}, ...]}}
    """
    if date_to is None:
        date_to = _default_date()
    requested = [c.strip() for c in codes.split(',') if c.strip()]
    d_from = CBRParser._parse_date(date_from)
    d_to   = CBRParser._parse_date(date_to)
    dates  = CBRParser._generate_date_range(d_from, d_to, period)

    # Парсим запрошенные группы
    groups_req  = [g.strip() for g in group.split(',') if g.strip()]   if group    else []
    dist_req    = [d.strip() for d in district.split(',') if d.strip()] if district else []

    # Читаем список банков из all_metrics для определения топов/регионов
    all_metrics = cbr.all_metrics
    top_groups: dict[str, list[str]] = {}
    for g in groups_req:
        if g == 'all':
            top_groups['all'] = [b['reg_num'] for b in all_metrics if b.get('reg_num')]
        else:
            n = int(g.replace('top', '')) if g.startswith('top') else 0
            if n:
                sorted_banks = sorted(
                    [b for b in all_metrics if b.get('assets') and not b.get('closed')],
                    key=lambda b: b.get('assets', 0), reverse=True
                )
                top_groups[g] = [b['reg_num'] for b in sorted_banks[:n]]

    dist_groups: dict[str, list[str]] = {}
    for d in dist_req:
        dist_groups[d] = [b['reg_num'] for b in all_metrics if b.get('district') == d and b.get('reg_num')]

    # Для каждой даты — читаем кэш и агрегируем F101 коды
    # series_data: group_key -> code_key -> [{date, value}]
    series_out: dict[str, dict[str, list]] = {}
    all_group_keys = list(top_groups.keys()) + list(dist_groups.keys())
    for gk in all_group_keys:
        series_out[gk] = {ck: [] for ck in requested}

    for d in dates:
        label    = d.strftime("%Y-%m")
        date_str = d.strftime("%Y-%m-%d")
        # Ищем файл кэша для этой даты
        year  = d.year
        if period == "month":
            cache_file = DATA_DIR / "metrics" / f"metrics_v2_{year}_{d.month:02d}.json"
        else:
            cache_file = DATA_DIR / "metrics" / f"metrics_v2_{year}_01.json"
        if not cache_file.exists():
            continue
        import json as _json
        with open(cache_file, encoding="utf-8") as f:
            cache_data = _json.load(f)
        # Строим lookup reg_num -> _f101
        f101_lookup: dict[str, dict] = {}
        for entry in cache_data:
            rn = entry.get("reg_num")
            if rn and "_f101" in entry:
                f101_lookup[rn] = entry["_f101"]

        for gk, reg_list in list(top_groups.items()) + list(dist_groups.items()):
            for ck in requested:
                vals = []
                for rn in reg_list:
                    raw = f101_lookup.get(rn, {}).get(ck)
                    if raw is not None:
                        vals.append(raw)
                if vals:
                    agg_val = sum(vals) if agg_mode == "sum" else sum(vals) / len(vals)
                    series_out[gk][ck].append({"date": label, "value": round(agg_val, 3)})

    return {"series": series_out}


@app.get("/api/moex/keyrate")
def get_key_rate(
    date_from: str = Query("2020-01-01"),
    date_to: str   = Query(None),
):
    """История ключевой ставки ЦБ (через MOEX ISS)."""
    if date_to is None:
        date_to = date.today().strftime("%Y-%m-%d")
    data = moex.get_cbr_key_rate_history(date_from, date_to)
    return {"data": data}


@app.get("/api/cbr/keyrate-history")
async def get_keyrate_history(
    date_from: str = Query("2013-09-01"),
    date_to:   str = Query(None),
):
    """
    Полная история ключевой ставки ЦБ РФ через SOAP DailyInfoWebServ/KeyRate.
    Возвращает [{date, value}] в хронологическом порядке.
    """
    import datetime as dt_mod
    import httpx
    from lxml import etree

    if date_to is None:
        date_to = dt_mod.date.today().isoformat()

    DAILY_INFO_URL = "https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx"
    body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
               xmlns:CB="http://web.cbr.ru/">
  <soap:Body>
    <CB:KeyRate>
      <CB:fromDate>{date_from}</CB:fromDate>
      <CB:ToDate>{date_to}</CB:ToDate>
    </CB:KeyRate>
  </soap:Body>
</soap:Envelope>"""
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.post(
                DAILY_INFO_URL,
                content=body.encode("utf-8"),
                headers={"Content-Type": "text/xml; charset=utf-8",
                         "SOAPAction": '"http://web.cbr.ru/KeyRate"'},
            )
            r.raise_for_status()
            root = etree.fromstring(r.content)

        rows = root.findall(".//{http://web.cbr.ru/}KR") or root.findall(".//KR")
        result = []
        for row in rows:
            def _t(tag, el=row):
                n = el.find(f"{{http://web.cbr.ru/}}{tag}") or el.find(tag)
                return n.text.strip() if n is not None and n.text else None
            dt_str   = _t("DT")
            rate_str = _t("Rate")
            if dt_str and rate_str:
                result.append({"date": dt_str[:10], "value": float(rate_str.replace(",", "."))})
        return {"data": result, "count": len(result)}
    except Exception as e:
        logger.warning("keyrate-history error: %s", e)
        raise HTTPException(502, f"Ошибка получения истории ключевой ставки: {e}")


@app.get("/api/cbr/currency-history")
async def get_currency_history(
    code: str = Query("USD", description="ISO код валюты: USD, EUR, CNY, GBP, BYR"),
    date_from: str = Query("2020-01-01"),
    date_to:   str = Query(None),
):
    """
    История курса валюты ЦБ РФ через SOAP GetCursDynamic.
    code: USD | EUR | CNY | GBP | BYR
    """
    import datetime as dt_mod
    import httpx
    from lxml import etree

    if date_to is None:
        date_to = dt_mod.date.today().isoformat()

    # Коды валют по классификатору ЦБ (VAL_NM_RQ)
    CODE_MAP = {
        "USD": "R01235",
        "EUR": "R01239",
        "CNY": "R01375",
        "GBP": "R01035",
        "BYR": "R01090",
    }
    val_code = CODE_MAP.get(code.upper())
    if not val_code:
        raise HTTPException(400, f"Неизвестный код валюты: {code}. Доступны: {', '.join(CODE_MAP)}")

    DAILY_INFO_URL = "https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx"
    body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
               xmlns:CB="http://web.cbr.ru/">
  <soap:Body>
    <CB:GetCursDynamic>
      <CB:FromDate>{date_from}</CB:FromDate>
      <CB:ToDate>{date_to}</CB:ToDate>
      <CB:ValutaCode>{val_code}</CB:ValutaCode>
    </CB:GetCursDynamic>
  </soap:Body>
</soap:Envelope>"""
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.post(
                DAILY_INFO_URL,
                content=body.encode("utf-8"),
                headers={"Content-Type": "text/xml; charset=utf-8",
                         "SOAPAction": '"http://web.cbr.ru/GetCursDynamic"'},
            )
            r.raise_for_status()
            root = etree.fromstring(r.content)

        items = (root.findall(".//{http://web.cbr.ru/}ValuteCursDynamic")
                 or root.findall(".//ValuteCursDynamic"))
        result = []
        for item in items:
            def _t(tag, el=item):
                n = el.find(f"{{http://web.cbr.ru/}}{tag}") or el.find(tag)
                return n.text.strip() if n is not None and n.text else None
            dt_str   = _t("CursDate")
            curs_str = _t("Vcurs")
            nom_str  = _t("Vnom")
            if dt_str and curs_str:
                nom = int(nom_str or "1")
                result.append({
                    "date":  dt_str[:10],
                    "value": round(float(curs_str.replace(",", ".")) / nom, 4),
                })
        return {"data": result, "code": code, "count": len(result)}
    except HTTPException:
        raise
    except Exception as e:
        logger.warning("currency-history error: %s", e)
        raise HTTPException(502, f"Ошибка получения истории курса {code}: {e}")


@app.get("/api/cbr/metals-history")
async def get_metals_history(
    metal: str = Query("1", description="Код металла: 1=Au, 2=Ag, 3=Pt, 4=Pd"),
    date_from: str = Query("2020-01-01"),
    date_to:   str = Query(None),
):
    """История котировок драгоценных металлов ЦБ РФ через SOAP DragMetDynamic."""
    import datetime as dt_mod
    import httpx
    from lxml import etree

    if date_to is None:
        date_to = dt_mod.date.today().isoformat()

    METALS_MAP = {"1": "Au Золото", "2": "Ag Серебро", "3": "Pt Платина", "4": "Pd Палладий"}
    if metal not in METALS_MAP:
        raise HTTPException(400, f"Неизвестный код металла: {metal}. Доступны: 1, 2, 3, 4")

    DAILY_INFO_URL = "https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx"
    CACHE_METALS = Path(__file__).parent / "data" / "cache"
    CACHE_METALS.mkdir(parents=True, exist_ok=True)

    async def fetch_metal_chunk(chunk_from: str, chunk_to: str) -> list:
        """Загружает один чанк (≤1 год) и возвращает список {date, value}."""
        # Кэш на диске — файл на каждый год/металл
        year = chunk_from[:4]
        cache_file = CACHE_METALS / f"metals_{metal}_{year}.json"
        today_str  = dt_mod.date.today().isoformat()
        # Используем кэш только для прошлых лет (текущий год не кэшируем)
        if cache_file.exists() and year < today_str[:4]:
            try:
                with open(cache_file, encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass

        body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
               xmlns:CB="http://web.cbr.ru/">
  <soap:Body>
    <CB:DragMetDynamic>
      <CB:fromDate>{chunk_from}</CB:fromDate>
      <CB:ToDate>{chunk_to}</CB:ToDate>
    </CB:DragMetDynamic>
  </soap:Body>
</soap:Envelope>"""
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                r = await client.post(
                    DAILY_INFO_URL,
                    content=body.encode("utf-8"),
                    headers={"Content-Type": "text/xml; charset=utf-8",
                             "SOAPAction": '"http://web.cbr.ru/DragMetDynamic"'},
                )
                r.raise_for_status()
                root = etree.fromstring(r.content)
        except Exception as e:
            logger.warning("metals chunk %s-%s error: %s", chunk_from, chunk_to, e)
            return []

        items = (root.findall(".//{http://web.cbr.ru/}DrgMet")
                 or root.findall(".//DrgMet")
                 or root.findall(".//{http://web.cbr.ru/}DrgMetall")
                 or root.findall(".//DrgMetall"))
        chunk_result = []
        for item in items:
            def _t(tag, el=item):
                n = el.find(f"{{http://web.cbr.ru/}}{tag}") or el.find(tag)
                return n.text.strip() if n is not None and n.text else None
            # ЦБ возвращает: CodMet, DateMet, price (не cdmet/data/cena!)
            code_val = _t("CodMet") or _t("cdmet")
            if code_val != metal:
                continue
            dt_str   = _t("DateMet") or _t("data")
            cena_str = _t("price") or _t("cena")
            if dt_str and cena_str:
                try:
                    chunk_result.append({
                        "date":  dt_str[:10],
                        "value": round(float(cena_str.replace(",", ".")), 2),
                    })
                except Exception:
                    pass

        # Дедупликация по дате
        seen = {}
        for p in chunk_result:
            seen[p["date"]] = p["value"]
        chunk_result = [{"date": d, "value": v} for d, v in sorted(seen.items())]

        # Сохраняем кэш для прошлых лет
        if chunk_result and year < today_str[:4]:
            try:
                with open(cache_file, "w", encoding="utf-8") as f:
                    json.dump(chunk_result, f)
            except Exception:
                pass
        return chunk_result

    # Разбиваем диапазон на чанки по календарным годам
    d_from = dt_mod.date.fromisoformat(date_from)
    d_to   = dt_mod.date.fromisoformat(date_to)
    chunks = []
    cur = d_from
    while cur <= d_to:
        chunk_end = dt_mod.date(cur.year, 12, 31)   # конец текущего года
        chunk_end = min(chunk_end, d_to)
        chunks.append((cur.isoformat(), chunk_end.isoformat()))
        cur = chunk_end + dt_mod.timedelta(days=1)

    # Загружаем все чанки параллельно
    import asyncio
    all_data = await asyncio.gather(*[fetch_metal_chunk(c[0], c[1]) for c in chunks])

    # Объединяем и дедуплицируем
    merged = {}
    for chunk_data in all_data:
        for p in chunk_data:
            merged[p["date"]] = p["value"]
    result = [{"date": d, "value": v} for d, v in sorted(merged.items())]

    if not result:
        raise HTTPException(502, f"Нет данных по металлу {metal} за период {date_from}..{date_to}")

    return {"data": result, "metal": metal, "name": METALS_MAP[metal], "count": len(result)}


@app.get("/api/moex/bonds")
def get_bonds(query: str = Query("ПРИМСОЦБАНК")):
    """Облигации банка на MOEX."""
    return moex.get_bank_bonds(query)


# ─── MOEX Биржа ──────────────────────────────────────────────────────

@app.get("/api/moex/indices")
def get_moex_indices():
    """Текущие значения основных индексов MOEX."""
    return moex.get_indices_snapshot()


@app.get("/api/moex/index-history/{secid}")
def get_moex_index_history(
    secid: str,
    date_from: str = Query("2024-01-01"),
    date_to: str = Query(None),
):
    """История индекса MOEX."""
    data = moex.get_index_history(secid, date_from, date_to)
    return {"secid": secid, "data": data, "count": len(data)}


@app.get("/api/moex/currencies")
def get_moex_currencies():
    """Текущие биржевые курсы валют на MOEX."""
    return moex.get_currency_rates()


@app.get("/api/moex/stocks")
def get_moex_stocks(limit: int = Query(30, ge=1, le=100)):
    """Топ акций на MOEX по обороту."""
    return moex.get_top_stocks(limit)


@app.get("/api/moex/stock-history/{secid}")
def get_moex_stock_history(
    secid: str,
    date_from: str = Query("2024-01-01"),
    date_to: str = Query(None),
):
    """История котировок акции на MOEX."""
    data = moex.get_stock_history(secid, date_from, date_to)
    return {"secid": secid, "data": data, "count": len(data)}


@app.get("/api/moex/metals-history")
def get_moex_metals_history(
    date_from: str = Query("2023-01-01"),
    date_to: str   = Query(None),
):
    """Учётные цены ЦБ на драгоценные металлы (Au/Ag/Pt/Pd, руб./грамм)."""
    data = moex.get_metals_history(date_from, date_to)
    return {"data": data, "count": len(data), "metals": moex.METALS}


@app.get("/api/moex/fx-history/{currency}")
def get_moex_fx_history(
    currency: str,
    date_from: str = Query("2023-01-01"),
    date_to: str   = Query(None),
):
    """История курса валюты: CNY (MOEX), USD/EUR (ЦБ РФ)."""
    data = moex.get_fx_history(currency, date_from, date_to)
    return {"currency": currency.upper(), "data": data, "count": len(data)}


@app.get("/api/moex/trading-summary")
def get_moex_trading_summary(
    instrument: str = Query("CNY", description="CNY, USD, EUR, Au, Ag, Pt, Pd"),
    trade_date: str = Query(None, description="YYYY-MM-DD, по умолчанию — вчера"),
):
    """Итоги торгов за день по инструменту."""
    summary = moex.get_trading_summary(instrument, trade_date)
    if summary is None:
        raise HTTPException(status_code=404, detail="Нет данных за указанную дату")
    return summary


@app.get("/api/cbr/market-data")
async def get_market_data():
    """
    Ключевая ставка ЦБ РФ + курсы валют + металлы.
    Использует SOAP сервис ЦБ РФ DailyInfoWebServ.
    """
    import datetime
    from lxml import etree
    import httpx

    DAILY_INFO_URL = "https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx"
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    month_ago = today - datetime.timedelta(days=45)

    result = {
        "key_rate": None,
        "currencies": [],
        "metals": [],
        "as_of": str(today),
        "demo_mode": False,
    }

    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": "",
    }

    async def soap_call(action: str, body: str):
        envelope = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
               xmlns:xsi="http://www.w3.org/1999/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/1999/XMLSchema"
               xmlns:CB="http://web.cbr.ru/">
  <soap:Body>{body}</soap:Body>
</soap:Envelope>"""
        h = dict(headers)
        h["SOAPAction"] = f'"http://web.cbr.ru/{action}"'
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.post(DAILY_INFO_URL, content=envelope.encode("utf-8"), headers=h)
            r.raise_for_status()
            return etree.fromstring(r.content)

    # ── Ключевая ставка ────────────────────────────────────────────────────────
    try:
        body = f"""<CB:KeyRate>
  <CB:fromDate>{month_ago.isoformat()}</CB:fromDate>
  <CB:ToDate>{today.isoformat()}</CB:ToDate>
</CB:KeyRate>"""
        root = await soap_call("KeyRate", body)
        # ЦБ возвращает данные в diffgram без namespace — пробуем все варианты
        rows = (root.findall(".//{http://web.cbr.ru/}KR")
                or root.findall(".//KR"))
        if rows:
            # Поля тоже могут быть без namespace
            def _find_text(el, tag):
                node = el.find(f"{{http://web.cbr.ru/}}{tag}") or el.find(tag)
                return node.text.strip() if node is not None and node.text else None

            # Собираем все записи и находим самую свежую по дате
            parsed_rows = []
            for r2 in rows:
                dt2   = _find_text(r2, "DT")
                rate2 = _find_text(r2, "Rate")
                if dt2 and rate2:
                    parsed_rows.append({
                        "date":  dt2[:10],
                        "value": float(rate2.replace(",", "."))
                    })

            if parsed_rows:
                # Сортируем по дате, берём самую свежую
                parsed_rows.sort(key=lambda x: x["date"])
                latest = parsed_rows[-1]
                result["key_rate"] = {
                    "value": latest["value"],
                    "date":  latest["date"],
                }
                result["key_rate_history"] = parsed_rows
    except Exception as e:
        logger.warning("KeyRate SOAP error: %s", e)
        result["demo_mode"] = True

    # ── Курсы валют ────────────────────────────────────────────────────────────
    try:
        # Ищем актуальную дату (ЦБ может не публиковать в выходные)
        dt_use = today
        items = []
        for delta in [0, 1, 2, 3]:
            dt_try = today - datetime.timedelta(days=delta)
            body = f"""<CB:GetCursOnDate>
  <CB:On_date>{dt_try.isoformat()}</CB:On_date>
</CB:GetCursOnDate>"""
            root = await soap_call("GetCursOnDate", body)
            items = root.findall(".//{http://web.cbr.ru/}ValuteCursOnDate") or root.findall(".//ValuteCursOnDate")
            if items:
                dt_use = dt_try
                break
        # Показываем ВСЕ валюты которые передаёт ЦБ РФ (~40 штук)
        # Маппинг русских названий -> ISO-код (для удобства JS-стороны)
        NAME_TO_ISO = {
            "Австралийский доллар": "AUD", "Азербайджанский манат": "AZN",
            "Армянский драм": "AMD", "Белорусский рубль": "BYR",
            "Болгарский лев": "BGN", "Бразильский реал": "BRL",
            "Венгерский форинт": "HUF", "Гонконгский доллар": "HKD",
            "Грузинский лари": "GEL", "Датская крона": "DKK",
            "Дирхам ОАЭ": "AED", "Доллар США": "USD",
            "Египетский фунт": "EGP", "Евро": "EUR",
            "Индийская рупия": "INR", "Индонезийская рупия": "IDR",
            "Казахстанский тенге": "KZT", "Канадский доллар": "CAD",
            "Катарский риал": "QAR", "Киргизский сом": "KGS",
            "Китайский юань": "CNY", "Юань": "CNY",
            "Корейский вон": "KRW", "Кувейтский динар": "KWD",
            "Молдавский лей": "MDL", "Новозеландский доллар": "NZD",
            "Норвежская крона": "NOK", "Польский злотый": "PLN",
            "Румынский лей": "RON", "Саудовский риял": "SAR",
            "Сингапурский доллар": "SGD", "Таджикский сомони": "TJS",
            "Таиландский бат": "THB", "Туркменский манат": "TMT",
            "Турецкая лира": "TRY", "Узбекский сум": "UZS",
            "Фунт стерлингов": "GBP", "Чешская крона": "CZK",
            "Шведская крона": "SEK", "Швейцарский франк": "CHF",
            "Южноафриканский рэнд": "ZAR", "Японская иена": "JPY",
        }
        seen_codes = set()
        for cur_item in items:
            def _tc(tag, _ci=cur_item):
                el = _ci.find(f"{{http://web.cbr.ru/}}{tag}") or _ci.find(tag)
                return el.text.strip() if el is not None and el.text else ""
            name = _tc("Vname")
            iso  = NAME_TO_ISO.get(name)
            if not iso:
                # Если нет в маппинге — используем поле Vcode (буквенный код ЦБ)
                iso = _tc("VchCode").strip() or name[:3].upper()
            if iso in seen_codes:
                continue
            seen_codes.add(iso)
            try:
                nominal  = int(_tc("Vnom") or "1")
                rate_val = float(_tc("Vcurs").replace(",", ".")) / nominal
                result["currencies"].append({
                    "code": iso,
                    "name": name,
                    "rate": round(rate_val, 4),
                    "date": str(dt_use),
                })
            except Exception:
                pass
        # Сортируем: сначала популярные, потом остальные по алфавиту
        PRIORITY = ["USD", "EUR", "CNY", "GBP", "JPY", "CHF", "TRY", "BYR", "KZT", "AED"]
        result["currencies"].sort(
            key=lambda c: (PRIORITY.index(c["code"]) if c["code"] in PRIORITY else 99, c["code"])
        )
    except Exception as e:
        logger.warning("Currencies SOAP error: %s", e)

    # ── Металлы ────────────────────────────────────────────────────────────────
    METALS_MAP_SNAP = {"1": "Au Золото", "2": "Ag Серебро", "3": "Pt Платина", "4": "Pd Палладий"}
    try:
        # Прямой SOAP-вызов с увеличенным таймаутом (separate client, не через soap_call)
        metals_body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
               xmlns:CB="http://web.cbr.ru/">
  <soap:Body>
    <CB:DragMetDynamic>
      <CB:fromDate>{(today - datetime.timedelta(days=30)).isoformat()}</CB:fromDate>
      <CB:ToDate>{today.isoformat()}</CB:ToDate>
    </CB:DragMetDynamic>
  </soap:Body>
</soap:Envelope>"""
        from lxml import etree as _etree
        async with httpx.AsyncClient(timeout=30.0) as _mc:
            _mr = await _mc.post(
                DAILY_INFO_URL,
                content=metals_body.encode("utf-8"),
                headers={"Content-Type": "text/xml; charset=utf-8",
                         "SOAPAction": '"http://web.cbr.ru/DragMetDynamic"'},
            )
            _mr.raise_for_status()
            met_root = _etree.fromstring(_mr.content)
        met_items = (met_root.findall(".//{http://web.cbr.ru/}DrgMet")
                     or met_root.findall(".//DrgMet")
                     or met_root.findall(".//{http://web.cbr.ru/}DrgMetall")
                     or met_root.findall(".//DrgMetall"))
        latest = {}
        for met_item in met_items:
            def _tm(tag, _mi=met_item):
                el = _mi.find(f"{{http://web.cbr.ru/}}{tag}") or _mi.find(tag)
                return el.text.strip() if el is not None and el.text else ""
            code_m = _tm("CodMet") or _tm("cdmet")
            if code_m in METALS_MAP_SNAP:
                try:
                    dt2  = (_tm("DateMet") or _tm("data"))[:10]
                    cena = _tm("price") or _tm("cena")
                    if cena:
                        buy = float(cena.replace(",", "."))
                        if code_m not in latest or dt2 > latest[code_m]["date"]:
                            latest[code_m] = {
                                "code":  code_m,
                                "name":  METALS_MAP_SNAP[code_m],
                                "price": buy,
                                "date":  dt2,
                            }
                except Exception:
                    pass
        result["metals"] = list(latest.values())
        logger.info("Metals snapshot loaded: %d items", len(result["metals"]))
    except Exception as e:
        logger.warning("Metals SOAP error: %s", e)

    # Fallback: если SOAP вернул пустой ответ — берём последнее значение из кэша metals-history
    if not result["metals"]:
        _cache_dir = Path(__file__).parent / "data" / "cache"
        for code_f, name_f in METALS_MAP_SNAP.items():
            for yr in [str(today.year), str(today.year - 1)]:
                _cf = _cache_dir / f"metals_{code_f}_{yr}.json"
                if _cf.exists():
                    try:
                        with open(_cf, encoding="utf-8") as _f:
                            _cached = json.load(_f)
                        if _cached:
                            last_pt = _cached[-1]
                            result["metals"].append({
                                "code":  code_f,
                                "name":  name_f,
                                "price": last_pt["value"],
                                "date":  last_pt["date"],
                            })
                            break
                    except Exception:
                        pass
        if result["metals"]:
            logger.info("Metals snapshot from cache: %d items", len(result["metals"]))

    return result


@app.post("/api/cache/clear")
def clear_all_cache():
    """
    Удаляет ВСЕ файлы кэша метрик и банковского справочника.
    После этого данные будут заново запрошены у ЦБ РФ при следующем обращении.
    """
    deleted = []
    errors  = []

    metrics_dir  = Path(__file__).parent / "data" / "metrics"
    cache_dir    = Path(__file__).parent / "data" / "cache"

    # Удаляем все metrics_*.json
    for f in metrics_dir.glob("metrics_*.json"):
        try:
            f.unlink()
            deleted.append(f.name)
        except Exception as e:
            errors.append(f"{f.name}: {e}")

    # Удаляем bank_list.json (список банков)
    bank_list = cache_dir / "bank_list.json"
    if bank_list.exists():
        try:
            bank_list.unlink()
            deleted.append("cache/bank_list.json")
        except Exception as e:
            errors.append(f"bank_list.json: {e}")

    logger.info("Кэш очищен: удалено %d файлов", len(deleted))
    return {
        "status":  "ok",
        "deleted": len(deleted),
        "files":   deleted,
        "errors":  errors,
    }


@app.post("/api/refresh")
def force_refresh(date: str = Query(None)):
    """Принудительное обновление данных."""
    global _last_refresh
    try:
        d = date or _default_date()
        metrics_dir = Path(__file__).parent / "data" / "metrics"
        ref_d = CBRParser._parse_date(d)
        # Удаляем оба варианта кэша (с закрытыми и без)
        for suffix in ("", "_closed"):
            cache_file = metrics_dir / f"metrics_v2_{ref_d.year}_{ref_d.month:02d}{suffix}.json"
            if cache_file.exists():
                cache_file.unlink()

        data = cbr.get_metrics_for_date(d)
        _last_refresh = datetime.now()
        return {
            "status":        "ok",
            "updated_banks": len(data),
            "date":          d,
            "demo_mode":     cbr.is_demo_mode,
        }
    except Exception as e:
        logger.exception("Ошибка обновления данных")
        raise HTTPException(500, f"Ошибка обновления: {e}")



# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import os

    port = int(os.environ.get("PORT", 8000))

    logger.info("Запуск uvicorn на 0.0.0.0:%s ...", port)

    try:
        uvicorn.run(
            app,
            host="0.0.0.0",   # ОБЯЗАТЕЛЬНО для Railway
            port=port,
            log_level="info",
        )
    except Exception as e:
        logger.critical("Критическая ошибка запуска: %s", e, exc_info=True)
        sys.exit(1)
