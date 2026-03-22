"""
Парсер данных Московской Биржи (MOEX ISS API).
API полностью бесплатное, без авторизации.
Документация: https://iss.moex.com/iss/reference/
"""
import logging
from datetime import date, datetime, timedelta
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

BASE_URL = "https://iss.moex.com/iss"


class MOEXParser:
    def __init__(self):
        self._client = httpx.Client(timeout=15, follow_redirects=True)

    # ------------------------------------------------------------------
    # Ключевая ставка ЦБ
    # ------------------------------------------------------------------

    def get_cbr_key_rate_history(
        self,
        date_from: str = "2020-01-01",
        date_to: Optional[str] = None,
    ) -> list[dict]:
        """
        История ключевой ставки ЦБ с MOEX ISS.
        Возвращает список {"date": "YYYY-MM-DD", "rate": float}.
        """
        if date_to is None:
            date_to = date.today().strftime("%Y-%m-%d")

        url = (
            f"{BASE_URL}/statistics/engines/stock/markets/index/analytics/KEYRATE.json"
            f"?from={date_from}&till={date_to}&limit=500"
        )
        try:
            resp = self._client.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            analytics = data.get("analytics", {})
            columns = analytics.get("columns", [])
            rows = analytics.get("data", [])

            date_idx  = columns.index("tradedate")   if "tradedate"  in columns else None
            value_idx = columns.index("value")       if "value"      in columns else None

            if date_idx is None or value_idx is None:
                raise ValueError("Неожиданная структура ответа MOEX")

            result = []
            for row in rows:
                result.append({
                    "date": row[date_idx],
                    "rate": float(row[value_idx]),
                })
            return result

        except Exception as e:
            logger.warning(f"[MOEX] Не удалось получить ключевую ставку: {e}")
            return self._demo_key_rate(date_from, date_to)

    # ------------------------------------------------------------------
    # Облигации банка
    # ------------------------------------------------------------------

    def get_bank_bonds(self, query: str = "ПРИМСОЦБАНК") -> list[dict]:
        """
        Облигации банка на Московской Бирже.
        query — название банка или ISIN.
        """
        url = (
            f"{BASE_URL}/securities.json"
            f"?q={query}&engine=stock&market=bonds&is_trading=1&limit=50"
        )
        try:
            resp = self._client.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            securities = data.get("securities", {})
            columns = securities.get("columns", [])
            rows    = securities.get("data", [])

            bonds = []
            for row in rows:
                item = dict(zip(columns, row))
                bonds.append({
                    "secid":     item.get("secid", ""),
                    "name":      item.get("name", ""),
                    "isin":      item.get("isin", ""),
                    "matdate":   item.get("matdate", ""),
                    "coupon":    item.get("couponvalue", None),
                    "face":      item.get("facevalue", None),
                    "currency":  item.get("faceunit", "RUB"),
                })
            return bonds

        except Exception as e:
            logger.warning(f"[MOEX] Не удалось получить облигации: {e}")
            return self._demo_bonds()

    # ------------------------------------------------------------------
    # Акции банка (если торгуются)
    # ------------------------------------------------------------------

    def get_bank_stock(self, ticker: str) -> Optional[dict]:
        """Последняя котировка акции по тикеру."""
        url = (
            f"{BASE_URL}/engines/stock/markets/shares/securities/{ticker}.json"
            f"?limit=1"
        )
        try:
            resp = self._client.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            market_data = data.get("marketdata", {})
            columns = market_data.get("columns", [])
            rows    = market_data.get("data", [])
            if not rows:
                return None

            item = dict(zip(columns, rows[0]))
            return {
                "ticker": ticker,
                "last":   item.get("LAST"),
                "open":   item.get("OPEN"),
                "high":   item.get("HIGH"),
                "low":    item.get("LOW"),
                "volume": item.get("VOLRUR"),
                "date":   item.get("TRADEDATE"),
            }
        except Exception as e:
            logger.warning(f"[MOEX] Не удалось получить котировку {ticker}: {e}")
            return None

    # ------------------------------------------------------------------
    # Индексы MOEX (текущие значения)
    # ------------------------------------------------------------------

    # Основные индексы для отображения
    INDICES = [
        {"secid": "IMOEX",  "name": "Индекс МосБиржи",       "emoji": "📈"},
        {"secid": "RTSI",   "name": "Индекс РТС",            "emoji": "📊"},
        {"secid": "MOEXOG", "name": "Нефть и газ",            "emoji": "🛢️"},
        {"secid": "MOEXFN", "name": "Финансы",                "emoji": "🏦"},
        {"secid": "MOEXMM", "name": "Металлы и добыча",       "emoji": "⛏️"},
        {"secid": "MOEXCN", "name": "Потребительский сектор",  "emoji": "🛒"},
        {"secid": "MOEXTL", "name": "Телекоммуникации",        "emoji": "📡"},
        {"secid": "MOEXEU", "name": "Электроэнергетика",       "emoji": "⚡"},
        {"secid": "RGBITR", "name": "Гос. облигации (RGBITR)", "emoji": "📜"},
    ]

    def get_indices_snapshot(self) -> list[dict]:
        """Текущие значения основных индексов MOEX."""
        secids = ",".join(i["secid"] for i in self.INDICES)
        url = f"{BASE_URL}/engines/stock/markets/index/securities.json?securities={secids}"
        try:
            resp = self._client.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            md = data.get("marketdata", {})
            mcols = md.get("columns", [])
            mrows = md.get("data", [])

            md_map = {}
            for row in mrows:
                item = dict(zip(mcols, row))
                sid = item.get("SECID")
                val = item.get("CURRENTVALUE") or item.get("LASTVALUE")
                if sid and val:
                    md_map[sid] = {
                        "value": val,
                        "change": item.get("LASTCHANGEPRCNT") or item.get("LASTCHANGETOOPEN"),
                        "time": item.get("TIME", ""),
                        "date": item.get("TRADEDATE", ""),
                    }

            result = []
            for idx in self.INDICES:
                sid = idx["secid"]
                info = md_map.get(sid)
                if info:
                    result.append({
                        "secid": sid,
                        "name": idx["name"],
                        "emoji": idx["emoji"],
                        "value": info["value"],
                        "change_pct": info["change"],
                        "time": info["time"],
                        "date": info["date"],
                    })
            return result
        except Exception as e:
            logger.warning("[MOEX] Indices snapshot error: %s", e)
            return []

    # ------------------------------------------------------------------
    # История индекса MOEX
    # ------------------------------------------------------------------

    def get_index_history(
        self, secid: str = "IMOEX",
        date_from: str = "2020-01-01",
        date_to: Optional[str] = None,
    ) -> list[dict]:
        """История индекса MOEX (дневные данные)."""
        if date_to is None:
            date_to = date.today().strftime("%Y-%m-%d")

        all_rows = []
        start = 0
        page_size = 100

        while True:
            url = (
                f"{BASE_URL}/history/engines/stock/markets/index/securities/{secid}.json"
                f"?from={date_from}&till={date_to}&limit={page_size}&start={start}"
            )
            try:
                resp = self._client.get(url, timeout=15)
                resp.raise_for_status()
                data = resp.json()

                h = data.get("history", {})
                cols = h.get("columns", [])
                rows = h.get("data", [])

                for row in rows:
                    item = dict(zip(cols, row))
                    close_val = item.get("CLOSE")
                    if close_val and close_val > 0:
                        all_rows.append({
                            "date": item["TRADEDATE"],
                            "open": item.get("OPEN"),
                            "high": item.get("HIGH"),
                            "low": item.get("LOW"),
                            "close": close_val,
                        })

                # Пагинация
                cursor = data.get("history.cursor", {})
                ccols = cursor.get("columns", [])
                crows = cursor.get("data", [])
                if crows:
                    ci = dict(zip(ccols, crows[0]))
                    total = ci.get("TOTAL", 0)
                    if start + page_size >= total:
                        break
                    start += page_size
                else:
                    if len(rows) < page_size:
                        break
                    start += page_size

            except Exception as e:
                logger.warning("[MOEX] Index history error for %s: %s", secid, e)
                break

        return all_rows

    # ------------------------------------------------------------------
    # Текущие курсы валют на MOEX
    # ------------------------------------------------------------------

    def get_currency_rates(self) -> dict:
        """Текущие биржевые курсы валют MOEX."""
        url = f"{BASE_URL}/statistics/engines/currency/markets/selt/rates.json"
        try:
            resp = self._client.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            result = {}

            # Блок cbrf — курсы ЦБ и биржевой USD
            cbrf = data.get("cbrf", {})
            ccols = cbrf.get("columns", [])
            crows = cbrf.get("data", [])
            if crows:
                ci = dict(zip(ccols, crows[0]))
                result["usd_moex"] = {
                    "value": ci.get("USDTOM_UTS_CLOSEPRICE"),
                    "change_pct": ci.get("USDTOM_UTS_CLOSEPRICETOPREVPRCN"),
                    "date": ci.get("USDTOM_UTS_TRADEDATE"),
                }
                result["usd_cbr"] = {
                    "value": ci.get("CBRF_USD_LAST"),
                    "change_pct": ci.get("CBRF_USD_LASTCHANGEPRCNT"),
                    "date": ci.get("CBRF_USD_TRADEDATE"),
                }
                result["eur_cbr"] = {
                    "value": ci.get("CBRF_EUR_LAST"),
                    "change_pct": ci.get("CBRF_EUR_LASTCHANGEPRCNT"),
                    "date": ci.get("CBRF_EUR_TRADEDATE"),
                }
                result["volume_bln"] = ci.get("TODAY_VALTODAY")

            # Блок wap_rates — средневзвешенные курсы
            wap = data.get("wap_rates", {})
            wcols = wap.get("columns", [])
            wrows = wap.get("data", [])
            for row in wrows:
                wi = dict(zip(wcols, row))
                secid = wi.get("secid", "")
                if "CNY" in secid:
                    result["cny_moex"] = {
                        "value": wi.get("price"),
                        "change_pct": wi.get("lasttoprevprice"),
                        "date": wi.get("tradedate"),
                    }

            return result
        except Exception as e:
            logger.warning("[MOEX] Currency rates error: %s", e)
            return {}

    # ------------------------------------------------------------------
    # Акции — топ по обороту (TQBR board)
    # ------------------------------------------------------------------

    def get_top_stocks(self, limit: int = 30) -> list[dict]:
        """Топ акций на MOEX (board TQBR) с текущими ценами."""
        url = (
            f"{BASE_URL}/engines/stock/markets/shares/boards/TQBR/securities.json"
            f"?limit={limit}&sort_column=VALTODAY&sort_order=desc"
        )
        try:
            resp = self._client.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            sec = data.get("securities", {})
            scols = sec.get("columns", [])
            srows = sec.get("data", [])

            md = data.get("marketdata", {})
            mcols = md.get("columns", [])
            mrows = md.get("data", [])

            # Мэтчим по SECID
            md_map = {}
            for row in mrows:
                item = dict(zip(mcols, row))
                md_map[item.get("SECID")] = item

            result = []
            for row in srows:
                si = dict(zip(scols, row))
                sid = si.get("SECID", "")
                mi = md_map.get(sid, {})

                last = mi.get("LAST")
                if not last or last <= 0:
                    continue

                vol = mi.get("VALTODAY") or 0
                result.append({
                    "secid": sid,
                    "name": si.get("SHORTNAME", ""),
                    "fullname": si.get("SECNAME", ""),
                    "last": last,
                    "open": mi.get("OPEN"),
                    "high": mi.get("HIGH"),
                    "low": mi.get("LOW"),
                    "change_pct": mi.get("LASTTOPREVPRICE"),
                    "volume": vol,
                    "date": mi.get("TRADEDATE", ""),
                    "time": mi.get("TIME", ""),
                })

            # Сортируем по обороту (убывание)
            result.sort(key=lambda x: x.get("volume", 0), reverse=True)
            return result[:limit]

        except Exception as e:
            logger.warning("[MOEX] Top stocks error: %s", e)
            return []

    # ------------------------------------------------------------------
    # История акции
    # ------------------------------------------------------------------

    def get_stock_history(
        self, secid: str,
        date_from: str = "2024-01-01",
        date_to: Optional[str] = None,
    ) -> list[dict]:
        """История котировок акции (TQBR board)."""
        if date_to is None:
            date_to = date.today().strftime("%Y-%m-%d")

        all_rows = []
        start = 0
        page_size = 100

        while True:
            url = (
                f"{BASE_URL}/history/engines/stock/markets/shares/boards/TQBR/securities/{secid}.json"
                f"?from={date_from}&till={date_to}&limit={page_size}&start={start}"
            )
            try:
                resp = self._client.get(url, timeout=15)
                resp.raise_for_status()
                data = resp.json()

                h = data.get("history", {})
                cols = h.get("columns", [])
                rows = h.get("data", [])

                for row in rows:
                    item = dict(zip(cols, row))
                    close_val = item.get("CLOSE") or item.get("LEGALCLOSEPRICE")
                    if close_val and close_val > 0:
                        all_rows.append({
                            "date": item["TRADEDATE"],
                            "open": item.get("OPEN"),
                            "high": item.get("HIGH"),
                            "low": item.get("LOW"),
                            "close": close_val,
                            "volume": item.get("NUMTRADES"),
                        })

                cursor = data.get("history.cursor", {})
                ccols = cursor.get("columns", [])
                crows = cursor.get("data", [])
                if crows:
                    ci = dict(zip(ccols, crows[0]))
                    total = ci.get("TOTAL", 0)
                    if start + page_size >= total:
                        break
                    start += page_size
                else:
                    if len(rows) < page_size:
                        break
                    start += page_size
            except Exception as e:
                logger.warning("[MOEX] Stock history error for %s: %s", secid, e)
                break

        return all_rows

    # ------------------------------------------------------------------
    # Учётные цены ЦБ на драгоценные металлы
    # ------------------------------------------------------------------

    CBR_METAL_URL = "https://www.cbr.ru/scripts/xml_metall.asp"
    CBR_FX_URL    = "https://www.cbr.ru/scripts/XML_dynamic.asp"
    FX_CODES = {"USD": "R01235", "EUR": "R01239", "CNY": "R01375"}

    METALS = [
        {"code": "1", "symbol": "Au", "name": "Золото",   "color": "#FFD700"},
        {"code": "2", "symbol": "Ag", "name": "Серебро",  "color": "#C0C0C0"},
        {"code": "3", "symbol": "Pt", "name": "Платина",  "color": "#9BAEFF"},
        {"code": "4", "symbol": "Pd", "name": "Палладий", "color": "#DDA0DD"},
    ]

    def get_metals_history(
        self,
        date_from: str = "2023-01-01",
        date_to: Optional[str] = None,
    ) -> list[dict]:
        """Учётные цены ЦБ на драгоценные металлы (руб./грамм).
        Возвращает список {'date': ..., 'Au': float, 'Ag': float, 'Pt': float, 'Pd': float}.
        """
        import xml.etree.ElementTree as ET

        if date_to is None:
            date_to = date.today().strftime("%Y-%m-%d")

        d1 = datetime.strptime(date_from[:10], "%Y-%m-%d").strftime("%d/%m/%Y")
        d2 = datetime.strptime(date_to[:10],   "%Y-%m-%d").strftime("%d/%m/%Y")

        metal_map = {m["code"]: m["symbol"] for m in self.METALS}

        try:
            resp = self._client.get(
                self.CBR_METAL_URL,
                params={"date_req1": d1, "date_req2": d2},
                timeout=15,
            )
            resp.raise_for_status()

            root = ET.fromstring(resp.content)
            by_date: dict = {}
            for rec in root.findall("Record"):
                dt_raw = rec.get("Date", "")   # ЦБ возвращает "DD.MM.YYYY"
                code   = rec.get("Code", "")
                sym    = metal_map.get(code)
                if not (dt_raw and sym):
                    continue
                # Нормализуем дату в YYYY-MM-DD
                try:
                    dt = datetime.strptime(dt_raw, "%d.%m.%Y").strftime("%Y-%m-%d")
                except ValueError:
                    dt = dt_raw  # уже в нужном формате
                raw = rec.findtext("Buy") or rec.findtext("Sell") or ""
                raw = raw.replace(",", ".")
                try:
                    price = float(raw)
                except ValueError:
                    continue
                if dt not in by_date:
                    by_date[dt] = {"date": dt}
                by_date[dt][sym] = price

            return sorted(by_date.values(), key=lambda x: x["date"])

        except Exception as e:
            logger.warning("[CBR] Metals history error: %s", e)
            return []

    def get_fx_history(
        self,
        currency: str = "CNY",
        date_from: str = "2023-01-01",
        date_to: Optional[str] = None,
    ) -> list[dict]:
        """История курса валюты.
        CNY: с MOEX (биржевые торги, OHLCWAP + объём + кол-во сделок).
        USD/EUR: официальный курс ЦБ РФ (только закрытие).
        """
        if date_to is None:
            date_to = date.today().strftime("%Y-%m-%d")

        currency = currency.upper()
        if currency == "CNY":
            return self._get_cny_moex_history(date_from, date_to)
        else:
            return self._get_cbr_fx_history(currency, date_from, date_to)

    def _get_cny_moex_history(self, date_from: str, date_to: str) -> list[dict]:
        """История CNY/RUB с MOEX CETS board."""
        all_rows = []
        start = 0
        page_size = 100

        while True:
            url = (
                f"{BASE_URL}/history/engines/currency/markets/selt"
                f"/boards/CETS/securities/CNYRUB_TOM.json"
                f"?from={date_from}&till={date_to}&limit={page_size}&start={start}"
            )
            try:
                resp = self._client.get(url, timeout=15)
                resp.raise_for_status()
                data = resp.json()

                h    = data.get("history", {})
                cols = h.get("columns", [])
                rows = h.get("data", [])

                for row in rows:
                    item  = dict(zip(cols, row))
                    close = item.get("CLOSE") or item.get("LEGALCLOSEPRICE") or item.get("WAPRICE")
                    if close and close > 0:
                        all_rows.append({
                            "date":      item.get("TRADEDATE", ""),
                            "open":      item.get("OPEN"),
                            "high":      item.get("HIGH"),
                            "low":       item.get("LOW"),
                            "close":     close,
                            "wap":       item.get("WAPRICE"),
                            "volume":    item.get("VOLRUR") or item.get("VALUE"),
                            "numtrades": item.get("NUMTRADES"),
                        })

                cursor = data.get("history.cursor", {})
                ccols  = cursor.get("columns", [])
                crows  = cursor.get("data", [])
                if crows:
                    ci = dict(zip(ccols, crows[0]))
                    if start + page_size >= ci.get("TOTAL", 0):
                        break
                    start += page_size
                else:
                    if len(rows) < page_size:
                        break
                    start += page_size
            except Exception as e:
                logger.warning("[MOEX] CNY history error: %s", e)
                break

        return all_rows

    def _get_cbr_fx_history(self, currency: str, date_from: str, date_to: str) -> list[dict]:
        """Официальный курс валюты ЦБ РФ (XML API)."""
        import xml.etree.ElementTree as ET

        code = self.FX_CODES.get(currency)
        if not code:
            return []

        d1 = datetime.strptime(date_from[:10], "%Y-%m-%d").strftime("%d/%m/%Y")
        d2 = datetime.strptime(date_to[:10],   "%Y-%m-%d").strftime("%d/%m/%Y")

        try:
            resp = self._client.get(
                self.CBR_FX_URL,
                params={"date_req1": d1, "date_req2": d2, "VAL_NM_RQ": code},
                timeout=15,
            )
            resp.raise_for_status()

            root    = ET.fromstring(resp.content)
            result  = []
            for rec in root.findall("Record"):
                dt      = rec.get("Date", "")    # "18.03.2024"
                value   = rec.findtext("Value") or ""
                nominal = rec.findtext("Nominal") or "1"
                if not dt or not value:
                    continue
                try:
                    d    = datetime.strptime(dt, "%d.%m.%Y").strftime("%Y-%m-%d")
                    nom  = float(nominal.replace(",", "."))
                    val  = float(value.replace(",", ".")) / nom
                    result.append({"date": d, "close": round(val, 4)})
                except ValueError:
                    pass

            return sorted(result, key=lambda x: x["date"])

        except Exception as e:
            logger.warning("[CBR] FX history error for %s: %s", currency, e)
            return []

    def get_trading_summary(
        self,
        instrument: str,
        trade_date: Optional[str] = None,
    ) -> Optional[dict]:
        """Итоги торгов за день по инструменту.
        instrument: 'CNY', 'USD', 'EUR', 'Au', 'Ag', 'Pt', 'Pd'.
        trade_date: 'YYYY-MM-DD', по умолчанию — вчера (последний торговый день).
        """
        if trade_date is None:
            trade_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

        instr_up = instrument.upper()

        if instr_up == "CNY":
            return self._trading_summary_cny(trade_date)
        elif instr_up in ("USD", "EUR"):
            return self._trading_summary_cbr_fx(instr_up, trade_date)
        elif instr_up in ("AU", "AG", "PT", "PD"):
            sym_map = {"AU": "Au", "AG": "Ag", "PT": "Pt", "PD": "Pd"}
            return self._trading_summary_metal(sym_map[instr_up], trade_date)
        elif instr_up in ("GLDRUB_TOM", "SLVRUB_TOM"):
            # Биржевые торги металлами на MOEX (CETS board)
            return self._trading_summary_moex_metal(instr_up, trade_date)
        return None

    def _trading_summary_cny(self, trade_date: str) -> Optional[dict]:
        """Итоги торгов CNY/RUB на MOEX за указанную дату."""
        url = (
            f"{BASE_URL}/history/engines/currency/markets/selt"
            f"/boards/CETS/securities/CNYRUB_TOM.json"
            f"?from={trade_date}&till={trade_date}&limit=1"
        )
        try:
            resp = self._client.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            h    = data.get("history", {})
            cols = h.get("columns", [])
            rows = h.get("data", [])
            if not rows:
                return None
            item = dict(zip(cols, rows[0]))
            close = item.get("CLOSE") or item.get("WAPRICE")
            if not close or close <= 0:
                return None
            return {
                "instrument":  "CNY/RUB",
                "source":      "MOEX (CETS board)",
                "date":        item.get("TRADEDATE", trade_date),
                "open":        item.get("OPEN"),
                "high":        item.get("HIGH"),
                "low":         item.get("LOW"),
                "close":       close,
                "wap":         item.get("WAPRICE"),
                "volume_rub":  item.get("VOLRUR") or item.get("VALUE"),
                "numtrades":   item.get("NUMTRADES"),
                "unit":        "руб./юань",
            }
        except Exception as e:
            logger.warning("[MOEX] CNY summary error: %s", e)
            return None

    def _trading_summary_moex_metal(self, ticker: str, trade_date: str) -> Optional[dict]:
        """Биржевые торги металлами на MOEX (GLDRUB_TOM, SLVRUB_TOM) — полный OHLCV."""
        name_map = {
            "GLDRUB_TOM": "Золото (MOEX)",
            "SLVRUB_TOM": "Серебро (MOEX)",
        }
        unit_map = {
            "GLDRUB_TOM": "руб./грамм",
            "SLVRUB_TOM": "руб./грамм",
        }
        # Берём 5 дней назад чтобы поймать предыдущую сессию
        date_from = (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
        url = (
            f"{BASE_URL}/history/engines/currency/markets/selt"
            f"/boards/CETS/securities/{ticker}.json"
            f"?from={date_from}&till={trade_date}&limit=10&sort_order=desc"
        )
        try:
            resp = self._client.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            h    = data.get("history", {})
            cols = h.get("columns", [])
            rows = h.get("data", [])
            if not rows:
                return None
            # Берём последнюю торговую сессию с ненулевым закрытием
            last_item = None
            prev_item = None
            for row in rows:
                item = dict(zip(cols, row))
                close = item.get("CLOSE") or item.get("WAPRICE")
                if close and float(close) > 0:
                    if last_item is None:
                        last_item = item
                    elif prev_item is None:
                        prev_item = item
                        break
            if not last_item:
                return None
            close     = last_item.get("CLOSE") or last_item.get("WAPRICE")
            prev_close = (prev_item.get("CLOSE") or prev_item.get("WAPRICE")) if prev_item else None
            result = {
                "instrument": name_map.get(ticker, ticker),
                "source":     "MOEX (CETS board, биржевые торги)",
                "date":       last_item.get("TRADEDATE", trade_date),
                "open":       last_item.get("OPEN"),
                "high":       last_item.get("HIGH"),
                "low":        last_item.get("LOW"),
                "close":      close,
                "wap":        last_item.get("WAPRICE"),
                "volume_rub": last_item.get("VOLRUR") or last_item.get("VALUE"),
                "numtrades":  last_item.get("NUMTRADES"),
                "unit":       unit_map.get(ticker, "руб./грамм"),
            }
            if prev_close and close:
                result["prev_close"] = prev_close
                result["prev_date"]  = prev_item.get("TRADEDATE") if prev_item else None
                result["change"]     = round(float(close) - float(prev_close), 4)
                result["change_pct"] = round((float(close) - float(prev_close)) / float(prev_close) * 100, 2)
            return result
        except Exception as e:
            logger.warning("[MOEX] %s summary error: %s", ticker, e)
            return None

    def _trading_summary_cbr_fx(self, currency: str, trade_date: str) -> Optional[dict]:
        """Официальный курс ЦБ РФ на дату + изменение за день."""
        # Берём окно до 10 дней назад чтобы захватить предыдущую котировку
        window_from = (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=10)).strftime("%Y-%m-%d")
        rows = self._get_cbr_fx_history(currency, window_from, trade_date)
        if not rows:
            return None
        last = rows[-1]
        prev_close = rows[-2]["close"] if len(rows) >= 2 else None
        name = {"USD": "Доллар США", "EUR": "Евро"}.get(currency, currency)
        result = {
            "instrument":  f"{currency}/RUB",
            "source":      "ЦБ РФ (официальный курс)",
            "date":        last["date"],
            "close":       last["close"],
            "unit":        f"руб./{name.split()[0].lower()}",
        }
        if prev_close:
            result["prev_close"]  = prev_close
            result["change"]      = round(last["close"] - prev_close, 4)
            result["change_pct"]  = round((last["close"] - prev_close) / prev_close * 100, 2)
        return result

    def _trading_summary_metal(self, symbol: str, trade_date: str) -> Optional[dict]:
        """Учётная цена металла ЦБ на дату + цены покупки/продажи + изменение за день."""
        import xml.etree.ElementTree as ET
        # Запрашиваем окно 14 дней, чтобы получить текущую и предыдущую котировки
        window_from = (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=14)).strftime("%Y-%m-%d")
        d1 = datetime.strptime(window_from, "%Y-%m-%d").strftime("%d/%m/%Y")
        d2 = datetime.strptime(trade_date,  "%Y-%m-%d").strftime("%d/%m/%Y")
        name_map = {"Au": "Золото", "Ag": "Серебро", "Pt": "Платина", "Pd": "Палладий"}
        metal_code_map = {"Au": "1", "Ag": "2", "Pt": "3", "Pd": "4"}
        try:
            resp = self._client.get(
                self.CBR_METAL_URL,
                params={"date_req1": d1, "date_req2": d2},
                timeout=15,
            )
            resp.raise_for_status()
            root = ET.fromstring(resp.content)
            target_code = metal_code_map.get(symbol)
            # Собираем по дате: {date: {buy, sell}}
            by_date: dict = {}
            for rec in root.findall("Record"):
                if rec.get("Code") != target_code:
                    continue
                dt_raw = rec.get("Date", "")
                try:
                    dt = datetime.strptime(dt_raw, "%d.%m.%Y").strftime("%Y-%m-%d")
                except ValueError:
                    dt = dt_raw
                buy_raw  = (rec.findtext("Buy")  or "").replace(",", ".")
                sell_raw = (rec.findtext("Sell") or "").replace(",", ".")
                try:
                    buy  = float(buy_raw)  if buy_raw  else None
                    sell = float(sell_raw) if sell_raw else None
                except ValueError:
                    buy = sell = None
                if buy is not None or sell is not None:
                    by_date[dt] = {"buy": buy, "sell": sell}
            if not by_date:
                return None
            sorted_dates = sorted(by_date.keys())
            last_date    = sorted_dates[-1]
            last         = by_date[last_date]
            prev_date    = sorted_dates[-2] if len(sorted_dates) >= 2 else None
            prev         = by_date[prev_date] if prev_date else None
            # Учётная цена ЦБ = цена покупки (Buy)
            close     = last.get("buy") or last.get("sell")
            prev_close = prev.get("buy") or prev.get("sell") if prev else None
            result = {
                "instrument": f"{symbol} ({name_map.get(symbol, symbol)})",
                "source":     "ЦБ РФ (учётная цена)",
                "date":       last_date,
                "close":      close,           # учётная цена (покупка)
                "buy":        last.get("buy"),  # цена покупки ЦБ
                "sell":       last.get("sell"), # цена продажи ЦБ
                "unit":       "руб./грамм",
            }
            if prev_close and close:
                result["prev_close"] = prev_close
                result["prev_date"]  = prev_date
                result["change"]     = round(close - prev_close, 4)
                result["change_pct"] = round((close - prev_close) / prev_close * 100, 2)
            return result
        except Exception as e:
            logger.warning("[CBR] Metal trading summary error (%s): %s", symbol, e)
            # Fallback: простой запрос через get_metals_history
            rows = self.get_metals_history(window_from, trade_date)
            for r in reversed(rows):
                val = r.get(symbol)
                if val:
                    return {
                        "instrument": f"{symbol} ({name_map.get(symbol, symbol)})",
                        "source":     "ЦБ РФ (учётная цена)",
                        "date":       r["date"],
                        "close":      val,
                        "unit":       "руб./грамм",
                    }
            return None

    # ------------------------------------------------------------------
    # Демо-данные (fallback)
    # ------------------------------------------------------------------

    @staticmethod
    def _demo_key_rate(date_from: str, date_to: str) -> list[dict]:
        """Реалистичная история ключевой ставки ЦБ (демо)."""
        history = [
            ("2020-01-01", 6.25),
            ("2020-02-07", 6.00),
            ("2020-04-27", 5.50),
            ("2020-06-22", 4.50),
            ("2020-07-27", 4.25),
            ("2021-03-22", 4.50),
            ("2021-04-26", 5.00),
            ("2021-06-14", 5.50),
            ("2021-07-26", 6.50),
            ("2021-09-13", 6.75),
            ("2021-10-25", 7.50),
            ("2021-12-17", 8.50),
            ("2022-02-14", 9.50),
            ("2022-02-28", 20.00),
            ("2022-04-11", 17.00),
            ("2022-05-04", 14.00),
            ("2022-05-26", 11.00),
            ("2022-06-10", 9.50),
            ("2022-07-25", 8.00),
            ("2022-09-16", 7.50),
            ("2023-02-16", 7.50),
            ("2023-07-21", 8.50),
            ("2023-08-15", 12.00),
            ("2023-09-18", 13.00),
            ("2023-10-27", 15.00),
            ("2023-12-15", 16.00),
            ("2024-02-16", 16.00),
            ("2024-07-26", 18.00),
            ("2024-09-13", 19.00),
            ("2024-10-25", 21.00),
            ("2025-02-14", 21.00),
        ]
        d_from = datetime.strptime(date_from[:10], "%Y-%m-%d").date()
        d_to   = datetime.strptime(date_to[:10],   "%Y-%m-%d").date()
        result = []
        for i, (d_str, rate) in enumerate(history):
            d = datetime.strptime(d_str, "%Y-%m-%d").date()
            if d < d_from:
                continue
            if d > d_to:
                break
            result.append({"date": d_str, "rate": rate})
        return result

    @staticmethod
    def _demo_bonds() -> list[dict]:
        """Демо-облигации Примсоцбанка."""
        return [
            {
                "secid":    "RU000A106PE0",
                "name":     "Примсоцбанк БО-01",
                "isin":     "RU000A106PE0",
                "matdate":  "2026-06-15",
                "coupon":   22.5,
                "face":     1000,
                "currency": "RUB",
            },
            {
                "secid":    "RU000A105NN5",
                "name":     "Примсоцбанк БО-02",
                "isin":     "RU000A105NN5",
                "matdate":  "2025-12-10",
                "coupon":   18.0,
                "face":     1000,
                "currency": "RUB",
            },
        ]
