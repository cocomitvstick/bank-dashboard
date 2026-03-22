"""
Обработка и агрегация банковских данных.
Чистые функции без внешних зависимостей.
"""
import statistics
from typing import Optional


class DataProcessor:

    # ------------------------------------------------------------------
    # Базовые финансовые коэффициенты
    # ------------------------------------------------------------------

    def calculate_roa(self, profit: float, assets: float) -> Optional[float]:
        """ROA = Прибыль / Активы * 100"""
        if not assets:
            return None
        return round(profit / assets * 100, 2)

    def calculate_roe(self, profit: float, capital: float) -> Optional[float]:
        """ROE = Прибыль / Капитал * 100"""
        if not capital:
            return None
        return round(profit / capital * 100, 2)

    def calculate_nim(
        self,
        interest_income: float,
        interest_expense: float,
        earning_assets: float,
    ) -> Optional[float]:
        """NIM = (Проц.доходы - Проц.расходы) / Работающие активы * 100"""
        if not earning_assets:
            return None
        return round((interest_income - interest_expense) / earning_assets * 100, 2)

    def calculate_npl_ratio(
        self, overdue_loans: float, total_loans: float
    ) -> Optional[float]:
        """NPL = Просроченные кредиты / Кредитный портфель * 100"""
        if not total_loans:
            return None
        return round(overdue_loans / total_loans * 100, 2)

    def calculate_leverage(self, assets: float, capital: float) -> Optional[float]:
        """Финансовый рычаг = Активы / Капитал"""
        if not capital:
            return None
        return round(assets / capital, 2)

    # ------------------------------------------------------------------
    # Групповая статистика
    # ------------------------------------------------------------------

    def get_group_stats(self, banks_data: list[dict], metric: str) -> dict:
        """Статистика по группе: среднее, медиана, мин, макс, перцентили."""
        values = [
            b[metric]
            for b in banks_data
            if b.get(metric) is not None and isinstance(b[metric], (int, float))
        ]
        if not values:
            return {}

        values_sorted = sorted(values)
        n = len(values_sorted)

        def percentile(p: float) -> float:
            idx = int(p / 100 * (n - 1))
            return round(values_sorted[idx], 4)

        return {
            "count": n,
            "mean": round(statistics.mean(values), 4),
            "median": round(statistics.median(values), 4),
            "min": round(min(values), 4),
            "max": round(max(values), 4),
            "p25": percentile(25),
            "p75": percentile(75),
            "p90": percentile(90),
            "stdev": round(statistics.stdev(values), 4) if n > 1 else 0,
        }

    def get_rank(self, value: float, all_values: list[float], ascending: bool = False) -> int:
        """
        Место банка среди всех банков по показателю.
        ascending=False -> больше = лучше (активы, прибыль)
        ascending=True  -> меньше = лучше (NPL)
        """
        sorted_vals = sorted(all_values, reverse=not ascending)
        try:
            return sorted_vals.index(value) + 1
        except ValueError:
            # Ближайшее значение
            closest = min(sorted_vals, key=lambda v: abs(v - value))
            return sorted_vals.index(closest) + 1

    def get_percentile(self, value: float, all_values: list[float], ascending: bool = False) -> float:
        """
        Перцентиль банка (75-й = лучше 75% банков).
        ascending=False -> больше = лучше
        """
        if not all_values:
            return 0.0
        n = len(all_values)
        if ascending:
            count_worse = sum(1 for v in all_values if v > value)
        else:
            count_worse = sum(1 for v in all_values if v < value)
        return round(count_worse / n * 100, 1)

    # ------------------------------------------------------------------
    # Нормализация для радар-диаграммы
    # ------------------------------------------------------------------

    def normalize_for_radar(
        self, bank_metrics: dict, group_stats: dict[str, dict]
    ) -> dict:
        """
        Нормализует метрики банка в шкалу 0–100 относительно всей выборки.
        Возвращает словарь {metric: score 0..100}.
        """
        radar_metrics = {
            "n1":    {"ascending": False, "weight": 1},
            "n2":    {"ascending": False, "weight": 1},
            "n3":    {"ascending": False, "weight": 1},
            "roe":   {"ascending": False, "weight": 1},
            "nim":   {"ascending": False, "weight": 1},
            "npl":   {"ascending": True,  "weight": 1},  # меньше NPL = лучше
        }

        result = {}
        for metric, cfg in radar_metrics.items():
            val = bank_metrics.get(metric)
            stats = group_stats.get(metric, {})
            if val is None or not stats:
                result[metric] = 50
                continue

            lo, hi = stats.get("p25", stats.get("min", 0)), stats.get("p75", stats.get("max", 100))
            if hi == lo:
                result[metric] = 50
                continue

            # Клипируем и масштабируем в 0..100
            score = (val - lo) / (hi - lo) * 100
            if cfg["ascending"]:
                score = 100 - score  # меньше = лучше, инвертируем
            result[metric] = round(max(0, min(100, score)), 1)

        return result

    # ------------------------------------------------------------------
    # Дельта к предыдущему периоду
    # ------------------------------------------------------------------

    def calc_delta(self, current: Optional[float], previous: Optional[float]) -> dict:
        """Возвращает абсолютное и процентное изменение."""
        if current is None or previous is None or previous == 0:
            return {"abs": None, "pct": None}
        abs_delta = round(current - previous, 4)
        pct_delta = round((current - previous) / abs(previous) * 100, 2)
        return {"abs": abs_delta, "pct": pct_delta}

    # ------------------------------------------------------------------
    # Агрегация группы банков (топ-N)
    # ------------------------------------------------------------------

    def aggregate_group(
        self,
        banks_data: list[dict],
        metric: str,
        top_n: Optional[int] = None,
    ) -> dict:
        """
        Вычисляет агрегаты (среднее, медиана, сумма) по группе банков.
        Если top_n задан — берёт топ N по убыванию активов.
        """
        data = banks_data
        if top_n is not None:
            data = sorted(data, key=lambda b: b.get("assets", 0), reverse=True)[:top_n]

        values = [b[metric] for b in data if b.get(metric) is not None]
        if not values:
            return {}

        return {
            "mean":   round(statistics.mean(values), 4),
            "median": round(statistics.median(values), 4),
            "sum":    round(sum(values), 4),
            "count":  len(values),
        }
