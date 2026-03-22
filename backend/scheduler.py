"""
Автоматическое обновление данных с ЦБ РФ.
Данные ЦБ публикуются раз в месяц (~15-е число следующего месяца).
"""
import logging
from datetime import date, datetime

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None


def _check_and_refresh(cbr_parser):
    """Проверяет наличие новых данных ЦБ и обновляет кэш при необходимости."""
    logger.info("[Scheduler] Проверка актуальности данных ЦБ...")
    try:
        today = date.today()
        # Данные за прошлый месяц должны появиться ~15-го текущего
        if today.day >= 15:
            target_month = today.month - 1 if today.month > 1 else 12
            target_year  = today.year if today.month > 1 else today.year - 1
        else:
            target_month = today.month - 2 if today.month > 2 else 12 + today.month - 2
            target_year  = today.year if today.month > 2 else today.year - 1

        target_date = f"{target_year}-{target_month:02d}-01"
        from pathlib import Path
        metrics_dir = Path(__file__).parent / "data" / "metrics"
        cache_file = metrics_dir / f"metrics_{target_year}_{target_month:02d}.json"

        if cache_file.exists():
            logger.info(f"[Scheduler] Данные за {target_date} уже в кэше.")
            return

        logger.info(f"[Scheduler] Загружаем данные за {target_date}...")
        data = cbr_parser.get_metrics_for_date(target_date)
        logger.info(f"[Scheduler] Обновлено {len(data)} банков за {target_date}.")

    except Exception as e:
        logger.error(f"[Scheduler] Ошибка при обновлении: {e}")


def start_scheduler(cbr_parser) -> None:
    """Запускает фоновый планировщик."""
    global _scheduler
    if _scheduler and _scheduler.running:
        return

    _scheduler = BackgroundScheduler()

    # Проверка каждый день в 06:00
    _scheduler.add_job(
        _check_and_refresh,
        trigger=CronTrigger(hour=6, minute=0),
        args=[cbr_parser],
        id="cbr_daily_check",
        replace_existing=True,
    )

    _scheduler.start()
    logger.info("[Scheduler] Планировщик запущен (ежедневно в 06:00).")

    # Сразу выполняем первую проверку в фоне
    _scheduler.add_job(
        _check_and_refresh,
        args=[cbr_parser],
        id="cbr_startup_check",
    )


def stop_scheduler() -> None:
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("[Scheduler] Планировщик остановлен.")
