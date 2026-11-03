import time
from datetime import date, timedelta

import requests
import dagster as dg

from pipeline import config


def _fetch(params, retries=3):
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(config.SODA_URL, params=params, timeout=300)
            r.raise_for_status()
            return r.text
        except requests.exceptions.RequestException:
            if attempt == retries:
                raise
            time.sleep(5 * attempt)


@dg.asset
def bronze_trips(context: dg.AssetExecutionContext):
    config.BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    day = date.fromisoformat(config.START_DATE)
    end = date.fromisoformat(config.END_DATE)

    while day < end:
        out = config.BRONZE_DIR / f'trips_{day.isoformat()}.csv'
        if out.exists():                        # idempotence : on ne retélécharge pas
            context.log.info(f'{day} déjà présent, ignoré')
        else:
            nxt = day + timedelta(days=1)
            where = f"trip_start_timestamp >= '{day}T00:00:00' AND trip_start_timestamp < '{nxt}T00:00:00'"
            text = _fetch({'$where': where, '$limit': config.PAGE_SIZE})
            n = len(text.strip().splitlines()) - 1
            if n >= config.PAGE_SIZE:
                context.log.warning(f'{day} : {n} lignes, page pleine, des lignes peuvent manquer')
            out.write_text(text, encoding='utf-8')
            context.log.info(f'{day} : {n} lignes')
        day += timedelta(days=1)