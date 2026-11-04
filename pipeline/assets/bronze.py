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
            chunks, total, offset = [], 0, 0
            while True:                         # pagination par tranches de PAGE_SIZE
                text = _fetch({'$where': where, '$order': ':id',   # ordre stable
                               '$limit': config.PAGE_SIZE, '$offset': offset})
                lines = text.strip().splitlines()
                n = len(lines) - 1              # -1 : en-tête
                if n <= 0:
                    break
                chunks.append('\n'.join(lines if not chunks else lines[1:]))  # en-tête une seule fois
                total += n
                if n < config.PAGE_SIZE:
                    break
                offset += config.PAGE_SIZE
            if chunks:                          # pas de fichier pour un jour sans données
                out.write_text('\n'.join(chunks) + '\n', encoding='utf-8')
            context.log.info(f'{day} : {total} lignes')
        day += timedelta(days=1)
