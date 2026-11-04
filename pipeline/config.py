import os
from pathlib import Path

DATA_DIR = Path(os.getenv('DATA_DIR', 'data'))
BRONZE_DIR = DATA_DIR / 'bronze'
SILVER_DIR = DATA_DIR / 'silver'
REJECTED_DIR = DATA_DIR / 'rejected'   
GOLD_DIR = DATA_DIR / 'gold'

START_DATE = os.getenv('START_DATE', '2023-01-01')
END_DATE = os.getenv('END_DATE', '2023-01-08')   

SODA_URL = 'https://data.cityofchicago.org/resource/wrvz-psew.csv'
PAGE_SIZE = 100000