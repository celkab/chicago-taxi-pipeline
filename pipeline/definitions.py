import dagster as dg
from pipeline.assets import bronze, silver, gold

defs = dg.Definitions(assets=dg.load_assets_from_modules([bronze, silver, gold]))