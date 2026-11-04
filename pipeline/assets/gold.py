import dagster as dg
from pyspark.sql import functions as F

from pipeline import config
from pipeline.spark import get_spark
from pipeline.assets.silver import silver_trips


def _read_silver():
    return get_spark().read.parquet(str(config.SILVER_DIR))


@dg.asset(deps=[silver_trips])
def gold_revenue_per_day():
    df = (
        _read_silver()
        .groupBy('trip_date')
        .agg(F.round(F.sum('trip_total'), 2).alias('revenue'),
             F.count('*').alias('nb_trips'))
        .orderBy('trip_date')
    )
    df.write.mode('overwrite').parquet(str(config.GOLD_DIR / 'revenue_per_day'))


@dg.asset(deps=[silver_trips])
def gold_avg_duration_per_day():
    df = (
        _read_silver()
        .groupBy('trip_date')
        .agg(F.round(F.avg('trip_seconds') / 60, 2).alias('avg_duration_min'),
             F.count('*').alias('nb_trips'))
        .orderBy('trip_date')
    )
    df.write.mode('overwrite').parquet(str(config.GOLD_DIR / 'avg_duration_per_day'))


@dg.asset(deps=[silver_trips])
def gold_top_pickup_zones():
    df = (
        _read_silver()
        .filter(F.col('pickup_community_area').isNotNull())   # zone inconnue exclue du classement
        .groupBy('pickup_community_area')
        .agg(F.count('*').alias('nb_trips'))
        .orderBy(F.desc('nb_trips'))
        .limit(10)
    )
    df.write.mode('overwrite').parquet(str(config.GOLD_DIR / 'top_pickup_zones'))
