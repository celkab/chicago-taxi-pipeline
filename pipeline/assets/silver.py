import dagster as dg
from pyspark.sql import functions as F

from pipeline import config
from pipeline.spark import get_spark
from pipeline.assets.bronze import bronze_trips


@dg.asset(deps=[bronze_trips])
def silver_trips(context: dg.AssetExecutionContext):
    spark = get_spark()
    df = spark.read.csv(str(config.BRONZE_DIR), header=True)   # tout en string au départ
    n_raw = df.count()

    df = (
        df.withColumn('trip_start_timestamp', F.to_timestamp('trip_start_timestamp'))
          .withColumn('trip_end_timestamp', F.to_timestamp('trip_end_timestamp'))
          .withColumn('trip_seconds', F.col('trip_seconds').cast('int'))
          .withColumn('trip_miles', F.col('trip_miles').cast('double'))
          .withColumn('fare', F.col('fare').cast('double'))
          .withColumn('tips', F.col('tips').cast('double'))
          .withColumn('tolls', F.col('tolls').cast('double'))
          .withColumn('extras', F.col('extras').cast('double'))
          .withColumn('trip_total', F.col('trip_total').cast('double'))
          # "8.0" -> 8 : on passe par double avant int
          .withColumn('pickup_community_area', F.col('pickup_community_area').cast('double').cast('int'))
          .withColumn('dropoff_community_area', F.col('dropoff_community_area').cast('double').cast('int'))
          .withColumn('trip_date', F.to_date('trip_start_timestamp'))
    )

    # Règles de qualité : chaque ligne reçoit la liste des règles qu'elle viole (voir README).
    # Une condition NULL (valeur manquante/illisible) compte comme violation pour les champs obligatoires.
    rules = {
        'trip_id_null': F.col('trip_id').isNull(),
        'start_null': F.col('trip_start_timestamp').isNull(),
        'duration_not_positive': F.coalesce(F.col('trip_seconds') <= 0, F.lit(True)),
        'total_negative': F.coalesce(F.col('trip_total') < 0, F.lit(True)),
        'end_before_start': F.coalesce(F.col('trip_end_timestamp') < F.col('trip_start_timestamp'), F.lit(False)),
    }
    df = df.withColumn(
        'reject_reasons',
        F.array_compact(F.array(*[F.when(cond, F.lit(name)) for name, cond in rules.items()])),
    ).cache()

    rejected = df.filter(F.size('reject_reasons') > 0)
    clean = df.filter(F.size('reject_reasons') == 0).drop('reject_reasons').dropDuplicates(['trip_id'])

    n_rejected = rejected.count()
    n_clean = clean.count()
    n_dups = n_raw - n_rejected - n_clean
    context.log.info(
        f'lignes brutes={n_raw}, rejetées={n_rejected}, doublons supprimés={n_dups}, conservées={n_clean}'
    )
    for row in rejected.select(F.explode('reject_reasons').alias('rule')).groupBy('rule').count().collect():
        context.log.info(f"  règle {row['rule']} : {row['count']} lignes")

    rejected.write.mode('overwrite').parquet(str(config.REJECTED_DIR))
    clean.write.mode('overwrite').partitionBy('trip_date').parquet(str(config.SILVER_DIR))
    df.unpersist()
