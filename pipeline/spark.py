import os
from pyspark.sql import SparkSession


def get_spark():
    return (
        SparkSession.builder.master('local[*]')
        .appName('chicago-pipeline')
        .config('spark.driver.memory', os.getenv('SPARK_DRIVER_MEMORY', '2g'))
        # Spark 4 active ANSI par défaut : un cast invalide lèverait une erreur. On veut un NULL,
        # que les règles de qualité du silver enverront en rejet.
        .config('spark.sql.ansi.enabled', 'false')
        .getOrCreate()
    )
