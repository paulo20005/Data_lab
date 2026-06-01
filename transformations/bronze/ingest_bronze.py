from pyspark import pipelines as dp

@dp.table(
    name="marathos_catalog.marathon_bronze.bronze_races",
    comment="Raw marathon data - Bronze layer",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5"
    }
)
def bronze_races():
    return spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("/Volumes/marathos_catalog/marathon_raw/marathon_volume/TWO_CENTURIES_OF_UM_RACES.csv")