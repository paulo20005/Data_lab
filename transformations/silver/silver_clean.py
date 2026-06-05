from pyspark import pipelines as dp
from pyspark.sql.functions import col, dense_rank, split, round
from pyspark.sql.window import Window

# Här gör vi rent silver-lagret från rådata
@dp.table(
    name="marathos_catalog.marathon_silver.silver_cleaned",
    comment="Cleaned marathon data - Silver layer",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5"
    }
)
def silver_cleaned():
    # Läser in datan från bronze
    df = spark.read.table("marathos_catalog.marathon_bronze.bronze_races")
    
    # Väljer bara de kolumner jag behöver
    df = df.select(
        col("`Year of event`").alias("event_year"),
        col("`Event name`").alias("event_name"),
        col("`Event distance/length`").alias("event_distance"),
        col("`Athlete performance`").alias("athlete_performance_raw"),
        col("`Athlete country`").alias("athlete_country"),
        col("`Athlete year of birth`").alias("birth_year"),
        col("`Athlete gender`").alias("gender"),
        col("`Athlete ID`").alias("athlete_id")
    )
    
    # Tar bort rader med "d" (days)
    df = df.filter(~col("athlete_performance_raw").contains("d"))
    
    # Behåller bara rader med tid-format
    df = df.filter(col("athlete_performance_raw").contains(":"))
    
    # Skapar event_id
    window_spec = Window.orderBy("event_name")
    df = df.withColumn("event_id", dense_rank().over(window_spec))
    
    # Tar bort dubbletter (samma löpare i samma event)
    df = df.dropDuplicates(["athlete_id", "event_id"])
    
    # Delar upp tiden
    df = df.withColumn("hours_split", split(col("athlete_performance_raw"), ":"))
    df = df.withColumn("hours", col("hours_split").getItem(0).cast("int"))
    df = df.withColumn("minutes", col("hours_split").getItem(1).cast("int"))
    df = df.withColumn("seconds_str", col("hours_split").getItem(2))
    df = df.withColumn("seconds", split(col("seconds_str"), " ").getItem(0).cast("int"))
    
    # Räknar om till timmar
    df = df.withColumn(
        "athlete_performance",
        round(col("hours") + col("minutes") / 60 + col("seconds") / 3600, 2)
    )
    
    # Tar bort rader med 0
    df = df.filter(col("athlete_performance") > 0)
    
    # Tar bort temporära kolumner
    df = df.drop("hours_split", "hours", "minutes", "seconds_str", "seconds", "athlete_performance_raw")
    
    return df