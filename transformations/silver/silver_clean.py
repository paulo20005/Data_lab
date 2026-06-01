from pyspark import pipelines as dp
from pyspark.sql.functions import col, dense_rank, when, regexp_extract, round
from pyspark.sql.window import Window

# Här gör vi rent silver layer från rådata
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
    
    # Väljer bara de kolumner jag behöver och ger dom enklare namn
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
    
    # Tar bort alla rader som har "d" (days) för dom är ogiltiga
    df = df.filter(~col("athlete_performance_raw").contains("d"))
    
    # Skapar ett eget id för varje event med dense_rank
    window_spec = Window.orderBy("event_name")
    df = df.withColumn("event_id", dense_rank().over(window_spec))
    
    # Plockar ut timmar, minuter och sekunder från tiden
    df = df.withColumn("hours", regexp_extract(col("athlete_performance_raw"), r"(\d+):", 1).cast("int"))
    df = df.withColumn("minutes", regexp_extract(col("athlete_performance_raw"), r":(\d+):", 1).cast("int"))
    df = df.withColumn("seconds", regexp_extract(col("athlete_performance_raw"), r":(\d+)$", 1).cast("int"))
    
    # Räknar om till timmar som decimal (typ 4.86 timmar istället för 4:51:39)
    df = df.withColumn(
        "athlete_performance",
        when(
            col("hours").isNotNull() & col("minutes").isNotNull() & col("seconds").isNotNull(),
            round(col("hours") + col("minutes") / 60 + col("seconds") / 3600, 2)
        ).otherwise(None)
    )
    
    # Tar bort temporära kolumnenr som jag inte behöver spara
    df = df.drop("hours", "minutes", "seconds", "athlete_performance_raw")
    
    return df