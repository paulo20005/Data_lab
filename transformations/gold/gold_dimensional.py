from pyspark import pipelines as dp
from pyspark.sql.functions import col

# Här gör jag en tabell över alla olika lopp som finns

@dp.table(
    name="marathos_catalog.marathon_gold.dim_event",
    comment="Dimension table for events - Gold layer",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5"
    }
)
def dim_event():
    # Läser från silver och plockar ut unika event
    df = spark.read.table("marathos_catalog.marathon_silver.silver_cleaned")
    return df.select("event_id", "event_name", "event_distance").distinct()

# Här gör jag en tabell över alla löpare och info om dom
@dp.table(
    name="marathos_catalog.marathon_gold.dim_athlete",
    comment="Dimension table for athletes - Gold layer",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5"
    }
)
def dim_athlete():
    # Läser från silver och plockar ut unika idrottare
    df = spark.read.table("marathos_catalog.marathon_silver.silver_cleaned")
    return df.select("athlete_id", "athlete_country", "birth_year", "gender").distinct()

# Här lagrar jag alla resultat, alltså vem som sprang vilket lopp och hur snabbt
@dp.table(
    name="marathos_catalog.marathon_gold.fct_results",
    comment="Fact table for race results - Gold layer",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5"
    }
)
def fct_results():
    # Läser från silver och kopplar ihop event_id med athlete_id och deras tid
    df = spark.read.table("marathos_catalog.marathon_silver.silver_cleaned")
    return df.select("event_id", "athlete_id", "athlete_performance")

# En vy som bara visar 50km lopp
@dp.table(
    name="marathos_catalog.marathon_gold.vw_50km_races",
    comment="View for 50km races - Gold layer"
)
def vw_50km_races():
    # Filtrerar bara 50km lopp
    df = spark.read.table("marathos_catalog.marathon_silver.silver_cleaned")
    return df.filter(col("event_distance") == "50km") \
        .select("event_name", "athlete_id", "athlete_performance")

# En vy som bara visar 100km lopp
@dp.table(
    name="marathos_catalog.marathon_gold.vw_100km_races",
    comment="View for 100km races - Gold layer"
)
def vw_100km_races():
    # Filtrerar bara 100km lopp
    df = spark.read.table("marathos_catalog.marathon_silver.silver_cleaned")
    return df.filter(col("event_distance") == "100km") \
        .select("event_name", "athlete_id", "athlete_performance")