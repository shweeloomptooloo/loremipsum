from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.types import StructType
import pyspark.sql.functions as F
from typing import Optional

def ingest_raw_to_bronze(
    spark: SparkSession, 
    source_path: str
    ) -> DataFrame:
    """
    Ingests raw files from the Landing Zone into the Bronze layer.
    
    In a real-world scenario, this function would handle various file formats
    and apply initial metadata (like source file name and ingestion timestamp).
    
    Args:
        spark: The active SparkSession.
        source_path: Path to the raw .tgz or extracted .csv files.
        schema: Optional schema to enforce during read.
        
    Returns:
        DataFrame: The raw data with added ingestion metadata.
    """
    
    raw_df = spark.read.text(source_path)

    raw_df = raw_df.filter(~F.col("value").startswith("#"))
    
    raw_df = (raw_df.select(
        F.to_timestamp(F.substring("value", 1, 19)).alias("dtg"),
        F.trim(F.substring("value", 22, 20)).alias("location"),
        F.trim(F.substring("value", 42, 48)).alias("name"),
        F.substring("value", 90, 20).try_cast("double").alias("latitude"),
        F.substring("value", 110, 20).try_cast("double").alias("longitude"),
        F.substring("value", 130, 20).try_cast("double").alias("altitude"),
        F.substring("value", 150, 20).try_cast("int").alias("u_bool_10"),
        F.substring("value", 170, 20).try_cast("double").alias("t_dryb_10"),
        F.substring("value", 190, 20).try_cast("double").alias("tn_10cm_past_6h_10"),
        F.substring("value", 210, 20).try_cast("double").alias("t_dewp_10"),
        F.substring("value", 230, 20).try_cast("double").alias("t_dewp_sea_10"),
        F.substring("value", 250, 20).try_cast("double").alias("t_dryb_sea_10"),
        F.substring("value", 270, 20).try_cast("double").alias("tn_dryb_10"),
        F.substring("value", 290, 20).try_cast("double").alias("t_wetb_10"),
        F.substring("value", 310, 20).try_cast("double").alias("tx_dryb_10"),
        F.substring("value", 330, 20).try_cast("double").alias("u_10"),
        F.substring("value", 350, 20).try_cast("double").alias("u_sea_10")
        )
              )
    
    bronze_df = raw_df.withColumn("_ingested_at", F.current_timestamp())
    
    return bronze_df

def raw_to_silver(df_bronze: DataFrame) -> DataFrame:
    """
    Performs data cleaning and standardization.
    Target: Filter for De Bilt (260_T_a), handle types, and units.
    
    Args:
        df_bronze: The raw ingested data from the Bronze layer.
        
    Returns:
        DataFrame: Cleaned data ready for business logic.
    """
    silver_df = (
        df_bronze
        .filter(F.year(F.col("dtg")) >= 2003)
        .filter(F.col("location") == "260_T_a")
        # .filter(F.col("tx_dryb_10").between(-20,40))
        # .filter(F.col("tn_dryb_10").between(-20,40))
        .withColumn("date", F.to_date(F.col("dtg")))
        .groupBy("location", "date").agg(
            F.max("tx_dryb_10").alias("max_temp"),
            F.min("tn_dryb_10").alias("min_temp")
        )
        .select("location", "date", "max_temp", "min_temp")
        )
    
    return silver_df

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F

def calculate_extreme_weather(
    df_silver: DataFrame, 
    base_threshold: float, 
    base_col: str, 
    intensity_threshold: float, 
    intensity_col: str,
    intensity_count: int = 3,
    duration_days: int = 5,
    operator: str = "ge" # "ge" for >= (heat), "lt" for < (cold)
) -> DataFrame:
    """
    Calculates weather 'waves' based on custom thresholds and columns.
    Uses Gaps and Islands logic via Spark Window functions.
    """
    
    # 1. Define conditions based on the operator
    if operator == "ge":
        is_base = F.col(base_col) >= base_threshold
        is_intense = F.when(F.col(intensity_col) >= intensity_threshold, 1).otherwise(0)
        extreme_agg = F.max(base_col).alias("peak_temperature")
    elif operator == "lt":
        is_base = F.col(base_col) < base_threshold
        is_intense = F.when(F.col(intensity_col) < intensity_threshold, 1).otherwise(0)
        extreme_agg = F.min(intensity_col).alias("peak_temperature")
    else:
        raise ValueError("Operator must be 'ge' (>=) or 'lt' (<)")

    # 2. Filter for base condition
    df_filtered = df_silver.withColumn("is_intense", is_intense) \
                           .filter(is_base)

    # 3. Gaps and Islands Logic
    window_spec = Window.partitionBy("location").orderBy("date")
    
    df_islands = df_filtered.withColumn("row_num", F.row_number().over(window_spec)) \
                            .withColumn("island_group", F.expr("date_add(date, -row_num)"))

    # 4. Aggregate sequences
    candidates = df_islands.groupBy("location", "island_group").agg(
        F.min("date").alias("from_date"),
        F.max("date").alias("to_date"),
        F.count("date").alias("duration_days"),
        F.sum("is_intense").alias("intense_days"),
        extreme_agg
    )

    # 5. Apply Wave Definition (5+ days duration, 3+ intense days)
    final_report = candidates.filter(
        (F.col("duration_days") >= duration_days) & 
        (F.col("intense_days") >= intensity_count)
    ).select(
        F.col("from_date"),
        F.col("to_date"),
        F.col("duration_days"),
        F.col("intense_days"),
        F.col("peak_temperature")
    ).orderBy(F.col("from_date"))

    return final_report

def save_to_delta(df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
    """
    Utility function to write DataFrames to Delta tables.
    
    Args:
        df: The DataFrame to persist.
        table_name: The destination table name in the catalog.
        mode: Spark save mode (default: overwrite).
    """
    df.write.format("delta").mode(mode).saveAsTable(table_name)