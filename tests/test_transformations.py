import pytest
from datetime import date, timedelta
from pyspark.sql import Row, SparkSession
from src.transformations import calculate_extreme_weather

@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for the entire test session."""
    return SparkSession.builder \
        .master("local[1]") \
        .appName('integrity-tests') \
        .getOrCreate()

def test_calculate_extreme_weather_multi_period(spark):
    """
    Tests that a heatwave is correctly identified only when 
    both duration and intensity criteria are met.
    """
    # Period A: Valid Heatwave (6 days >= 25, 3 days >= 30)
    # Period B: Gap day (20 degrees)
    # Period C: Invalid (5 days >= 25, but only 2 days >= 30 - Fails intensity)
    data = [
        # Period A (Success)
        Row(location="260_T_a", date=date(2023, 7, 1), max_temp=26.0, min_temp=15.0),
        Row(location="260_T_a", date=date(2023, 7, 2), max_temp=31.0, min_temp=15.0), 
        Row(location="260_T_a", date=date(2023, 7, 3), max_temp=32.0, min_temp=15.0), 
        Row(location="260_T_a", date=date(2023, 7, 4), max_temp=33.0, min_temp=15.0), 
        Row(location="260_T_a", date=date(2023, 7, 5), max_temp=26.0, min_temp=15.0),
        Row(location="260_T_a", date=date(2023, 7, 6), max_temp=26.0, min_temp=15.0),
        
        # Period B (Separator)
        Row(location="260_T_a", date=date(2023, 7, 7), max_temp=20.0, min_temp=15.0),
        
        # Period C (Failure - Intensity)
        Row(location="260_T_a", date=date(2023, 7, 10), max_temp=26.0, min_temp=15.0),
        Row(location="260_T_a", date=date(2023, 7, 11), max_temp=31.0, min_temp=15.0), 
        Row(location="260_T_a", date=date(2023, 7, 12), max_temp=31.0, min_temp=15.0), 
        Row(location="260_T_a", date=date(2023, 7, 13), max_temp=26.0, min_temp=15.0),
        Row(location="260_T_a", date=date(2023, 7, 14), max_temp=26.0, min_temp=15.0),
    ]
    
    df_input = spark.createDataFrame(data)

    result_df = calculate_extreme_weather(
        df_input,
        base_threshold=25.0,
        base_col="max_temp",
        intensity_threshold=30.0,
        intensity_col="max_temp",
        intensity_count=3,
        duration_days=5,
        operator="ge"
    )

    results = result_df.collect()

    assert len(results) == 1
    assert results[0]["from_date"] == date(2023, 7, 1)
    assert results[0]["duration_days"] == 6
    assert results[0]["intense_days"] == 3

def test_calculate_extreme_weather_fails_duration(spark):
    """
    Tests that a period of extreme heat is excluded if it 
    does not meet the minimum consecutive day requirement.
    """
    # Setup: Only 4 days (fails the 5-day requirement)
    data = [
        Row(location="260_T_a", date=date(2023, 7, 1), max_temp=35.0, min_temp=15.0),
        Row(location="260_T_a", date=date(2023, 7, 2), max_temp=35.0, min_temp=15.0),
        Row(location="260_T_a", date=date(2023, 7, 3), max_temp=35.0, min_temp=15.0),
        Row(location="260_T_a", date=date(2023, 7, 4), max_temp=35.0, min_temp=15.0),
    ]
    df = spark.createDataFrame(data)

    result = calculate_extreme_weather(
        df, 
        base_threshold=25.0, base_col="max_temp",
        intensity_threshold=30.0, intensity_col="max_temp"
    )

    assert result.count() == 0

def test_calculate_extreme_weather_coldwave(spark):
    """
    Tests that a coldwave is correctly identified based on 
    minimum temperature thresholds and consecutive sub-zero days.
    """
    # Setup: 5 days max < 0, 3 days min < -10
    data = [
        Row(location="260_T_a", date=date(2023, 1, 1), max_temp=-1.0, min_temp=-11.0),
        Row(location="260_T_a", date=date(2023, 1, 2), max_temp=-2.0, min_temp=-12.0),
        Row(location="260_T_a", date=date(2023, 1, 3), max_temp=-0.5, min_temp=-13.0),
        Row(location="260_T_a", date=date(2023, 1, 4), max_temp=-1.0, min_temp=-5.0),
        Row(location="260_T_a", date=date(2023, 1, 5), max_temp=-1.0, min_temp=-5.0),
    ]
    df = spark.createDataFrame(data)

    result = calculate_extreme_weather(
        df,
        base_threshold=0.0,
        base_col="max_temp",
        intensity_threshold=-10.0,
        intensity_col="min_temp",
        operator="lt"
    )

    assert result.count() == 1
    assert result.first()["duration_days"] == 5
    assert result.first()["intense_days"] == 3