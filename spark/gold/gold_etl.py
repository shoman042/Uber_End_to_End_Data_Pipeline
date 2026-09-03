import argparse
import h3
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, udf, when, hour, minute, to_date, dayofmonth, date_format,
    dayofweek, weekofyear, month, quarter, year, lit
)
from pyspark.sql.types import StringType, DoubleType, IntegerType


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--unified-path", default="/home/student/uber_pipeline/silver/unified_rides")
    parser.add_argument("--gold-base", default="/home/student/uber_pipeline/gold")
    parser.add_argument("--checkpoints-base", default="/home/student/uber_pipeline/checkpoints")
    args = parser.parse_args()

    dim_date_path = f"{args.gold_base}/dim_date"
    dim_time_path = f"{args.gold_base}/dim_time"
    dim_geo_path = f"{args.gold_base}/dim_geography"
    dim_bucket_path = f"{args.gold_base}/dim_duration_bucket"
    fact_path = f"{args.gold_base}/fact_trip"

    seen_trips_path = f"{args.checkpoints_base}/seen_trip_ids_gold"
    seen_dates_path = f"{args.checkpoints_base}/seen_date_keys_gold"
    seen_geo_path = f"{args.checkpoints_base}/seen_geo_hash_gold"

    spark = SparkSession.builder.appName("GoldETLIncremental").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    hadoop_conf = spark._jsc.hadoopConfiguration()
    HPath = spark._jvm.org.apache.hadoop.fs.Path
    FileSystem = spark._jvm.org.apache.hadoop.fs.FileSystem
    fs = FileSystem.get(hadoop_conf)

    def path_exists(p):
        return fs.exists(HPath(p))

    def read_seen_df(path, col_name):
        if path_exists(path):
            return spark.read.text(path).withColumnRenamed("value", col_name)
        return spark.createDataFrame([], f"{col_name} string")

    def append_seen(path, new_keys_df, col_name):
        if new_keys_df.limit(1).count() == 0:
            return
        (
            new_keys_df
            .select(col(col_name).cast("string").alias("value"))
            .coalesce(1)
            .write.mode("append").text(path)
        )

    if not path_exists(dim_time_path):
        print("انشاء dim_time (seed awal mara)...")
        rows = []
        for h in range(24):
            if 6 <= h < 12:
                period = "Morning"
            elif 12 <= h < 17:
                period = "Afternoon"
            elif 17 <= h < 21:
                period = "Evening"
            else:
                period = "Night"
            is_rush = 1 if h in (7, 8, 9, 16, 17, 18, 19) else 0
            rows.append((h, h, 0, period, is_rush))
        spark.createDataFrame(
            rows, ["time_key", "hour", "minute", "period", "is_rush_hour"]
        ).coalesce(1).write.mode("overwrite").option("header", "true").csv(dim_time_path)

    if not path_exists(dim_bucket_path):
        print("insha2 dim_duration_bucket (seed awal mara)...")
        buckets = [
            (1, "0-5 min", 0, 300),
            (2, "5-15 min", 300, 900),
            (3, "15-30 min", 900, 1800),
            (4, "30-60 min", 1800, 3600),
            (5, "60+ min", 3600, 999999999),
        ]
        spark.createDataFrame(
            buckets, ["duration_bucket_key", "bucket_label", "min_seconds", "max_seconds"]
        ).coalesce(1).write.mode("overwrite").option("header", "true").csv(dim_bucket_path)

    seen_trips_df = read_seen_df(seen_trips_path, "trip_id")
    seen_trips_count = seen_trips_df.count()
    print(f"seen trip_id count: {seen_trips_count}")

    unified_df = spark.read.option("header", "true").csv(args.unified_path)
    new_df = unified_df.join(seen_trips_df, on="trip_id", how="left_anti")
    new_df.cache()

    new_count = new_df.count()
    if new_count == 0:
        print("No new trip_id. Gold layer is up to date.")
        spark.stop()
        return

    new_df = new_df.withColumn("start_time", col("start_time").cast("timestamp")) \
                    .withColumn("trip_duration_sec", col("trip_duration_sec").cast("long"))

    enriched_df = (
        new_df
        .withColumn("date_key", date_format(col("start_time"), "yyyyMMdd").cast(IntegerType()))
        .withColumn("time_key", hour(col("start_time")))
        .withColumn("start_geo_key", col("start_geo_hash"))
        .withColumn("end_geo_key", col("end_geo_hash"))
        .withColumn("is_same_cell", when(col("start_geo_hash") == col("end_geo_hash"), 1).otherwise(0))
        .withColumn(
            "duration_bucket_key",
            when(col("trip_duration_sec") < 300, 1)
            .when(col("trip_duration_sec") < 900, 2)
            .when(col("trip_duration_sec") < 1800, 3)
            .when(col("trip_duration_sec") < 3600, 4)
            .otherwise(5)
        )
        .withColumn("trip_count", lit(1))
    )
    enriched_df.cache()

    seen_dates_df = read_seen_df(seen_dates_path, "date_key")

    dates_df = enriched_df.select(
        col("date_key").cast("string").alias("date_key"), col("start_time").alias("_st")
    ).dropDuplicates(["date_key"])

    new_dates_df = dates_df.join(seen_dates_df, on="date_key", how="left_anti")
    new_dates_df.cache()

    if new_dates_df.limit(1).count() > 0:
        dim_date_new = (
            new_dates_df
            .withColumn("date_key", col("date_key").cast(IntegerType()))
            .withColumn("full_date", to_date(col("_st")))
            .withColumn("day", dayofmonth(col("_st")))
            .withColumn("day_name", date_format(col("_st"), "EEEE"))
            .withColumn("day_of_week", dayofweek(col("_st")))
            .withColumn("is_weekend", when(dayofweek(col("_st")).isin(1, 7), 1).otherwise(0))
            .withColumn("week_of_year", weekofyear(col("_st")))
            .withColumn("month", month(col("_st")))
            .withColumn("month_name", date_format(col("_st"), "MMMM"))
            .withColumn("quarter", quarter(col("_st")))
            .withColumn("year", year(col("_st")))
            .select("date_key", "full_date", "day", "day_name", "day_of_week",
                    "is_weekend", "week_of_year", "month", "month_name", "quarter", "year")
        )
        dim_date_new.cache()
        dim_date_new.write.mode("append").option("header", "true").csv(dim_date_path)

        added = dim_date_new.count()
        append_seen(seen_dates_path, dim_date_new.select("date_key"), "date_key")
        print(f"added {added} new rows to dim_date")
        dim_date_new.unpersist()

    new_dates_df.unpersist()

    seen_geo_df = read_seen_df(seen_geo_path, "geo_hash")

    all_geo_df = (
        enriched_df.select(col("start_geo_hash").alias("geo_hash"))
        .union(enriched_df.select(col("end_geo_hash").alias("geo_hash")))
        .dropDuplicates(["geo_hash"])
    )
    new_geo_df = all_geo_df.join(seen_geo_df, on="geo_hash", how="left_anti")
    new_geo_df.cache()

    if new_geo_df.limit(1).count() > 0:
        def get_lat(gh):
            try:
                return h3.cell_to_latlng(gh)[0]
            except Exception:
                return None

        def get_lon(gh):
            try:
                return h3.cell_to_latlng(gh)[1]
            except Exception:
                return None

        def get_parent(gh, res):
            try:
                return h3.cell_to_parent(gh, res)
            except Exception:
                return None

        lat_udf = udf(get_lat, DoubleType())
        lon_udf = udf(get_lon, DoubleType())
        parent6_udf = udf(lambda gh: get_parent(gh, 6), StringType())
        parent5_udf = udf(lambda gh: get_parent(gh, 5), StringType())

        dim_geo_new = (
            new_geo_df
            .withColumn("lat", lat_udf(col("geo_hash")))
            .withColumn("lon", lon_udf(col("geo_hash")))
            .withColumn("parent_geo_res6", parent6_udf(col("geo_hash")))
            .withColumn("parent_geo_res5", parent5_udf(col("geo_hash")))
        )
        dim_geo_new.cache()
        dim_geo_new.write.mode("append").option("header", "true").csv(dim_geo_path)

        added = dim_geo_new.count()
        append_seen(seen_geo_path, dim_geo_new.select("geo_hash"), "geo_hash")
        print(f"added {added} new rows to dim_geography")
        dim_geo_new.unpersist()

    new_geo_df.unpersist()

    fact_df = enriched_df.select(
        "trip_id", "date_key", "time_key", "start_geo_key", "end_geo_key",
        "duration_bucket_key", "start_time", "trip_duration_sec",
        "trip_count", "is_same_cell"
    )
    fact_df.write.mode("append").option("header", "true").csv(fact_path)

    append_seen(seen_trips_path, enriched_df.select("trip_id"), "trip_id")

    print(f"added {new_count} new rows to fact_trip")
    print(f"total tracked trip_id in Gold now: {seen_trips_count + new_count}")

    new_df.unpersist()
    enriched_df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
