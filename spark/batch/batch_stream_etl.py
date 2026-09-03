import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, unix_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, LongType
)
import h3

RESOLUTION = 8

def geo_hash_udf():
    def _hash(lat, lon):
        if lat is None or lon is None:
            return None
        try:
            return h3.latlng_to_cell(lat, lon, RESOLUTION)
        except Exception:
            return None
    return udf(_hash, StringType())

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-path", default="/home/student/uber_pipeline/bronze/batch",
                         help="HDFS directory where NiFi puts new batch files")
    parser.add_argument("--silver-output", default="/home/student/uber_pipeline/silver/batch_output")
    parser.add_argument("--checkpoint", default="/home/student/uber_pipeline/checkpoints/batch_etl",
                         help="Checkpoint specific to this query (tracks read offsets/progress)")
    parser.add_argument("--seen-ids-path", default="/home/student/uber_pipeline/checkpoints/seen_trip_ids",
                         help="Same shared seen_ids file used by stream_etl.py for unified deduplication")
    parser.add_argument("--max-files-per-trigger", type=int, default=5,
                         help="Maximum number of files to read in each micro-batch")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("UberBatchToSilverETL").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Explicit schema required for any CSV source in Structured Streaming (no inferSchema here)
    schema = StructType([
        StructField("trip_id", StringType(), True),
        StructField("start_lat", DoubleType(), True),
        StructField("start_lon", DoubleType(), True),
        StructField("end_lat", DoubleType(), True),
        StructField("end_lon", DoubleType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("distance", DoubleType(), True),
    ])

    raw = (
        spark.readStream
        .format("csv")
        .option("header", "true")
        .option("maxFilesPerTrigger", args.max_files_per_trigger)
        .schema(schema)
        .load(args.bronze_path)
    )

    valid = raw.filter(
        col("end_time") > col("start_time")
    ).filter(
        col("start_lat").isNotNull() & col("start_lon").isNotNull() &
        col("end_lat").isNotNull() & col("end_lon").isNotNull()
    )

    hash_fn = geo_hash_udf()
    with_geo = valid.withColumn(
        "start_geo_hash", hash_fn(col("start_lat"), col("start_lon"))
    ).withColumn(
        "end_geo_hash", hash_fn(col("end_lat"), col("end_lon"))
    )

    result = with_geo.withColumn(
        "trip_duration_sec",
        (unix_timestamp(col("end_time")) - unix_timestamp(col("start_time"))).cast(LongType())
    ).select(
        "trip_id", "start_time", "start_geo_hash", "end_geo_hash", "trip_duration_sec"
    )

    def write_batch_deduped(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return

        hadoop_conf = spark._jsc.hadoopConfiguration()
        Path = spark._jvm.org.apache.hadoop.fs.Path
        FileSystem = spark._jvm.org.apache.hadoop.fs.FileSystem
        fs = FileSystem.get(hadoop_conf)
        seen_path = Path(args.seen_ids_path)

        if fs.exists(seen_path):
            seen_df = spark.read.text(args.seen_ids_path)
            seen_ids = set(row["value"] for row in seen_df.collect())
        else:
            seen_ids = set()

        batch_pd_ids = [row["trip_id"] for row in batch_df.select("trip_id").collect()]
        new_ids = [tid for tid in batch_pd_ids if tid not in seen_ids]

        if not new_ids:
            print(f"[batch {batch_id}] All rows are duplicates (already exist in the shared seen_ids), nothing new to write")
            return

        new_ids_set = set(new_ids)
        deduped_df = batch_df.filter(col("trip_id").isin(list(new_ids_set)))

        (
            deduped_df.write
            .mode("append")
            .option("header", "true")
            .csv(args.silver_output)
        )

        updated_seen = seen_ids.union(new_ids_set)
        spark.createDataFrame(
            [(tid,) for tid in updated_seen], ["value"]
        ).coalesce(1).write.mode("overwrite").text(args.seen_ids_path)

        print(f"[batch {batch_id}] Wrote {deduped_df.count()} new rows, "
              f"filtered {len(batch_pd_ids) - len(new_ids)} duplicate rows")

    query = (
        result.writeStream
        .foreachBatch(write_batch_deduped)
        .option("checkpointLocation", args.checkpoint)
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()