import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, udf, unix_timestamp
from pyspark.sql.types import StringType, DoubleType, TimestampType, LongType
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
    parser.add_argument("--topic", default="uber_stream_dec")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--starting-offsets", default="latest")
    parser.add_argument("--silver-output", required=True)
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--seen-ids-path", required=True,
                         help="Path to the trip_id tracking file used to prevent duplicates")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("UberStreamETL").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .load()
    )

    lines = raw.selectExpr("CAST(value AS STRING) as line")
    fields = split(lines["line"], ",")

    parsed = lines.select(
        fields.getItem(0).alias("trip_id"),
        fields.getItem(1).cast(DoubleType()).alias("start_lat"),
        fields.getItem(2).cast(DoubleType()).alias("start_lon"),
        fields.getItem(3).cast(DoubleType()).alias("end_lat"),
        fields.getItem(4).cast(DoubleType()).alias("end_lon"),
        fields.getItem(5).cast(TimestampType()).alias("start_time"),
        fields.getItem(6).cast(TimestampType()).alias("end_time"),
        fields.getItem(7).cast(DoubleType()).alias("distance"),
    )

    valid = parsed.filter(
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

        # Use the original Spark session defined above
        # instead of batch_df.sparkSession because this property
        # is not available in PySpark 3.1.2
        hadoop_conf = spark._jsc.hadoopConfiguration()
        Path = spark._jvm.org.apache.hadoop.fs.Path
        FileSystem = spark._jvm.org.apache.hadoop.fs.FileSystem
        fs = FileSystem.get(hadoop_conf)
        seen_path = Path(args.seen_ids_path)

        # Get the trip_ids that have already been seen
        if fs.exists(seen_path):
            seen_df = spark.read.text(args.seen_ids_path)
            seen_ids = set(row["value"] for row in seen_df.collect())
        else:
            seen_ids = set()

        # Filter: keep only rows whose trip_id does not exist yet
        batch_pd_ids = [row["trip_id"] for row in batch_df.select("trip_id").collect()]
        new_ids = [tid for tid in batch_pd_ids if tid not in seen_ids]

        if not new_ids:
            print(f"[batch {batch_id}] All rows are duplicates, nothing new to write")
            return

        new_ids_set = set(new_ids)
        deduped_df = batch_df.filter(col("trip_id").isin(list(new_ids_set)))

        # Write new records to Silver
        (
            deduped_df.write
            .mode("append")
            .option("header", "true")
            .csv(args.silver_output)
        )

        # Update the seen_ids file by adding the new IDs
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