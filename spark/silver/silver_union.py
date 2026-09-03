import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-path", default="/home/student/uber_pipeline/silver/batch_output",
                         help="Silver path for batch")
    parser.add_argument("--stream-path", default="/home/student/uber_pipeline/silver/stream_output",
                         help="Silver path for streaming")
    parser.add_argument("--unified-output", default="/home/student/uber_pipeline/silver/unified_rides",
                         help="Output path for unified Silver (append only for new records)")
    parser.add_argument("--seen-ids-path", default="/home/student/uber_pipeline/checkpoints/seen_trip_ids_unified",
                         help="Tracking file for trip_ids already moved to unified Silver")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("SilverUnionIncremental").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    hadoop_conf = spark._jsc.hadoopConfiguration()
    Path = spark._jvm.org.apache.hadoop.fs.Path
    FileSystem = spark._jvm.org.apache.hadoop.fs.FileSystem
    fs = FileSystem.get(hadoop_conf)

    # ---------- 1) Read the tracking file for trip_ids already moved ----------
    seen_path = Path(args.seen_ids_path)
    if fs.exists(seen_path):
        seen_df = spark.read.text(args.seen_ids_path)
        seen_ids = set(row["value"] for row in seen_df.collect())
    else:
        seen_ids = set()

    print(f"Previously moved trip_ids to unified Silver: {len(seen_ids)}")

    # ---------- 2) Read batch_output and stream_output (normal batch, not streaming) ----------
    batch_path = Path(args.batch_path)
    stream_path = Path(args.stream_path)

    dfs = []
    if fs.exists(batch_path):
        batch_df = spark.read.option("header", "true").csv(args.batch_path)
        dfs.append(batch_df)
    else:
        print(f"Warning: path {args.batch_path} does not exist, it will be skipped.")

    if fs.exists(stream_path):
        stream_df = spark.read.option("header", "true").csv(args.stream_path)
        dfs.append(stream_df)
    else:
        print(f"Warning: path {args.stream_path} does not exist, it will be skipped.")

    if not dfs:
        print("No data source is available (neither batch nor stream). Exiting without execution.")
        spark.stop()
        return

    # ---------- 3) Union between the two sources (unionByName ensures columns match by name) ----------
    combined_df = dfs[0]
    for df in dfs[1:]:
        combined_df = combined_df.unionByName(df)

    # ---------- 4) Filter: take only trip_ids that have not been moved yet ----------
    new_rows_df = combined_df.filter(~col("trip_id").isin(list(seen_ids))) if seen_ids else combined_df

    # Safety net: remove any possible internal duplicates between batch and stream in the same run
    new_rows_df = new_rows_df.dropDuplicates(["trip_id"])

    new_count = new_rows_df.count()
    if new_count == 0:
        print("No new trip_ids to move. Unified Silver is already up to date.")
        spark.stop()
        return

    # ---------- 5) Write (append only) to unified Silver ----------
    (
        new_rows_df.write
        .mode("append")
        .option("header", "true")
        .csv(args.unified_output)
    )

    # ---------- 6) Update the seen_ids tracking file ----------
    new_ids = [row["trip_id"] for row in new_rows_df.select("trip_id").collect()]
    updated_seen = seen_ids.union(set(new_ids))

    spark.createDataFrame(
        [(tid,) for tid in updated_seen], ["value"]
    ).coalesce(1).write.mode("overwrite").text(args.seen_ids_path)

    print(f"Moved {new_count} new rows to {args.unified_output}")
    print(f"Total tracked trip_ids now: {len(updated_seen)}")

    spark.stop()

if __name__ == "__main__":
    main()