

import argparse
from pyspark.sql import SparkSession


def export_single_csv(spark, source_path, target_path, table_name):
   
    print(f"[export] start {table_name} ...")
    df = spark.read.option("header", "true").csv(source_path)
    row_count = df.count()

    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(target_path)

    print(f"[export] export done {table_name} — : {row_count}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", required=True,
                         help="the important root HDFS، ex: /home/student/uber_pipeline")
    args = parser.parse_args()

    project_root = args.project_root.rstrip("/")

    spark = SparkSession.builder.appName("FinalExport").getOrCreate()

    # ============ Silver ============
    export_single_csv(
        spark,
        source_path=f"{project_root}/silver/unified_rides",
        target_path=f"{project_root}/export/silver_unified_rides",
        table_name="silver_unified_rides",
    )

    # ============ Gold: Dims ============
    export_single_csv(
        spark,
        source_path=f"{project_root}/gold/dim_date",
        target_path=f"{project_root}/export/gold_dim_date",
        table_name="gold_dim_date",
    )

    export_single_csv(
        spark,
        source_path=f"{project_root}/gold/dim_time",
        target_path=f"{project_root}/export/gold_dim_time",
        table_name="gold_dim_time",
    )

    export_single_csv(
        spark,
        source_path=f"{project_root}/gold/dim_geography",
        target_path=f"{project_root}/export/gold_dim_geography",
        table_name="gold_dim_geography",
    )

    export_single_csv(
        spark,
        source_path=f"{project_root}/gold/dim_duration_bucket",
        target_path=f"{project_root}/export/gold_dim_duration_bucket",
        table_name="gold_dim_duration_bucket",
    )

    # ============ Gold: Fact ============
    export_single_csv(
        spark,
        source_path=f"{project_root}/gold/fact_trip",
        target_path=f"{project_root}/export/gold_fact_trip",
        table_name="gold_fact_trip",
    )

    print("\n[export] all files done:")
    print(f"  {project_root}/export/")

    spark.stop()


if __name__ == "__main__":
    main()
