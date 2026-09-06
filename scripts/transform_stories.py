import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, to_timestamp, coalesce, lit

def process_hacker_news_data(execution_date):
    # 1. إنشاء Spark Session
    spark = SparkSession.builder \
        .appName("HackerNewsDataProcessing") \
        .getOrCreate()

    raw_path = f"/opt/airflow/data-lake/raw/stories/dt={execution_date}/stories.json"
    processed_path = f"/opt/airflow/data-lake/processed/stories/dt={execution_date}"

    print(f"Reading raw data from: {raw_path}")

    try:
        df_raw = spark.read.option("multiLine", "true").json(raw_path)

        df_cleaned = df_raw.select(
            col("id").cast("long").alias("story_id"),
            col("by").alias("author"),
            col("title"),
            col("url"),
            col("type"),
            coalesce(col("score"), lit(0)).cast("integer").alias("score"),
            coalesce(col("descendants"), lit(0)).cast("integer").alias("comments_count"),
            to_timestamp(from_unixtime(col("time"))).alias("created_at")
        ).filter(col("id").isNotNull())

        df_final = df_cleaned.dropDuplicates(["story_id"])

        print(f"Processed {df_final.count()} records successfully.")

        df_final.write \
            .mode("overwrite") \
            .parquet(processed_path)

        print(f"Successfully saved clean Parquet data to: {processed_path}")

    except Exception as e:
        print(f"Error processing data for date {execution_date}: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2026-09-03"
    process_hacker_news_data(target_date)