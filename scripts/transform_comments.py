import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, to_timestamp, coalesce, lit

def process_comments_data(execution_date):
    spark = SparkSession.builder \
        .appName("HackerNewsCommentsProcessing") \
        .getOrCreate()

    raw_path = f"/opt/airflow/data-lake/raw/comments/dt={execution_date}/comments.json"
    processed_path = f"/opt/airflow/data-lake/processed/comments/dt={execution_date}"

    print(f"Reading raw comments from: {raw_path}")

    try:
        df_raw = spark.read.option("multiLine", "true").json(raw_path)

        df_cleaned = df_raw.select(
            col("id").cast("long").alias("comment_id"),
            col("parent").cast("long").alias("parent_id"),
            col("by").alias("author"),
            col("text").alias("comment_text"),
            col("type"),
            to_timestamp(from_unixtime(col("time"))).alias("created_at")
        ).filter(col("id").isNotNull())

        df_final = df_cleaned.dropDuplicates(["comment_id"])

        df_final.write \
            .mode("overwrite") \
            .parquet(processed_path)

        print(f"Successfully saved clean comments Parquet to: {processed_path}")

    except Exception as e:
        print(f"Error processing comments for date {execution_date}: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2026-09-04"
    process_comments_data(target_date)