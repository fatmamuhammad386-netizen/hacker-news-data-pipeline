import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def run_pipeline_analytics(execution_date):
    spark = SparkSession.builder \
        .appName("HackerNewsAnalytics") \
        .getOrCreate()

    stories_path = f"/opt/airflow/data-lake/processed/stories/dt={execution_date}"
    comments_path = f"/opt/airflow/data-lake/processed/comments/dt={execution_date}"
    analytics_output_path = f"/opt/airflow/data-lake/analytics/dt={execution_date}"

    print(f"=== Starting Analytics for Date: {execution_date} ===")

    try:
        df_stories = spark.read.parquet(stories_path)
        print(f"Loaded Stories: {df_stories.count()} records")
    except Exception as e:
        print(f"No stories found for date {execution_date}: {e}")
        df_stories = None

    try:
        df_comments = spark.read.parquet(comments_path)
        print(f"Loaded Comments: {df_comments.count()} records")
    except Exception as e:
        print(f"No comments found for date {execution_date}: {e}")
        df_comments = None

    if df_stories:
        # --- [1] Top Stories by Score ---
        top_scored = df_stories.orderBy(F.desc("score")).select("story_id", "author", "title", "score", "comments_count")
        top_scored.write.mode("overwrite").parquet(f"{analytics_output_path}/top_stories_by_score_parquet")
        top_scored.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{analytics_output_path}/top_stories_by_score_csv")

        # --- [2] Domain Analytics ---
        df_domains = df_stories.withColumn(
            "domain", 
            F.regexp_extract(F.col("url"), r'https?://(?:www\.)?([^/]+)', 1)
        ).filter(F.col("domain") != "")

        domain_analytics = df_domains.groupBy("domain") \
            .agg(
                F.count("story_id").alias("total_articles"),
                F.sum("score").alias("total_score"),
                F.round(F.avg("score"), 2).alias("avg_score_per_article"),
                F.round(F.avg("comments_count"), 2).alias("avg_comments_per_article"),
                F.round(F.sum(F.col("score") + F.col("comments_count")) / F.count("story_id"), 2).alias("engagement_index")
            ) \
            .orderBy(F.desc("engagement_index"))

        domain_analytics.write.mode("overwrite").parquet(f"{analytics_output_path}/domain_analytics_parquet")
        domain_analytics.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{analytics_output_path}/domain_analytics_csv")

        # --- [3] Author Analytics ---
        author_window = Window.partitionBy("author").orderBy(F.desc("score"))
        df_ranked_stories = df_stories.withColumn("rank_per_author", F.row_number().over(author_window))
        
        author_top_perf = df_ranked_stories.filter(F.col("rank_per_author") == 1) \
            .select(
                F.col("author"),
                F.col("title").alias("best_story_title"),
                F.col("score").alias("best_story_score")
            )

        author_totals = df_stories.groupBy("author") \
            .agg(
                F.count("story_id").alias("total_posts"),
                F.sum("score").alias("author_total_score"),
                F.round(F.avg("score"), 2).alias("author_avg_score")
            )

        final_author_analytics = author_totals.join(author_top_perf, "author") \
            .orderBy(F.desc("author_total_score"))

        final_author_analytics.write.mode("overwrite").parquet(f"{analytics_output_path}/author_analytics_parquet")
        final_author_analytics.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{analytics_output_path}/author_analytics_csv")

        # --- [4] Keywords Analytics ---
        stop_words = [
            "to", "a", "the", "in", "of", "for", "and", "on", "is", "with", "at", 
            "by", "from", "it", "an", "how", "why", "show", "hn:", "you", "your", 
            "its", "can", "what", "are", "this", "that", "be", "as", "or", "using"
        ]

        df_words = df_stories.select(
            F.explode(F.split(F.lower(F.col("title")), r'\s+')).alias("word")
        ).withColumn("cleaned_word", F.trim(F.regexp_extract(F.col("word"), r'[a-zA-Z0-9+#]+', 0)))

        top_keywords = df_words.filter(
            (F.col("cleaned_word") != "") & 
            (~F.col("cleaned_word").isin(stop_words)) & 
            (F.col("cleaned_word").rlike(r'^.{3,}$'))
        ).groupBy("cleaned_word") \
         .agg(F.count("*").alias("keyword_frequency")) \
         .orderBy(F.desc("keyword_frequency"))

        top_keywords.write.mode("overwrite").parquet(f"{analytics_output_path}/keywords_analytics_parquet")
        top_keywords.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{analytics_output_path}/keywords_analytics_csv")

        # --- [5] Stories Trend By Hour ---
        if "time" in df_stories.columns:
            df_time = df_stories.withColumn("created_timestamp", F.from_unixtime(F.col("time")))
            stories_by_hour = df_time.withColumn("hour", F.hour(F.col("created_timestamp"))) \
                .groupBy("hour") \
                .agg(
                    F.count("story_id").alias("stories_count"),
                    F.round(F.avg("score"), 2).alias("avg_score"),
                    F.round(F.avg("comments_count"), 2).alias("avg_comments")
                ) \
                .orderBy("hour")

            stories_by_hour.write.mode("overwrite").parquet(f"{analytics_output_path}/stories_by_hour_parquet")
            stories_by_hour.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{analytics_output_path}/stories_by_hour_csv")

        # --- [6] Domain Scatter ---
        domain_scatter = df_domains.groupBy("domain") \
            .agg(
                F.count("story_id").alias("total_stories"),
                F.sum("score").alias("total_score"),
                F.sum("comments_count").alias("total_comments"),
                F.round(F.avg("score"), 2).alias("avg_score_per_story"),
                F.round(F.avg("comments_count"), 2).alias("avg_comments_per_story")
            ) \
            .filter(F.col("total_stories") >= 2) \
            .orderBy(F.desc("total_score"))

        domain_scatter.write.mode("overwrite").parquet(f"{analytics_output_path}/domain_scatter_parquet")
        domain_scatter.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{analytics_output_path}/domain_scatter_csv")

    if df_stories and df_comments:
        # --- [7] Most Discussed Stories ---
        comments_summary = df_comments.groupBy("parent_id") \
            .agg(F.count("comment_id").alias("actual_comments_fetched"))

        discussed_stories = df_stories.join(
            comments_summary, 
            df_stories.story_id == comments_summary.parent_id, 
            "inner"
        ).select("story_id", "title", "author", "actual_comments_fetched") \
         .orderBy(F.desc("actual_comments_fetched"))

        discussed_stories.write.mode("overwrite").parquet(f"{analytics_output_path}/most_discussed_stories_parquet")
        discussed_stories.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{analytics_output_path}/most_discussed_stories_csv")

    print(f"\n=== Analytics Completed Successfully! Saved to {analytics_output_path} ===")
    spark.stop()

if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2026-09-06"
    run_pipeline_analytics(target_date)
    