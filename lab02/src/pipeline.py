import argparse
import logging
from pathlib import Path

from src.extract import extract_users, extract_comments, extract_posts
from src.load import write_parquet
from src.transform import build_silver, build_gold


logging.basicConfig(level=logging.INFO, format="%(message)s")

def ensure_bronze_files(bronze_dir: Path) -> None:
    required = ["users.parquet", "posts.parquet", "comments.parquet"]
    missing = [str(bronze_dir / name) for name in required if not (bronze_dir / name).exists()]
    if missing:
        raise FileNotFoundError(
            "Cannot skip extract because bronze files are missing: " + ", ".join(missing)
        )


def main(skip_extract: bool = False) -> None:
    print("=== API Ingestion Pipeline ===\n")

    print("[Extract]")
    if skip_extract:
        print("  Skipping extract; reusing existing Bronze files\n")
        ensure_bronze_files(Path("output/bronze"))
    else:
        users = extract_users()
        print(f"  Users:    {len(users)} records fetched")
        posts = extract_posts()
        print(f"  Posts:    {len(posts)} records fetched")
        comments = extract_comments()
        print(f"  Comments: {len(comments)} records fetched\n")

        print("[Bronze]")
        write_parquet(users, "output/bronze/users.parquet")
        write_parquet(posts, "output/bronze/posts.parquet")
        write_parquet(comments, "output/bronze/comments.parquet")

    print("\n[Silver]")
    silver_path = "output/silver/enriched_posts.parquet"
    row_count = build_silver("output/bronze", silver_path)
    print(f"  {silver_path} — {row_count} rows")
    print("  Columns: post_id, user_name, user_email, company_name, title,")
    print("           title_word_count, body_word_count, comment_count, user_post_rank\n")

    print("[Gold]")
    gold_path = "output/gold/user_summary.parquet"
    gold_count = build_gold(silver_path, gold_path)
    print(f"  {gold_path} — {gold_count} rows")
    print("  Columns: user_name, total_posts, total_comments_received, avg_body_length\n")

    print("Pipeline complete.")


if __name__ == "__main__":
    main()