import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def build_silver(bronze_dir: str, silver_path: str) -> int:

    # Read bronze
    users = pd.read_parquet(f"{bronze_dir}/users.parquet")
    posts = pd.read_parquet(f"{bronze_dir}/posts.parquet")
    comments = pd.read_parquet(f"{bronze_dir}/comments.parquet")

    # Flatten nested company name
    if "company" in users.columns:
        users["company_name"] = users["company"].apply(
            lambda x: x["name"] if isinstance(x, dict) else None
        )

    # Join posts ← users
    df = posts.merge(
        users[["id", "name", "email", "company_name"]],
        left_on="userId", right_on="id", suffixes=("", "_user")
    ).rename(columns={"name": "user_name", "email": "user_email"})

    # Aggregate comments per post
    comment_counts = comments.groupby("postId").size().reset_index(name="comment_count")
    df = df.merge(comment_counts, left_on="id", right_on="postId", how="left")
    df["comment_count"] = df["comment_count"].fillna(0).astype(int)

    # Compute metrics
    df["title_word_count"] = df["title"].str.split().str.len()
    df["body_word_count"] = df["body"].str.split().str.len()

    # Rank within user by body length
    df["user_post_rank"] = df.groupby("userId")["body_word_count"].rank(
        ascending=False, method="dense"
    ).astype(int)

    # Filter
    df = df[df["title"].notna() & (df["title"] != "")]
    df = df[df["body"].notna() & (df["body"] != "")]

    # Select final columns
    result = df[[
        "id", "user_name", "user_email", "company_name",
        "title", "title_word_count", "body_word_count",
        "comment_count", "user_post_rank"
    ]].rename(columns={"id": "post_id"})

    # Memory optimization
    result["user_name"] = result["user_name"].astype("category")
    if "company_name" in result.columns:
        result["company_name"] = result["company_name"].astype("category")

    # Data quality checks
    required_fields = [
        "post_id", "user_name", "user_email", "title",
        "title_word_count", "body_word_count", "comment_count", "user_post_rank",
    ]
    null_rows = result[required_fields].isnull().any(axis=1)
    if null_rows.any():
        raise ValueError(
            f"Data quality check failed: null values found in required fields for rows {result[null_rows].index.tolist()}"
        )
    if not (result["comment_count"] >= 0).all():
        raise ValueError("Data quality check failed: comment_count must be >= 0")

    Path(silver_path).parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(silver_path, engine="pyarrow", compression="snappy", index=False)
    logger.info(f"{silver_path} - {len(result)} rows")
    return len(result)


def build_gold(silver_path: str, gold_path: str) -> int:
    silver = pd.read_parquet(silver_path)
    summary = silver.groupby("user_name", observed=True, as_index=False).agg(
        total_posts=("post_id", "count"),
        total_comments_received=("comment_count", "sum"),
        avg_body_length=("body_word_count", "mean"),
    )
    Path(gold_path).parent.mkdir(parents=True, exist_ok=True)
    summary.to_parquet(gold_path, engine="pyarrow", compression="snappy", index=False)
    logger.info(f"{gold_path} - {len(summary)} rows")
    return len(summary)

