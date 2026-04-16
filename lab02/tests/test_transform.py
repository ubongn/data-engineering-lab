import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(ROOT))

from transform import build_silver, build_gold


def write_parquet(output_path: Path, records: list[dict]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(records).to_parquet(output_path, engine="pyarrow", index=False)


def test_join_produces_correct_columns(tmp_path: Path) -> None:
    bronze_dir = tmp_path / "bronze"
    silver_path = tmp_path / "silver" / "enriched_posts.parquet"

    write_parquet(bronze_dir / "users.parquet", [
        {
            "id": 1,
            "name": "Alice",
            "email": "alice@example.com",
            "company": {"name": "Acme"},
        }
    ])
    write_parquet(bronze_dir / "posts.parquet", [
        {
            "id": 10,
            "userId": 1,
            "title": "Hello world",
            "body": "First post body",
        }
    ])
    write_parquet(bronze_dir / "comments.parquet", [
        {"postId": 10, "id": 100, "body": "Nice post!"}
    ])

    build_silver(str(bronze_dir), str(silver_path))
    df = pd.read_parquet(silver_path)

    assert set(df.columns) == {
        "post_id",
        "user_name",
        "user_email",
        "company_name",
        "title",
        "title_word_count",
        "body_word_count",
        "comment_count",
        "user_post_rank",
    }
    assert df.loc[0, "post_id"] == 10
    assert df.loc[0, "user_name"] == "Alice"
    assert df.loc[0, "user_email"] == "alice@example.com"
    assert df.loc[0, "company_name"] == "Acme"


def test_word_count_calculation_is_correct(tmp_path: Path) -> None:
    bronze_dir = tmp_path / "bronze"
    silver_path = tmp_path / "silver" / "enriched_posts.parquet"

    write_parquet(bronze_dir / "users.parquet", [
        {
            "id": 1,
            "name": "Bob",
            "email": "bob@example.com",
            "company": {"name": "Beta"},
        }
    ])
    write_parquet(bronze_dir / "posts.parquet", [
        {
            "id": 20,
            "userId": 1,
            "title": "Quick test",
            "body": "One two three four",
        }
    ])
    write_parquet(bronze_dir / "comments.parquet", [
        {"postId": 20, "id": 200, "body": "Good work"}
    ])

    build_silver(str(bronze_dir), str(silver_path))
    df = pd.read_parquet(silver_path)

    assert int(df.loc[0, "title_word_count"]) == 2
    assert int(df.loc[0, "body_word_count"]) == 4


def test_gold_summary_is_computed_correctly(tmp_path: Path) -> None:
    bronze_dir = tmp_path / "bronze"
    silver_path = tmp_path / "silver" / "enriched_posts.parquet"
    gold_path = tmp_path / "gold" / "user_summary.parquet"

    write_parquet(bronze_dir / "users.parquet", [
        {
            "id": 1,
            "name": "Carol",
            "email": "carol@example.com",
            "company": {"name": "Gamma"},
        }
    ])
    write_parquet(bronze_dir / "posts.parquet", [
        {
            "id": 30,
            "userId": 1,
            "title": "Short body",
            "body": "One two",
        },
        {
            "id": 31,
            "userId": 1,
            "title": "Longer body",
            "body": "One two three four five",
        }
    ])
    write_parquet(bronze_dir / "comments.parquet", [
        {"postId": 30, "id": 300, "body": "OK"},
        {"postId": 31, "id": 301, "body": "Nice one"},
    ])

    build_silver(str(bronze_dir), str(silver_path))
    build_gold(str(silver_path), str(gold_path))
    df = pd.read_parquet(gold_path).sort_values("user_name")

    assert set(df.columns) == {
        "user_name",
        "total_posts",
        "total_comments_received",
        "avg_body_length",
    }
    row = df.loc[df["user_name"] == "Carol"].iloc[0]
    assert row["total_posts"] == 2
    assert row["total_comments_received"] == 2
    assert abs(row["avg_body_length"] - 3.5) < 1e-6


def test_ranking_is_correct_within_group(tmp_path: Path) -> None:
    bronze_dir = tmp_path / "bronze"
    silver_path = tmp_path / "silver" / "enriched_posts.parquet"

    write_parquet(bronze_dir / "users.parquet", [
        {
            "id": 1,
            "name": "Carol",
            "email": "carol@example.com",
            "company": {"name": "Gamma"},
        }
    ])
    write_parquet(bronze_dir / "posts.parquet", [
        {
            "id": 30,
            "userId": 1,
            "title": "Short body",
            "body": "One two",
        },
        {
            "id": 31,
            "userId": 1,
            "title": "Longer body",
            "body": "One two three four five",
        }
    ])
    write_parquet(bronze_dir / "comments.parquet", [
        {"postId": 30, "id": 300, "body": "OK"},
        {"postId": 31, "id": 301, "body": "Nice one"},
    ])

    build_silver(str(bronze_dir), str(silver_path))
    df = pd.read_parquet(silver_path).sort_values("post_id")

    ranks = df.set_index("post_id")["user_post_rank"].to_dict()
    assert ranks[30] == 2
    assert ranks[31] == 1
