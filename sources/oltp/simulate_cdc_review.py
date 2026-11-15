"""Emit review update, insert and hard delete on an isolated CDC source."""

from datetime import datetime, timezone
from uuid import uuid4

import psycopg

from sources.oltp.load_olist import connection_settings, database_settings


def main() -> None:
    settings = database_settings()
    if not settings["POSTGRES_DB"].endswith("_1c_test"):
        raise ValueError("CDC mutation fixture requires an isolated *_1c_test database")
    connection_args = connection_settings(settings)
    now = datetime.now(timezone.utc)
    with psycopg.connect(**connection_args) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT review_row_id, review_score, order_id FROM order_reviews ORDER BY review_row_id LIMIT 1")
            row = cursor.fetchone()
            if row is None:
                raise ValueError("Load the isolated Olist source before CDC mutation proof")
            existing_id, score, order_id = row
            new_score = 4 if score == 5 else 5
            cursor.execute("UPDATE order_reviews SET review_score = %s, updated_at = %s WHERE review_row_id = %s",
                           (new_score, now, existing_id))
    with psycopg.connect(**connection_args) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO order_reviews (review_id, order_id, review_score, review_creation_date, "
                "review_answer_timestamp, created_at, updated_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING review_row_id",
                (uuid4().hex, order_id, 3, now, now, now, now),
            )
            inserted_id = cursor.fetchone()[0]
    with psycopg.connect(**connection_args) as connection:
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM order_reviews WHERE review_row_id = %s", (inserted_id,))
    print(f"Committed review update {existing_id}, insert and hard delete {inserted_id}")


if __name__ == "__main__":
    main()
