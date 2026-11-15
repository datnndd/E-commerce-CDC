"""Seed one coherent row per OLTP table in a disposable local CDC database."""

from datetime import datetime, timezone

import psycopg

from sources.oltp.load_olist import connection_settings, database_settings


TABLES = (
    "customers", "geolocation", "product_category_name_translation", "products",
    "sellers", "orders", "order_items", "order_payments", "order_reviews",
)


def main() -> None:
    settings = database_settings()
    if settings["POSTGRES_DB"] != "ecommerce_cdc_small_1c_test":
        raise ValueError("The small CDC seed requires ecommerce_cdc_small_1c_test")
    observed = datetime(2018, 1, 15, tzinfo=timezone.utc)
    with psycopg.connect(**connection_settings(settings)) as connection:
        with connection.cursor() as cursor:
            for table in TABLES:
                cursor.execute(f"SELECT 1 FROM {table} LIMIT 1")
                if cursor.fetchone():
                    raise ValueError(f"{table} is not empty; refusing to reseed")
            cursor.execute(
                "INSERT INTO customers (customer_id, customer_unique_id, customer_zip_code_prefix, "
                "customer_city, customer_state, created_at, updated_at) "
                "VALUES ('cdc_customer_1', 'cdc_unique_1', '01001', 'sao paulo', 'SP', %s, %s)",
                (observed, observed),
            )
            cursor.execute(
                "INSERT INTO geolocation (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, "
                "geolocation_city, geolocation_state, created_at, updated_at) "
                "VALUES ('01001', -23.55, -46.63, 'sao paulo', 'SP', %s, %s)",
                (observed, observed),
            )
            cursor.execute(
                "INSERT INTO product_category_name_translation "
                "(product_category_name, product_category_name_english, created_at, updated_at) "
                "VALUES ('beleza_saude', 'health_beauty', %s, %s)",
                (observed, observed),
            )
            cursor.execute(
                "INSERT INTO products (product_id, product_category_name, current_price, created_at, updated_at) "
                "VALUES ('cdc_product_1', 'beleza_saude', 100.00, %s, %s)",
                (observed, observed),
            )
            cursor.execute(
                "INSERT INTO sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state, "
                "created_at, updated_at) VALUES ('cdc_seller_1', '01001', 'sao paulo', 'SP', %s, %s)",
                (observed, observed),
            )
            cursor.execute(
                "INSERT INTO orders (order_id, customer_id, order_status, order_purchase_timestamp, "
                "order_estimated_delivery_date, created_at, updated_at) "
                "VALUES ('cdc_order_1', 'cdc_customer_1', 'delivered', %s, %s, %s, %s)",
                (observed, observed, observed, observed),
            )
            cursor.execute(
                "INSERT INTO order_items (order_id, order_item_id, product_id, seller_id, shipping_limit_date, "
                "price, freight_value, created_at, updated_at) "
                "VALUES ('cdc_order_1', 1, 'cdc_product_1', 'cdc_seller_1', %s, 100.00, 10.00, %s, %s)",
                (observed, observed, observed),
            )
            cursor.execute(
                "INSERT INTO order_payments (order_id, payment_sequential, payment_type, payment_installments, "
                "payment_value, created_at, updated_at) "
                "VALUES ('cdc_order_1', 1, 'credit_card', 1, 110.00, %s, %s)",
                (observed, observed),
            )
            cursor.execute(
                "INSERT INTO order_reviews (review_id, order_id, review_score, review_creation_date, "
                "review_answer_timestamp, created_at, updated_at) "
                "VALUES ('cdc_review_1', 'cdc_order_1', 3, %s, %s, %s, %s)",
                (observed, observed, observed, observed),
            )
    print(f"Seeded {len(TABLES)} OLTP tables with one related row each")


if __name__ == "__main__":
    main()
