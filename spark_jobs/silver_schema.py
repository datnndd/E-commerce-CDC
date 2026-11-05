"""Silver table names and keys shared by Spark and lightweight orchestration."""

OLTP_NAMES = {
    "customers": "slv_customers",
    "geolocation": "slv_geolocation",
    "product_category_name_translation": "slv_category_translation",
    "products": "slv_products",
    "sellers": "slv_sellers",
    "orders": "slv_orders",
    "order_items": "slv_order_items",
    "order_payments": "slv_payments",
    "order_reviews": "slv_reviews",
}
SILVER_NAMES = {**OLTP_NAMES, "clickstream": "slv_clickstream_events",
                "exchange_rate": "slv_exchange_rates", "campaign": "slv_campaigns"}
KEYS = {
    "customers": ["customer_id"],
    "geolocation": ["geolocation_id"],
    "product_category_name_translation": ["product_category_name"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "order_payments": ["order_id", "payment_sequential"],
    "order_reviews": ["review_row_id"],
    "clickstream": ["event_id"],
    "exchange_rate": ["date", "base", "quote"],
    "campaign": ["campaign_id"],
}
