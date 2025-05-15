from recharge_connector.configs import (
    ACTIVE_SUB_URL,
    BASE_SUB_URL,
    HEADERS,
    ORDERS_PROCESSED_BY_ID_URL,
)
from tqdm import tqdm
import requests
import json
import time
import polars as pl
from recharge_connector.utils import get_next_url


def pull_active_subs() -> pl.DataFrame:
    """Retrieves and processes active subscription data from the Recharge API.
    This function fetches all active subscriptions using pagination, processes the raw data,
    and returns it in a structured Polars DataFrame format. It handles the API pagination,
    includes rate limiting protection, and transforms the subscription data by:
    - Converting variant and product IDs to proper numeric types
    - Casting price to float
    - Removing unnecessary fields
    - Unnesting nested JSON structures
    Raises:
        requests.exceptions.RequestException: If there is an error making the API request
        json.JSONDecodeError: If the API response cannot be parsed as JSON
    Returns:
        pl.DataFrame: A Polars DataFrame containing processed subscription data
    Note:
        The function uses pagination to retrieve all active subscriptions and includes
        a 0.5 second delay between API calls to prevent rate limiting.
    """
    all_subs = []
    url = ACTIVE_SUB_URL
    progress = tqdm()
    while True:
        progress.update()
        response = requests.get(url, headers=HEADERS)
        data = json.loads(response.text)
        for sub in data.get("subscriptions", []):
            all_subs.append(sub)
        if not data.get("next_cursor"):
            break
        else:
            url = get_next_url(data, BASE_SUB_URL)
            if not url:
                break
        time.sleep(0.5)
    sub_frame = pl.from_dicts(all_subs, infer_schema_length=10000)
    sub_frame = sub_frame.with_columns(sub_frame.unnest("external_variant_id"))
    sub_frame = sub_frame.rename({"ecommerce": "external_variant_id_processed"})
    sub_frame = sub_frame.with_columns(sub_frame.unnest("external_product_id"))
    sub_frame = sub_frame.rename({"ecommerce": "external_product_id_processed"})
    sub_frame = sub_frame.with_columns(
        pl.col("external_variant_id_processed").cast(pl.Int64),
        pl.col("external_product_id_processed").cast(pl.Int64),
        pl.col("price").cast(pl.Float64),
    )
    sub_frame = sub_frame.drop(
        [
            "address_id",
            "analytics_data",
            "cancellation_reason",
            "cancellation_reason_comments",
            "cancelled_at",
            "has_queued_charges",
            "is_prepaid",
            "is_swappable",
            "max_retries_reached",
            "order_day_of_month",
            "order_day_of_week",
            "presentment_currency",
            "sku_override",
        ]
    )

    return sub_frame


def pull_orders_by_ids(ids: list) -> pl.DataFrame:
    """Retrieves and processes order data for specified order IDs from the Recharge API.

    This function fetches order data for a list of order IDs, processes the raw data,
    and returns it in a structured Polars DataFrame format. It handles API pagination,
    includes rate limiting protection, and transforms the order data by:
    - Exploding the line_items array to create one row per line item
    - Extracting nested customer and line item information into flat columns
    - Converting identifiers to proper types
    - Removing unnecessary fields

    Args:
        ids (list): List of order IDs to retrieve from the Recharge API

    Raises:
        requests.exceptions.RequestException: If there is an error making the API request
        json.JSONDecodeError: If the API response cannot be parsed as JSON

    Returns:
        pl.DataFrame: A Polars DataFrame containing processed order data with each row
                     representing a line item in an order

    Note:
        The function uses pagination to retrieve all orders and includes
        a 0.5 second delay between API calls to prevent rate limiting.
    """

    url = ORDERS_PROCESSED_BY_ID_URL + ",".join(ids)
    print(url)
    all_orders = []
    progress = tqdm()
    while True:
        progress.update()
        response = requests.get(url, headers=HEADERS)
        data = json.loads(response.text)
        for order in data.get("orders", []):
            all_orders.append(order)
        if not data.get("next_cursor"):
            break
        else:
            url = get_next_url(data, BASE_SUB_URL)
            if not url:
                break
        time.sleep(0.5)
    order_frame = pl.from_dicts(all_orders, infer_schema_length=10000)
    order_frame = order_frame.explode("line_items")
    order_frame = order_frame.with_columns(
        recharge_customer_id=pl.col("customer").struct.field("id"),
        shopify_customer_id=pl.col("customer")
        .struct.field("external_customer_id")
        .struct.field("ecommerce"),
        customer_email=pl.col("customer").struct.field("email"),
        shopify_order_name=pl.col("external_order_name").struct.field("ecommerce"),
        shopify_order_id=pl.col("external_order_id").struct.field("ecommerce"),
        shopify_product_id=pl.col("line_items")
        .struct.field("external_product_id")
        .struct.field("ecommerce"),
        shopify_variant_id=pl.col("line_items")
        .struct.field("external_variant_id")
        .struct.field("ecommerce"),
        subscription_id=pl.col("line_items").struct.field("purchase_item_id"),
        line_item_properties=pl.col("line_items").struct.field("properties"),
        sku=pl.col("line_items").struct.field("sku"),
        qty=pl.col("line_items").struct.field("quantity"),
        product_title=pl.col("line_items").struct.field("title"),
        charge_id=pl.col("charge").struct.field("id"),
    )

    # order_frame = order_frame.unnest("line_item_properties")
    order_frame = order_frame.drop(
        [
            "billing_address",
            "currency",
            "total_weight_grams",
            "shipping_address",
            "shipping_lines",
            "client_details",
            "customer",
            "tax_lines",
            "line_items",
            "is_prepaid",
            "external_order_number",
            "external_order_name",
            "external_order_id",
            "external_cart_token",
            "charge",
        ]
    )
    return order_frame
