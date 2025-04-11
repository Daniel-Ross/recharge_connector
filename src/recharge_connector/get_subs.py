from .configs import ACTIVE_SUB_URL, BASE_SUB_URL, HEADERS
from tqdm import tqdm
import requests
import json
import time
import polars as pl
from .utils import get_next_url


def pull_active_subs():
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
