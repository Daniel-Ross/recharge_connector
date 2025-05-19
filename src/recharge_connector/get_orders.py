from recharge_connector.configs import (
    BASE_ORDER_URL,
    HEADERS,
    ORDERS_PROCESSED_BY_ID_URL,
)
from tqdm import tqdm
import requests
import json
import time
import polars as pl
from recharge_connector.utils import get_next_url, create_order_df


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
    # print(url)
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
            url = get_next_url(data, BASE_ORDER_URL)
            if not url:
                break
        time.sleep(0.5)
    order_frame = create_order_df(all_orders)
    return order_frame


def pull_all_orders():
    url = BASE_ORDER_URL
    # print(url)
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
            url = get_next_url(data, BASE_ORDER_URL)
            if not url:
                break
        time.sleep(0.5)
    order_frame = create_order_df(all_orders)
    return order_frame


if __name__ == "__main__":
    data = pull_all_orders()
    print("Done")
