import polars as pl


def get_next_url(response_data, url):
    """Create the next url when results are paginated

    Args:
        response_data (json): json array of data

    Returns:
        str: next url of results
    """
    next_url = None
    next_cursor = response_data.get("next_cursor")
    if next_cursor is not None:
        next_url = f"{url}?limit=250&page_info={next_cursor}"
    return next_url


def progress_generator():
    """Generator function for infinite progress updates.

    Used with tqdm to provide progress bar updates during API pagination.
    Each yield signals one page of data has been processed.

    Yields:
        None: Signals completion of one pagination step
    """
    while True:
        yield


def create_order_df(order_list: list[dict]) -> pl.DataFrame:
    order_frame = pl.from_dicts(order_list, infer_schema_length=10000)
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


def create_sub_df(sub_list: list[dict]) -> pl.DataFrame:
    """Transform raw subscription data into a structured Polars DataFrame.

    This function processes a list of subscription dictionaries from the Recharge API
    and transforms them into a clean, well-structured Polars DataFrame by:
    - Unnesting external_variant_id and external_product_id fields
    - Converting Shopify variant and product IDs to proper numeric types
    - Converting price values to float
    - Removing unnecessary fields that add noise to the data

    Args:
        sub_list (list[dict]): List of subscription dictionaries retrieved from the Recharge API

    Returns:
        pl.DataFrame: A cleaned and structured Polars DataFrame containing subscription data
    """
    sub_frame = pl.from_dicts(sub_list, infer_schema_length=10000)
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
