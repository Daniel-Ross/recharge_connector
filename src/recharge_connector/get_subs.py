from recharge_connector.configs import ACTIVE_SUB_URL, BASE_SUB_URL, CANCELLED_SUB_URL, HEADERS
from tqdm import tqdm
import requests
import json
import time
import polars as pl
from recharge_connector.utils import get_next_url, create_sub_df


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
    sub_frame = create_sub_df(all_subs)
    return sub_frame


def pull_cancelled_subs(start_date: str = "", end_date: str = "") -> pl.DataFrame:
    """
    Pulls cancelled subscriptions data from the Recharge API and returns it as a Polars DataFrame.
    This function fetches all cancelled subscriptions within the specified date range (if provided).
    The data is retrieved through paginated API requests and combined into a single DataFrame.

    Parameters
    ----------
    start_date : str, optional
        The minimum creation date for subscriptions to include in the format 'YYYY-MM-DD'.
        If not provided, no lower date boundary will be applied.
    end_date : str, optional
        The maximum creation date for subscriptions to include in the format 'YYYY-MM-DD'.
        If not provided, no upper date boundary will be applied.

    Returns
    -------
    pl.DataFrame
        A Polars DataFrame containing the cancelled subscription data with processed columns.

    Notes
    -----
    - The function uses a progress bar to track API request progress.
    - Requests are rate-limited with 0.5 second delays between calls.
    - Both start_date and end_date need to be provided to apply date filtering.
    """
    all_subs = []
    if start_date and end_date:
        url = CANCELLED_SUB_URL + f"&created_at_min={start_date}&created_at_max={end_date}"
    else:
        url = CANCELLED_SUB_URL
    print(url)
    progress = tqdm()
    while True:
        progress.update()
        response = requests.get(url, HEADERS)
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
    sub_frame = create_sub_df(all_subs)
    return sub_frame


if __name__ == "__main__":
    subs = pull_cancelled_subs()
    print("Done")
