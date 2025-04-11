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
    while True:
        yield