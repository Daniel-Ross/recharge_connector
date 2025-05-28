from dotenv import dotenv_values

CONFIGS = dotenv_values()
STAGING_TOKEN = CONFIGS["staging_api_token"]
PROD_TOKEN = CONFIGS["prod_api_token"]
HEADERS = {
    "X-Recharge-Access-Token": PROD_TOKEN,
    "X-Recharge-Version": "2021-11",
    "Accept": "application/json",
}
BASE_URL = "https://api.rechargeapps.com"
BASE_SUB_URL = BASE_URL + "/subscriptions"
ACTIVE_SUB_PRED = "?limit=250&status=active"
CANCELLED_SUB_PRED = "?limit=250&status=cancelled"
ACTIVE_SUB_URL = BASE_SUB_URL + ACTIVE_SUB_PRED
CANCELLED_SUB_URL = BASE_SUB_URL + CANCELLED_SUB_PRED
BASE_ORDER_URL = BASE_URL + "/orders"
ALL_PROCESSED_ORDERS_URL = BASE_ORDER_URL + "?limit=250&status=success"
ORDERS_PROCESSED_BY_ID_URL = ALL_PROCESSED_ORDERS_URL + "&ids="
