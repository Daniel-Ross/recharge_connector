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
ACTIVE_SUB_URL = BASE_SUB_URL + ACTIVE_SUB_PRED

