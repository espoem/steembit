import json
import logging
import os
from datetime import datetime

from beem import Steem

# logging
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
logging.basicConfig(
    filename=os.path.join(os.path.dirname(__file__), "steembit.log"),
    level=logging.DEBUG,
    format=LOG_FORMAT
)


# config
with open(os.path.join(os.path.dirname(__file__), "config.json"), "r") as f:
    config = json.load(f)

# Date and time
TODAY = datetime.date(datetime.now())
DATETIME_FORMATS = ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S%z"]

# Post age range
MIN_AGE_HOURS = 0
MAX_AGE_HOURS = 7 * 24

# Steem
POSTING_KEY = ""
ACCOUNT = config["account"]

STM = Steem(
    node=[
        "https://rpc.buildteam.io",
        "https://api.steemit.com",
        "https://api.steem.house",
        "https://steemd-appbase.steemit.com",
        "https://steemd.privex.io",
        "https://steemd.minnowsupportproject.org",
    ],
    keys=[config["posting_key"]],
    timeout=15,
)
