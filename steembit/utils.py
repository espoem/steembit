import math
import typing
from datetime import datetime, timedelta, timezone

from beem.blockchain import Blockchain
from beem.comment import Comment


def reputation_to_score(rep):
    """Converts the account reputation value into the reputation score"""
    if isinstance(rep, str):
        rep = int(rep)
    if rep == 0:
        return 25.0
    score = max([math.log10(abs(rep)) - 9, 0])
    if rep < 0:
        score *= -1
    score = (score * 9.0) + 25.0
    return score


def is_paid_out(post: Comment):
    dt = datetime(1970, 1, 1, 0, 0, 0, 0, timezone.utc)
    return post["last_payout"] > dt


def find_block_num_by_timestamp(
    blockchain: Blockchain, low_block_num, high_block_num, key_timestamp
):
    low = low_block_num
    high = high_block_num

    while high >= low:
        mid = low + (high - low) // 2
        mid_block = blockchain.wait_for_and_get_block(mid)
        is_equal_timestamp = (
            mid_block["timestamp"] - timedelta(seconds=5)
            <= key_timestamp
            <= mid_block["timestamp"] + timedelta(seconds=5)
        )
        if is_equal_timestamp:
            return mid
        elif mid_block["timestamp"] < key_timestamp:
            low = mid + 1
        else:
            high = mid - 1

    return None


def remove_duplicates(key: str, iterable: typing.Iterable):
    unique = {}
    for item in iterable:
        unique[item[key]] = item
        # unique.setdefault(item[key], item)
    return unique.values()
