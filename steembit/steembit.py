import logging
import math
import time
import typing
from datetime import datetime, timedelta, tzinfo

import beem
import click
from beem.comment import Comment
from beem.discussions import Discussions, Query
from prettytable import PrettyTable

from .constants import LOG_FORMAT, MAX_AGE_HOURS, STM
from .utils import is_paid_out, remove_duplicates

LOGGER = logging.getLogger(__name__)


def split_values_by_comma_callback(ctx, param, value):
    if value:
        return [tag.strip() for tag in value.split(",") if tag.strip()]
    else:
        return []


def is_not_negative_callback(ctx, param, value):
    if value < 0:
        click.echo(f"Parameter {param} can't be negative.")
        ctx.abort()
    return value


def has_all_tags(tags: typing.Iterable[str], post: dict):
    """Check that all tags are included.

    :param tags: Tags that should be included
    :type tags: typing.Iterable[str]
    :param post: A post with tags
    :type post: dict
    """
    for tag in tags:
        if tag not in post.get("tags", []):
            return False
    return True


def is_author(account: str, post: dict) -> bool:
    return account == post.get("author")


def is_authored_by_any(accounts: typing.Iterable[str], post: dict) -> bool:
    """Check that a post is authored by one of selected authors.

    :param accounts: Selected authors
    :type accounts: typing.Iterable[str]
    :param post: A post to check
    :type post: dict
    :return: True if the post has a selected author else false
    :rtype: bool
    """
    return not accounts or post.get("author") in accounts


def is_not_authored_by(accounts: typing.Iterable[str], post: dict) -> bool:
    """Check that a post is not authored by any of selected authors.

    :param accounts: Selected authors
    :type accounts: typing.Iterable[str]
    :param post: A post to check
    :type post: dict
    :return: True if not authored by selected authors else false
    :rtype: bool
    """
    if accounts:
        return not is_authored_by_any(accounts, post)
    return True


@click.group()
@click.option(
    "-t",
    "--tags",
    default="",
    type=click.STRING,
    is_flag=False,
    show_default=True,
    callback=split_values_by_comma_callback,
    help="Tags that must be included. Separate them with comma (,).",
)
@click.option(
    "--all-tags",
    default=False,
    is_flag=True,
    show_default=True,
    help="If set, all selected tags must be included.",
)
@click.option(
    "--authors",
    required=False,
    callback=split_values_by_comma_callback,
    help="Filter results by selected authors. Separate them with comma(,).",
)
@click.option(
    "--wo-authors",
    required=False,
    callback=split_values_by_comma_callback,
    help="Exclude results with selected authors. Separate them with comma(,).",
)
@click.option(
    "--voters",
    required=False,
    type=click.STRING,
    callback=split_values_by_comma_callback,
    help="Keep posts that were upvoted by selected accounts.",
)
@click.option(
    "--wo-voters",
    required=False,
    type=click.STRING,
    callback=split_values_by_comma_callback,
    help="Keep posts that were not upvoted by selected accounts.",
)
@click.option(
    "--limit",
    required=False,
    default=100,
    type=click.INT,
    show_default=True,
    callback=is_not_negative_callback,
    help="Results limit.",
)
@click.option("--verbose", default=3, type=click.INT, help="Verbosity")
@click.pass_context
def cli(ctx, tags, all_tags, authors, wo_authors, voters, wo_voters, limit, verbose):
    # logger
    VERBOSITY = ["critical", "error", "warn", "info", "debug"][int(min(verbose, 4))]
    LOGGER.setLevel(logging.DEBUG)
    FORMATTER = logging.Formatter(LOG_FORMAT)
    SH = logging.StreamHandler()
    SH.setLevel(getattr(logging, VERBOSITY.upper()))
    SH.setFormatter(FORMATTER)
    LOGGER.addHandler(SH)
    LOGGER.info("Starting script")

    # pass input vars to context
    ctx.ensure_object(dict)
    ctx.obj = {
        "TAGS": tags,
        "VOTERS": voters,
        "VOTERS_EXCLUDED": wo_voters,
        "LIMIT": limit,
        "AUTHORS": authors,
        "AUTHORS_EXCLUDED": wo_authors,
    }
    LOGGER.debug("Input params")
    LOGGER.debug(ctx.obj)

    results = []
    q_limit = 100 if limit <= 100 else math.ceil(limit * 1.25)

    if tags:
        for tag in tags:
            q = Query(tag=tag)
            discussions = Discussions(steem_instance=STM).get_discussions(
                discussion_type="created", discussion_query=q, limit=q_limit
            )
            results += [
                d
                for d in discussions
                if has_all_tags(tags, d)
                and is_authored_by_any(authors, d)
                and is_not_authored_by(wo_authors, d)
                and is_voted_by_any(voters, d)
                and is_not_voted_by_any(wo_voters, d)
            ]

    if authors:
        for author in authors:
            q = Query(tag=author)
            discussions = Discussions(steem_instance=STM).get_discussions(
                discussion_type="blog", discussion_query=q, limit=q_limit
            )
            results += [
                d
                for d in discussions
                if has_all_tags(tags, d)
                and is_authored_by_any(authors, d)
                and is_not_authored_by(wo_authors, d)
                and is_voted_by_any(voters, d)
                and is_not_voted_by_any(wo_voters, d)
            ]
    results = list(remove_duplicates("authorperm", results))
    results.sort(key=lambda x: x["created"], reverse=True)
    results = results[:limit]
    LOGGER.info("Found %d items to process", len(results))

    # pass computed vars to context
    ctx.obj["RESULTS"] = results


@cli.command()
@click.option(
    "--mode",
    type=click.Choice(["table"]),
    default="table",
    show_default=True,
    help="Defines the displayed output of the results.",
)
@click.option(
    "--number/--no-number",
    is_flag=True,
    default=False,
    show_default=True,
    help="Include number of the item.",
)
@click.option(
    "--author/--no-author",
    is_flag=True,
    default=True,
    show_default=True,
    help="Include author's",
)
@click.option(
    "--link/--no-link",
    is_flag=True,
    default=True,
    show_default=True,
    help="Include discussion link",
)
@click.option(
    "--created/--no-created",
    is_flag=True,
    default=True,
    show_default=True,
    help="Include time of creation",
)
@click.option(
    "--tags/--no-tags",
    is_flag=True,
    default=False,
    show_default=True,
    help="Include tags",
)
@click.option(
    "--comments/--no-comments",
    is_flag=True,
    default=False,
    show_default=True,
    help="Include number of comments",
)
@click.option(
    "--votes/--no-votes",
    is_flag=True,
    default=False,
    show_default=True,
    help="Include number of votes",
)
@click.option(
    "--rewards/--no-rewards",
    is_flag=True,
    default=True,
    show_default=True,
    help="Include total rewards",
)
@click.pass_context
def print_results(
    ctx, mode, number, author, link, created, tags, comments, votes, rewards
):
    """Print the results.

    :param ctx: Click context
    :type ctx: click.Context
    """
    results = ctx.obj["RESULTS"]  # type: typing.Collection[Comment]

    pad = len(str(len(results)))
    for idx, result in enumerate(results, 1):
        LOGGER.debug(f'{idx:0{pad}}::Created {result["created"]}::{result["url"]}')

    if mode == "table":
        table = _init_discussions_table()
        _build_discussions_table(results, table)
        fields = _build_fields_selector(
            number, author, link, created, tags, comments, votes, rewards
        )

        click.echo(table.get_string(fields=fields))


def _build_discussions_table(results, table):
    for idx, r in enumerate(results, 1):
        row = _build_discussion_table_row(r, idx)
        table.add_row(row)


def _build_fields_selector(
    number, author, link, created, tags, comments, votes, rewards
):
    fields = []
    if number:
        fields.append("Number")
    if author:
        fields.append("Author")
    if link:
        fields.append("Link")
    if created:
        fields.append("Created At")
    if tags:
        fields.append("Tags")
    if comments:
        fields.append("Comments")
    if votes:
        fields.append("Votes")
    if rewards:
        fields.append("Rewards")
    return fields


def _init_discussions_table() -> PrettyTable:
    table = PrettyTable()
    field_names = []
    field_names.append("Number")
    field_names.append("Author")
    field_names.append("Link")
    field_names.append("Created At")
    field_names.append("Tags")
    field_names.append("Comments")
    field_names.append("Votes")
    field_names.append("Rewards")
    table.field_names = field_names
    return table


def _build_discussion_table_row(discussion: dict, idx: int) -> list:
    DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
    row = []
    row.append(idx)
    row.append(discussion["author"])
    row.append(f'https://steemit.com{discussion["url"]}')
    row.append(discussion["created"].strftime(DATETIME_FORMAT))
    row.append(",".join(discussion.get("tags", [])))
    row.append(len(discussion.get_all_replies()))
    row.append(len(discussion.get_votes()))
    row.append(str(discussion.get_rewards().get("total_payout")))
    return row


@cli.command()
@click.option(
    "--weight",
    required=False,
    type=click.FloatRange(min=0.01, max=100, clamp=True),
    help="Fixed vote weight for all accounts. Range from 0.01 to 100.",
)
@click.option(
    "--uniform",
    required=False,
    type=click.FloatRange(min=0.01, max=5000, clamp=True),
    help="Results will be voted with uniform weight based on the number of items in results using specified total vote weight. Range from 0.01 to 5000",
)
@click.option(
    "--accounts",
    required=True,
    type=click.STRING,
    callback=split_values_by_comma_callback,
    help="Accounts with permissions to broadcast transactions.",
)
@click.option(
    "-f", "--force", required=False, is_flag=True, help="Force voting to all results."
)
@click.option(
    "--min-age",
    default=0.25,
    required=False,
    type=click.FloatRange(min=0, max=MAX_AGE_HOURS, clamp=True),
    show_default=True,
    help="Minimum age of a post or comment in hours.",
)
@click.option(
    "--max-age",
    default=6 * 24,
    required=False,
    type=click.FloatRange(min=0, max=MAX_AGE_HOURS, clamp=True),
    show_default=True,
    help="Maximum age of a post or comment in hours.",
)
@click.pass_context
def vote(ctx, weight, uniform, accounts, force, min_age, max_age):
    """Vote fetched posts and comments.

    :param ctx: Click context
    :type ctx: click.Context
    :param weight: Fixed weight of votes
    :type weight: float
    :param uniform: Maximum weight that will be distributed amongst all items
    :type uniform: float
    :param accounts: Accounts to vote with
    :type accounts: list
    :param force: A flag to override previous votes
    :type force: bool
    """

    if max_age < min_age:
        click.echo(
            f"Min age ({min_age} hours) can't be higher than max age ({max_age} hours)."
        )
        ctx.abort()

    if weight and uniform:
        LOGGER.error("You can specify only fixed weight or uniform weight.")
        ctx.abort()

    if not weight and not uniform:
        LOGGER.error("You did not specify a fixed weight or uniform weight.")
        ctx.abort()

    results = ctx.obj["RESULTS"]
    results = [r for r in results if not is_paid_out(r)]
    LOGGER.info("%d not paid out", len(results))
    results = [
        r
        for r in results
        if timedelta(hours=min_age) < r.time_elapsed() < timedelta(hours=max_age)
    ]
    LOGGER.info("%d are in valid voting time range.", len(results))

    if not weight:
        try:
            weight = min([max([uniform / len(results), 0.01]), 100])
        except ZeroDivisionError:
            weight = 1

    for result in results:
        voted = False
        for account in accounts:
            if is_voted_by_any([account], result) and not force:
                LOGGER.info("Already voted by %s. %s", account, result["url"])
                continue
            voted = vote_discussion(result, account, weight, 3)
        if voted:
            time.sleep(3)


def vote_discussion(
    discussion: Comment, voter: str, weight: float, retry_count: int = None
) -> bool:
    """Vote a discussion (post, comment) with selected account and vote weight.

    :param discussion: Post or comment
    :type discussion: beem.comment.Comment
    :param voter: Voter
    :type voter: str
    :param weight: Vote weight
    :type weight: float
    :param retry_count: A number of tries
    :type retry_count: int
    :return: True if vote was successful else False
    :rtype: bool
    """
    if not retry_count:
        retry_count = 0

    if retry_count < 0:
        return False

    try:
        retry_count -= 1
        discussion.upvote(weight, voter)
    except beem.exceptions.VotingInvalidOnArchivedPost:
        LOGGER.info("Invalid post, can't vote. %s", discussion["url"])
        return False
    except:
        LOGGER.exception("Error during upvoting with %s. %s", voter, discussion["url"])
        return vote_discussion(discussion, voter, weight, retry_count - 1)
    else:
        LOGGER.info(
            "Upvote with account %s at weight %s%%. %s",
            voter,
            weight,
            discussion["url"],
        )
    return True


def is_voted_by_any(voters: typing.Collection, discussion: Comment) -> bool:
    """Check if a post (comment) was voted by any of selected accounts.

    :param voters: A collection of voters
    :type voters: typing.Collection
    :param discussion: Post or comment
    :type discussion: Comment
    :return: True if any of the accounts voted else False
    :rtype: bool
    """
    votes = discussion.get_votes()
    for account in voters:
        if account in votes:
            return True
    return not voters


def is_not_voted_by_any(voters: typing.Collection, discussion: Comment) -> bool:
    """Checks if a post (comment) was not voted by any of the selected accounts.

    :param voters: A collection of voters
    :type voters: typing.Collection
    :param discussion: Post or comment
    :type discussion: Comment
    :return: True if none of the accounts voted else False
    :rtype: bool
    """
    if voters:
        return not is_voted_by_any(voters, discussion)
    return True
