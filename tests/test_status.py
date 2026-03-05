import datetime

import pytz
from telethon.tl import types

from telesaver import infer_online_status, relative_to_now, status_rank


def test_infer_online_sets_last_online_to_now():
    before = pytz.utc.localize(datetime.datetime.utcnow()) - datetime.timedelta(
        seconds=1
    )
    status, last_online = infer_online_status(
        types.UserStatusOnline(expires=datetime.datetime.utcnow()),
        previous_status=None,
        previous_last_online=None,
    )
    after = pytz.utc.localize(datetime.datetime.utcnow()) + datetime.timedelta(
        seconds=1
    )
    assert status == "online"
    assert isinstance(last_online, datetime.datetime)
    assert before <= last_online <= after


def test_infer_recently_from_week_sets_now():
    previous_last_online = pytz.utc.localize(datetime.datetime(2020, 1, 1))
    status, last_online = infer_online_status(
        types.UserStatusRecently(),
        previous_status="within a week",
        previous_last_online=previous_last_online,
    )
    assert status == "recently"
    assert isinstance(last_online, datetime.datetime)
    assert last_online > previous_last_online


def test_infer_week_clamps_to_week_ago():
    old = pytz.utc.localize(datetime.datetime.utcnow()) - datetime.timedelta(days=1)
    before = pytz.utc.localize(datetime.datetime.utcnow()) - datetime.timedelta(
        days=7, seconds=1
    )
    status, last_online = infer_online_status(
        types.UserStatusLastWeek(),
        previous_status="recently",
        previous_last_online=old,
    )
    after = (
        pytz.utc.localize(datetime.datetime.utcnow())
        - datetime.timedelta(days=7)
        + datetime.timedelta(seconds=1)
    )
    assert status == "within a week"
    assert isinstance(last_online, datetime.datetime)
    assert before <= last_online <= after


def test_infer_offline_uses_was_online():
    was_online = pytz.utc.localize(datetime.datetime(2025, 12, 31, 12, 0, 0))
    status, last_online = infer_online_status(
        types.UserStatusOffline(was_online=was_online),
        previous_status="recently",
        previous_last_online=None,
    )
    assert status == "offline"
    assert last_online == was_online


def test_infer_last_month_with_very_old_previous_becomes_long_time_ago():
    old = pytz.utc.localize(datetime.datetime.utcnow()) - datetime.timedelta(days=90)
    status, _ = infer_online_status(
        types.UserStatusLastMonth(),
        previous_status="within a month",
        previous_last_online=old,
    )
    assert status == "long time ago"


def test_relative_to_now_minutes():
    now = pytz.utc.localize(datetime.datetime(2026, 1, 1, 12, 0, 0))
    value = now - datetime.timedelta(minutes=10)
    assert relative_to_now(value, now=now) == "10 minutes ago"


def test_relative_to_now_weeks():
    now = pytz.utc.localize(datetime.datetime(2026, 1, 22, 12, 0, 0))
    value = now - datetime.timedelta(days=21)
    assert relative_to_now(value, now=now) == "3 weeks ago"


def test_status_rank_priority():
    assert status_rank("recently") < status_rank("within a week")
    assert status_rank("within a week") < status_rank("within a month")
    assert status_rank("within a month") < status_rank("long time ago")
