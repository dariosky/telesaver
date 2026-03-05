#!/usr/bin/env python
import argparse
import calendar
import datetime
import logging
import os
import shutil
import tempfile
import time

import emoji
import pytz
from telethon import TelegramClient, utils
from telethon.client import DownloadMethods
from telethon.tl import types
from telethon.tl.types import (
    MessageMediaContact,
    MessageMediaGeo,
    MessageMediaWebPage,
    PeerUser,
)

from sqlitestorage import Store
from util import file_hash, slugify
from utils.formatting import comprint
from watcher import filter_event, wait_for_updates

logger = logging.getLogger(__name__)
title = "TeleSave"
api_id = os.environ.get("TELEGRAM_API_ID")
api_hash = os.environ.get("TELEGRAM_API_HASH")

DONT_SAVE_MEDIA_TYPES = (MessageMediaWebPage, MessageMediaGeo)
WITHIN_A_WEEK = "within a week"
WITHIN_A_MONTH = "within a month"
LONG_TIME_AGO = "long time ago"


def utcnow():
    return pytz.utc.localize(datetime.datetime.utcnow())


def ensure_utc(value):
    if not value:
        return None
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            return pytz.utc.localize(value)
        return value.astimezone(datetime.timezone.utc)
    return value


def one_month_ago(now):
    now = ensure_utc(now) or utcnow()
    year = now.year
    month = now.month - 1
    if month == 0:
        month = 12
        year -= 1
    max_day = calendar.monthrange(year, month)[1]
    day = min(now.day, max_day)
    return now.replace(year=year, month=month, day=day)


def normalize_presence_status(status, last_online, now=None):
    if status not in {"recently", WITHIN_A_WEEK, WITHIN_A_MONTH}:
        return status
    last_online = ensure_utc(last_online)
    if not last_online:
        return status
    now = ensure_utc(now) or utcnow()
    if last_online < one_month_ago(now):
        return LONG_TIME_AGO
    return status


def status_rank(status):
    rank = {
        "recently": 1,
        WITHIN_A_WEEK: 2,
        WITHIN_A_MONTH: 3,
        LONG_TIME_AGO: 4,
    }
    return rank.get(status or "", 99)


def clamp_fuzzy_last_online(previous_last_online, min_timestamp):
    previous_last_online = ensure_utc(previous_last_online)
    if not previous_last_online:
        return min_timestamp
    return max(previous_last_online, min_timestamp)


def relative_to_now(value, now=None):
    value = ensure_utc(value)
    if not value:
        return ""
    now = ensure_utc(now) or utcnow()
    delta = now - value
    seconds = max(0, int(delta.total_seconds()))

    if seconds < 60:
        return "just now"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
    hours = minutes // 60
    if hours < 24:
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    days = hours // 24
    if days < 7:
        return f"{days} day{'s' if days != 1 else ''} ago"
    weeks = days // 7
    if weeks < 5:
        return f"{weeks} week{'s' if weeks != 1 else ''} ago"
    months = days // 30
    if months < 12:
        return f"{months} month{'s' if months != 1 else ''} ago"
    years = days // 365
    return f"{years} year{'s' if years != 1 else ''} ago"


def print_status_table(rows):
    headers = ["name", "telegram_online_status", "last_update"]
    now = utcnow()

    normalized = []
    for row in rows:
        status = normalize_presence_status(
            row.get("telegram_online_status"),
            row.get("last_online"),
            now=now,
        )
        normalized.append(
            {
                **row,
                "telegram_online_status": status,
                "last_update": relative_to_now(row.get("last_online"), now=now),
            }
        )

    def sort_key(row):
        last_online = ensure_utc(row.get("last_online"))
        timestamp = last_online.timestamp() if last_online else float("-inf")
        return (
            -timestamp,
            status_rank(row.get("telegram_online_status")),
            row.get("name") or "",
        )

    normalized.sort(key=sort_key)

    data = [
        [
            str(row.get("name") or ""),
            str(row.get("telegram_online_status") or ""),
            row.get("last_update") or "",
        ]
        for row in normalized
    ]
    widths = [
        max(len(headers[i]), *(len(line[i]) for line in data))
        if data
        else len(headers[i])
        for i in range(len(headers))
    ]

    def fmt_line(columns):
        return " | ".join(f"{columns[i]:<{widths[i]}}" for i in range(len(columns)))

    separator = "-+-".join("-" * w for w in widths)
    print(fmt_line(headers))
    print(separator)
    for line in data:
        print(fmt_line(line))


def infer_online_status(status, previous_status=None, previous_last_online=None):
    now = utcnow()
    previous_last_online = ensure_utc(previous_last_online)

    if isinstance(status, types.UserStatusOnline):
        new_status, new_last_online = "online", now
    elif isinstance(status, types.UserStatusOffline):
        new_status, new_last_online = "offline", ensure_utc(status.was_online)
    elif isinstance(status, types.UserStatusRecently):
        recent_bound = now - datetime.timedelta(days=3)
        new_status, new_last_online = "recently", clamp_fuzzy_last_online(
            previous_last_online, min_timestamp=recent_bound
        )
    elif isinstance(status, types.UserStatusLastWeek):
        week_bound = now - datetime.timedelta(days=7)
        new_status, new_last_online = WITHIN_A_WEEK, clamp_fuzzy_last_online(
            previous_last_online, min_timestamp=week_bound
        )
    elif isinstance(status, types.UserStatusLastMonth):
        month_bound = one_month_ago(now)
        new_status, new_last_online = WITHIN_A_MONTH, clamp_fuzzy_last_online(
            previous_last_online, min_timestamp=month_bound
        )
    elif isinstance(status, types.UserStatusEmpty):
        new_status, new_last_online = "empty", previous_last_online
    else:
        new_status, new_last_online = None, None

    new_status = normalize_presence_status(new_status, new_last_online, now=now)
    return new_status, new_last_online


def get_media_name(message):
    media = message.media
    date = message.date
    group = message.grouped_id
    possible_names = []
    media_id = None
    if isinstance(media, types.MessageMediaWebPage):
        if isinstance(media.webpage, types.WebPage):
            media = media.webpage.document or media.webpage.photo
    if isinstance(media, types.MessageMediaDocument):
        media = media.document
    if isinstance(media, (types.MessageMediaPhoto, types.Photo)):
        if media.photo is None or message.file is None:
            return
        kind = "photo"
        extension = message.file.ext
        media_id = media.photo.id
    elif isinstance(media, types.MessageMediaContact):
        kind = "contact"
        extension = ".vcard"
        possible_names = [f"{media.first_name}{extension}"]
    elif isinstance(
        media,
        (
            types.MessageMediaDocument,
            types.Document,
            types.WebDocument,
            types.WebDocumentNoProxy,
        ),
    ):
        kind, possible_names = DownloadMethods._get_kind_and_names(media.attributes)
        extension = utils.get_extension(media)
    elif isinstance(media, types.MessageMediaGeoLive):
        logger.warning(f"Skipping media type: {type(media)}")
        return
    else:
        if media is None:
            logger.debug("Expired message is gone")
        else:
            logger.error(f"Unknown media type: {type(media)}")
        return

    name_tokens = []
    if possible_names:
        document_name = possible_names[0]
        file_name, extension = os.path.splitext(document_name)
        name_tokens.append(f"{file_name}-")
    else:
        name_tokens.append(f"{kind}_")
    name_tokens.append(
        f"{date.year:02}-{date.month:02}-{date.day:02}"
        f"_{date.hour:02}-{date.minute:02}-{date.second:02}"
    )
    if group:
        if media_id:
            name_tokens.append(f"_{media_id}")
        else:
            logger.warning(f"Got a group {group} but unknown media_id")
    name_tokens.append(extension)
    return "".join(name_tokens)


def copy_in_folder(src, dst):
    dst_folder = os.path.dirname(dst)
    if not os.path.isdir(dst_folder):
        os.makedirs(dst_folder)
    shutil.copy2(src, dst)


class DialogSaver:
    def __init__(self, client, store, save_self_destructing=False) -> None:
        super().__init__()
        self.client = client
        self.store = store
        self.changed = False
        self.save_self_destructing = save_self_destructing
        self.scanned_messages = 0

        # we cache the known messages for the latest requested dialog
        self._known_dialog_id = None
        self._known = None

    def save_dialog(self, dialog_id, dialog_name):
        logger.debug(f"Saving dialog {dialog_id} as {dialog_name}")
        known_dialogs = self.store.dialog_names
        if (
            dialog_id not in known_dialogs
            or known_dialogs[dialog_id]["name"] != dialog_name
        ):
            self.store.add_dialog(
                dialog_id,
                dialog_name,
                folder=self.get_folder_name(dialog_id, dialog_name),
            )

    def get_folder_name(self, dialog_id, dialog_name=None):
        known_dialogs = self.store.dialog_names
        if dialog_id in known_dialogs:
            return known_dialogs[dialog_id]["folder"]
        assert dialog_name is not None, "Unknown dialog, give me its name first"
        logger.info(f"Adding new dialog info: {dialog_name}")
        folder_name = slugify(dialog_name)
        return folder_name

    def known(self, dialog_id):
        """Get back all the known messages for the given dialog_id"""
        if dialog_id == self._known_dialog_id:
            return self._known
        self._known = self.store.known_messages(dialog_id)
        self._known_dialog_id = dialog_id
        logger.debug(f"Loaded {len(self._known)} known messages")
        return self._known

    async def save_media(self, message, dialog_id):
        metadata = {}
        media = message.media
        known_messages = self.known(dialog_id)
        if message.id in known_messages:
            # the message is know - use its media
            full_path = known_messages[message.id].get("media")
            if not full_path:
                logger.warning(f"The message is known but without a file {media}")
        else:
            file_name = get_media_name(message)
            if not file_name:
                logger.debug(f"We don't save the {media}")
                return {}

            folder_name = self.get_folder_name(dialog_id)
            full_path = os.path.join(folder_name, file_name)

        if full_path:
            store_path = os.path.join("store", full_path)
            if not os.path.isfile(store_path):
                new_file = True
                with tempfile.NamedTemporaryFile() as fp:
                    path = await message.download_media(fp.name)
                    if not path:
                        logger.error(f"Error: Missing path after save? {message}")
                        return {}
                    fp.seek(0)
                    metadata["hash"] = file_hash(path)
                    metadata["size"] = os.path.getsize(path)
                    known_hash = self.store.known_media_hash(metadata["hash"])
                    if known_hash:
                        first_known = known_hash[0]
                        logger.debug(
                            f"This file is new but known as {first_known} - I'll reuse it"
                        )
                        full_path = known_hash[0]
                        store_path = os.path.join("store", full_path)
                        if os.path.isfile(store_path):
                            new_file = False
                        else:
                            logger.warning(
                                f"File {full_path} was known but is missing - reinjecting it"
                            )
                    if new_file:
                        copy_in_folder(path, store_path)
                if new_file:
                    logger.info(f"File saved to {full_path}")
                    try:
                        mod_time = time.mktime(message.date.timetuple())
                        os.utime(store_path, (mod_time, mod_time))
                    except Exception as e:
                        logger.error(
                            f"Cannot change the time of the file {full_path}: {e}"
                        )
                    if not isinstance(media, MessageMediaContact):
                        if media.ttl_seconds:
                            metadata["self_destructing"] = media.ttl_seconds
                            if self.save_self_destructing:
                                logger.info("Saving self-destructing media")
                                await self.client.send_file(
                                    "me", store_path, caption=message.text
                                )  # send the self_destructing to me
            else:
                pass
                # logger.debug(f"File {full_path} already saved, skipping")
        metadata["media"] = full_path
        return metadata

    def set_message_attributes(self, message_id, attributes, commit=True):
        known_message = self.store.known_message(message_id)
        if not known_message:
            logger.warning(
                f"We didn't know the message - setting the attributes {attributes} however"
            )
            known_message = dict(id=message_id)
            dialog_id = None
        else:
            dialog_id = known_message.pop("dialog")
        if not dialog_id:
            logger.warning("Unknown message, unknown dialog - skipping")
            return
        logger.info(
            f"Changed message: {comprint(known_message)} - {comprint(attributes)}"
        )
        msg = {**known_message, **attributes}

        if message_id in self.known(dialog_id):
            self.known(dialog_id)[message_id] = msg
        self.store.add_msg(dialog_id, msg)
        if commit:
            self.store.save()

    async def process_message(
        self,
        message,
        dialog_id,
        commit=True,  # commit every message?
    ):
        # print(message.id, message.text)
        message_id = message.id
        sender = (
            message.from_id if not message.sender.is_self else None
        )  # sender only if it's not me
        if isinstance(sender, PeerUser):
            sender = sender.user_id

        msg = dict(
            id=message_id,
            text=message.text,
            sender=sender,
            datetime=message.date,
            silent=message.silent,
            from_scheduled=message.from_scheduled,
            edit_date=message.edit_date,
            reactions=[
                emoji.demojize(r.reaction.emoticon) for r in message.reactions.results
            ]
            if message.reactions
            else None,
        )

        msg = {k: v for k, v in msg.items() if v}  # get rid of Falsey
        if message.media and not isinstance(message.media, DONT_SAVE_MEDIA_TYPES):
            media_metadata = await self.save_media(message, dialog_id)
            msg.update(media_metadata)

        self.check_changed(message_id, dialog_id, msg)
        is_known_message = message_id in self.known(dialog_id)
        self.scanned_messages += 1
        self.store.add_msg(dialog_id, msg)
        self.known(dialog_id)[message_id] = msg
        if commit:
            self.store.save()
        return is_known_message

    async def run(self, recent_only=False):
        async for dialog in self.client.iter_dialogs():
            if not await filter_event(dialog):
                continue
            logger.debug(f"{dialog.name} has ID {dialog.id}")
            self.save_dialog(
                dialog.id, dialog.name
            )  # we know the dialog - let's save it

            async for message in self.client.iter_messages(dialog):
                is_known_message = await self.process_message(
                    message,
                    dialog_id=dialog.id,
                    commit=False,
                )

                # exit conditions
                if isinstance(recent_only, datetime.datetime):
                    if message.date < recent_only:
                        logger.debug(
                            "We reached a older message - skipping the remaining"
                        )
                        break
                elif recent_only and is_known_message:
                    logger.debug("We reached a known message - skipping the remaining")
                    break

        logger.debug(f"Scanned {self.scanned_messages} messages")
        self.store.save()

    def check_changed(self, message_id, dialog_id, msg):
        """Fixme: this isn't nice - it set self.changed and modify the msg when edited"""
        known_message = self.known(dialog_id).get(message_id)
        if not known_message:
            logger.info(f"New message: {comprint(msg)}")
            self.changed = True
            return
        else:
            for field in known_message:
                # keep all the extra fields to the message
                if field not in msg:
                    msg[field] = known_message[field]
            if msg.get("text") != known_message.get("text"):
                # keep the history of previous edit
                if "prev" not in msg:
                    msg["prev"] = []
                msg["prev"].append(known_message.get("text"))

        if msg != {  # see if the fields that we have changed
            k: known_message.get(k) for k in msg
        }:
            changes = {k: v for k, v in msg.items() if msg[k] != known_message.get(k)}
            logger.info(f"Changed message: {comprint(changes)}")
            self.changed = True

    def commit(self):
        self.store.save()


async def main(
    client: "TelegramClient",
    store: "Store",
    recent_only=True,
    save_self_destructing=True,
    listen=False,
):
    saver = DialogSaver(
        client=client, store=store, save_self_destructing=save_self_destructing
    )
    if listen:
        print("Listening for Telegram updates...")
        await wait_for_updates(saver)
    else:
        await saver.run(
            # recent_only=recent_only,
            recent_only=recent_only and utcnow() - datetime.timedelta(days=7)
        )


async def main_status(client: "TelegramClient", store: "Store"):
    saver = DialogSaver(client=client, store=store)
    async for dialog in client.iter_dialogs():
        if not await filter_event(dialog):
            continue
        saver.save_dialog(dialog.id, dialog.name)

        known_dialog = store.dialog_names.get(dialog.id, {})
        previous_status = known_dialog.get("telegram_online_status")
        previous_last_online = known_dialog.get("last_online")

        entity = dialog.entity
        if not isinstance(entity, types.User):
            store.set_dialog_status(
                dialog.id, telegram_online_status=None, last_online=None
            )
            continue

        status, last_online = infer_online_status(
            entity.status,
            previous_status=previous_status,
            previous_last_online=previous_last_online,
        )
        store.set_dialog_status(
            dialog.id,
            telegram_online_status=status,
            last_online=last_online,
        )
    store.save()

    top = store.top_dialogs_by_last_online(limit=20)
    print("Top 20 dialogs by last_online")
    print_status_table(top)


if __name__ == "__main__":
    logging.basicConfig(
        # level=logging.DEBUG
    )
    logging.getLogger("telethon.network").setLevel(logging.ERROR)
    if not api_hash or not api_id:
        raise RuntimeError(
            "Please set TELEGRAM_API_ID and TELEGRAM_API_HASH environment variables"
        )

    def parse():
        parser = argparse.ArgumentParser(
            description="Preserve a history of known Telegram messages"
        )
        parser.add_argument(
            "--all",
            help="Go ahead to the beginning of history, don't stop at the last known message",
            default=False,
            action="store_true",
        )

        parser.add_argument(
            "--dontsaveselfdestructing",
            help="Avoid forwarding new self-destructing messages to yourself",
            default=False,
            dest="dont_save_self_destructing",
            action="store_true",
        )

        parser.add_argument(
            "--debug", help="Verbose logging active", default=False, action="store_true"
        )

        parser.add_argument(
            "--log", help="Log the latest messages", default=False, action="store_true"
        )

        parser.add_argument(
            "--config",
            help="Choose another config file",
            default="secret/.session",
            action="store",
        )

        parser.add_argument(
            "--listen", help="Start listening for updates", action="store_true"
        )
        parser.add_argument(
            "command",
            nargs="?",
            choices=("sync", "status"),
            default="sync",
            help="sync messages (default) or update Telegram contact status",
        )

        args = parser.parse_args()

        logger.setLevel(logging.DEBUG if args.debug else logging.INFO)
        store = Store()
        if args.log:
            store.log()
        else:
            config_parent = os.path.dirname(args.config)
            if config_parent:
                os.makedirs(config_parent, exist_ok=True)  # create parent
            if args.command == "status":
                if args.all:
                    logger.warning("Ignoring --all in status mode")
                if args.dont_save_self_destructing:
                    logger.warning("Ignoring --dontsaveselfdestructing in status mode")
                if args.listen:
                    logger.warning("Ignoring --listen in status mode")
            with TelegramClient(
                args.config,
                int(api_id),
                api_hash,
                connection_retries=-1,
                request_retries=-1,
            ) as client:
                client.loop.run_until_complete(
                    main_status(client=client, store=store)
                    if args.command == "status"
                    else main(
                        client=client,
                        store=store,
                        recent_only=not args.all,
                        save_self_destructing=not args.dont_save_self_destructing,
                        listen=args.listen,
                    )
                )
        store.close()

    parse()
    logger.debug("Fin.")
