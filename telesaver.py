#!/usr/bin/env python
import argparse
import datetime
import json
import logging
import os
import time

from cached_property import cached_property
from telethon import TelegramClient, utils
from telethon.client import DownloadMethods
from telethon.tl import types
from telethon.tl.types import MessageMediaWebPage, MessageMediaGeo

logger = logging.getLogger(__name__)
title = "TeleSave"
api_id = os.environ.get('TELEGRAM_API_ID')
api_hash = os.environ.get('TELEGRAM_API_HASH')

DONT_SAVE_MEDIA_TYPES = (MessageMediaWebPage, MessageMediaGeo)


def get_media_name(media, date):
    possible_names = []
    if isinstance(media, types.MessageMediaWebPage):
        if isinstance(media.webpage, types.WebPage):
            media = media.webpage.document or media.webpage.photo
    if isinstance(media, types.MessageMediaDocument):
        media = media.document
    if isinstance(media, (types.MessageMediaPhoto, types.Photo)):
        kind = 'photo'
        extension = '.jpg'
    elif isinstance(media, types.MessageMediaContact):
        kind = 'contact'
        extension = '.vcard'
        possible_names = [f"{media.first_name}{extension}"]
    elif isinstance(media, (types.MessageMediaDocument, types.Document,
                            types.WebDocument, types.WebDocumentNoProxy)):
        kind, possible_names = DownloadMethods._get_kind_and_names(media.attributes)
        extension = utils.get_extension(media)
    elif isinstance(media, (types.MessageMediaGeoLive, types.Document)):
        return
    else:
        logger.error(f"Unknow media type: {type(media)}")
        return

    if possible_names:
        document_name = possible_names[0]
        file_name, extension = os.path.splitext(document_name)

        return f"{file_name}-{date.year:02}-{date.month:02}" \
               f"_{date.day:02}-{date.hour:02}-{date.minute:02}{date.second}" \
               f"{extension}"
    else:
        return f'{kind}_{date.year}-{date.month:02}-{date.day:02}' \
               f'_{date.hour:02}-{date.minute:02}-{date.second:02}' \
               f'{extension}'


def get_user_name(user):
    return user.username or user.first_name


class DialogSaver:
    def __init__(self, dialog) -> None:
        super().__init__()
        self.dialog = dialog
        self.folder_path = f"store/{dialog.id}"
        self.create_store_folder()
        self.changed = False

    def create_store_folder(self):
        if not os.path.isdir(self.folder_path):
            os.mkdir(self.folder_path)
        if not os.path.isdir(self.media_dir_path):
            os.mkdir(self.media_dir_path)

    @cached_property
    def known(self):
        if os.path.isfile(self.store_file_path):
            with open(self.store_file_path, 'r') as f:
                known = json.load(f)
        else:
            known = {}
        logger.debug(f"Loaded {len(known)} known messages")
        return known

    @cached_property
    def store_file_path(self):
        return os.path.join('store', str(self.dialog.id), 'store.json')

    @cached_property
    def media_dir_path(self):
        return os.path.join('store', str(self.dialog.id), 'media')

    async def save_media(self, message):
        metadata = {}
        media = message.media
        file_name = get_media_name(media, message.date)
        if file_name:
            full_path = os.path.join(self.media_dir_path, file_name)
            if not os.path.isfile(full_path):
                path = await message.download_media(full_path)
                if not path:
                    logger.warning("Missing path after save?")
                else:
                    logger.info(f'File saved to {path}')
                    try:
                        mod_time = time.mktime(message.date.timetuple())
                        os.utime(full_path, (mod_time, mod_time))
                    except:
                        pass
            else:
                logger.debug(f"File {file_name} already saved, skipping")
            metadata['media'] = file_name
        return metadata

    async def run(self, recent_only=False):
        scanned_messages = 0
        async for message in client.iter_messages(self.dialog):
            # print(message.id, message.text)
            message_id = str(message.id)
            sender_name = get_user_name(message.sender)

            msg = dict(
                text=message.text,
                sender=message.from_id if not message.sender.is_self else None,  # sender only if it's not me
                datetime=message.date.timestamp(),
                silent=message.silent,
                from_scheduled=message.from_scheduled,
            )
            # if isinstance(message.to_id, PeerUser):
            #     msg['target'] = message.to_id.user_id

            msg = {k: v for k, v in msg.items() if v}  # get rid of Falsey
            if message.media and not isinstance(message.media, DONT_SAVE_MEDIA_TYPES):
                media_metadata = await self.save_media(message)
                msg.update(media_metadata)

            self.check_changed(message_id, msg)
            scanned_messages += 1
            if isinstance(recent_only, datetime.datetime):
                if message.date < recent_only:
                    logger.debug("We reached a older message - skipping the remaining")
                    break
            elif recent_only and message_id in self.known:
                logger.debug("We reached a known message - skipping the remaining")
                break
            self.known[message_id] = msg

        logger.debug(f"Scanned {scanned_messages} messages")
        self.save_store()
        self.scan_unreferenced_media(delete=False)

    def scan_unreferenced_media(self, delete=False):
        media_files = set(os.listdir(self.media_dir_path))
        referenced_media = {msg['media']
                            for msg in self.known.values()
                            if 'media' in msg}
        stale_files = media_files - referenced_media
        if stale_files:
            logger.debug(f"We have {len(stale_files)} stale files")
            for stale_filename in stale_files:
                if delete:
                    logger.info(f"Deleting stale {stale_filename}")
                    os.remove(os.path.join(self.media_dir_path, stale_filename))
                else:
                    logger.debug(f"Stale {stale_filename}")

    def check_changed(self, message_id, msg):
        known_message = self.known.get(message_id)
        if not known_message:
            logger.info(f"New message: {msg}")
            self.changed = True
        elif known_message != msg:
            changes = {k: v
                       for k, v in msg.items()
                       if msg[k] != known_message.get(k)}
            logger.info(f"Changed message: {changes}")
            self.changed = True

    def save_store(self):
        if not self.changed:
            logger.debug("Nothing changed - skipping save")
            return
        with open(self.store_file_path, 'w') as f:
            logger.debug(f"Saving know {len(self.known)} messages")
            json.dump(self.known, f)


async def main(dialog_id=None, recent_only=True, save_self_destructing=True):
    async for dialog in client.iter_dialogs():
        if dialog_id is None or dialog.id == dialog_id:
            if dialog.is_channel:
                logger.debug(f"Skipping channel {dialog.name}")
                continue
            if dialog.archived:
                logger.debug(f"Skipping archived {dialog.name}")
                continue
            logger.debug(f"{dialog.name} has ID {dialog.id}")
            saver = DialogSaver(dialog)
            await saver.run(
                recent_only=recent_only,
                # recent_only=pytz.utc.localize(datetime.datetime.utcnow()) - datetime.timedelta(days=5)
            )
            if dialog_id:
                break


if __name__ == '__main__':
    logging.basicConfig(
        # level=logging.DEBUG
    )
    if not api_hash or not api_id:
        raise RuntimeError("Please set TELEGRAM_API_ID and TELEGRAM_API_HASH environment variables")

    parser = argparse.ArgumentParser(description='Preserve a history of known Telegram messages')
    parser.add_argument("--all",
                        help="Go ahead to the beginning of history, don't stop at the last known message",
                        default=False,
                        action="store_true")

    parser.add_argument("--dontsaveselfdestructing",
                        help="Avoid forwarding new self-destructing messages to yourself",
                        default=False,
                        dest='dont_save_self_destructing',
                        action="store_true")

    args = parser.parse_args()

    logger.setLevel(logging.DEBUG)
    with TelegramClient('.session', api_id, api_hash) as client:
        client.loop.run_until_complete(
            main(
                recent_only=not args.all,
                save_self_destructing=not args.dont_save_self_destructing,
            )
        )
    logger.debug("Fin.")
