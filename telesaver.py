#!/usr/bin/env python
import argparse
import datetime
import logging
import os
import shutil
import tempfile
import time

from cached_property import cached_property
from telethon import TelegramClient, utils
from telethon.client import DownloadMethods
from telethon.tl import types
from telethon.tl.types import MessageMediaWebPage, MessageMediaGeo

from sqlitestorage import Store
from util import slugify, file_hash

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
        if media is None:
            logger.debug("Expired message is gone")
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


class DialogSaver:
    def __init__(self, store, dialog, save_self_destructing=False) -> None:
        super().__init__()
        self.store = store
        self.dialog = dialog
        self.changed = False
        self.save_self_destructing = save_self_destructing
        known_dialogs = self.store.dialog_names
        dialog_id = self.dialog.id
        if dialog_id not in known_dialogs:
            store.add_dialog(
                dialog_id, self.dialog.name, self.get_folder_name()
            )

    def get_user_name(self, user=None):
        if user is None:
            return self.dialog.name
        else:
            return user.username or user.first_name

    def get_folder_name(self):
        dialog_id = self.dialog.id
        known_dialogs = self.store.dialog_names
        if dialog_id in known_dialogs:
            return known_dialogs[dialog_id]['folder']
        logger.info(f"Adding new dialog info: {self.dialog.name}")
        folder_name = slugify(self.get_user_name())
        return folder_name

    @cached_property
    def known(self):
        known = self.store.known_messages(self.dialog.id)
        logger.debug(f"Loaded {len(known)} known messages")
        return known

    @cached_property
    def media_dir_path(self):
        dialog_folder_name = self.get_folder_name()  # this creates the dialog
        return os.path.join('store', dialog_folder_name, 'media')

    async def save_media(self, message):
        metadata = {}
        media = message.media
        if message.id in self.known:
            # the message is know - use its media
            full_path = self.known[message.id].get('media')
            if not full_path:
                logger.warning(f"The media is known but without filename {media}")
        else:
            file_name = get_media_name(media, message.date)
            if not file_name:
                logger.debug(f"We don't save the {media}")
                return {}
            full_path = os.path.join(self.media_dir_path, file_name)
        if full_path:
            if not os.path.isfile(full_path):
                new_file = True
                with tempfile.NamedTemporaryFile() as fp:
                    path = await message.download_media(fp.name)
                    if not path:
                        logger.error(f"Error: Missing path after save? {message}")
                        return {}
                    else:
                        fp.seek(0)
                        metadata['hash'] = file_hash(path)
                        metadata['size'] = os.path.getsize(path)
                        known = store.known_media_hash(metadata['hash'])
                        if known:
                            first_known = known[0]
                            logger.debug(f"This file is new but known as {first_known} - I'll reuse it")
                            full_path = known[0]
                            new_file = False
                        else:
                            shutil.copy2(path, full_path)
                if new_file:
                    logger.info(f'File saved to {full_path}')
                    try:
                        mod_time = time.mktime(message.date.timetuple())
                        os.utime(full_path, (mod_time, mod_time))
                    except:
                        pass
                    if media.ttl_seconds:
                        metadata['self_destructing'] = media.ttl_seconds
                        if self.save_self_destructing:
                            logger.info("Saving self-distructing media")
                            await client.send_file('me', full_path,
                                                   caption=message.text)  # send the self_destructing to me
            else:
                pass
                # logger.debug(f"File {full_path} already saved, skipping")
        metadata['media'] = full_path
        return metadata

    async def run(self, recent_only=False):
        scanned_messages = 0
        async for message in client.iter_messages(self.dialog):
            # print(message.id, message.text)
            message_id = message.id

            msg = dict(
                id=message_id,
                text=message.text,
                sender=message.from_id if not message.sender.is_self else None,  # sender only if it's not me
                datetime=message.date,
                silent=message.silent,
                from_scheduled=message.from_scheduled,
                edit_date=message.edit_date,
            )

            msg = {k: v for k, v in msg.items() if v}  # get rid of Falsey
            if message.media and not isinstance(message.media, DONT_SAVE_MEDIA_TYPES):
                media_metadata = await self.save_media(message)
                msg.update(media_metadata)

            self.check_changed(message_id, msg)
            is_known_message = message_id in self.known
            scanned_messages += 1
            self.store.add_msg(self.dialog.id, msg)
            self.known[message_id] = msg

            # exit conditions
            if isinstance(recent_only, datetime.datetime):
                if message.date < recent_only:
                    logger.debug("We reached a older message - skipping the remaining")
                    break
            elif recent_only and is_known_message:
                logger.debug("We reached a known message - skipping the remaining")
                break

        logger.debug(f"Scanned {scanned_messages} messages")
        self.store.save()

    def check_changed(self, message_id, msg):
        known_message = self.known.get(message_id)
        if not known_message:
            logger.info(f"New message: {msg}")
            self.changed = True
            return

        if msg != {  # see if the fields that we have have changed
            k: known_message.get(k)
            for k in msg
        }:
            changes = {k: v
                       for k, v in msg.items()
                       if msg[k] != known_message.get(k)}
            logger.info(f"Changed message: {changes}")
            self.changed = True

        if known_message:
            for field in known_message:
                # keep all the extra fields to the message
                if field not in msg:
                    msg[field] = known_message[field]
            if msg.get('text') != known_message.get('text'):
                # keep the history of previous edit
                if 'prev' not in msg:
                    msg['prev'] = []
                msg['prev'].append(known_message.get('text'))


async def main(store: 'Store', dialog_id=None, recent_only=True, save_self_destructing=True):
    async for dialog in client.iter_dialogs():
        if dialog_id is None or dialog.id == dialog_id:
            if dialog.is_channel:
                logger.debug(f"Skipping channel {dialog.name}")
                continue
            if dialog.archived:
                logger.debug(f"Skipping archived {dialog.name}")
                continue
            logger.debug(f"{dialog.name} has ID {dialog.id}")
            saver = DialogSaver(store=store,
                                dialog=dialog,
                                save_self_destructing=save_self_destructing)
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

    parser.add_argument("--debug",
                        help="Verbose logging active",
                        default=False,
                        action="store_true")

    parser.add_argument("--log",
                        help="Log the latest messages",
                        default=False,
                        action="store_true")

    parser.add_argument("--config",
                        help="Choose another config file",
                        default='secret/.session',
                        action="store")

    args = parser.parse_args()

    logger.setLevel(logging.DEBUG if args.debug else logging.INFO)
    store = Store()
    if args.log:
        store.log()
    else:
        with TelegramClient(args.config, api_id, api_hash) as client:
            client.loop.run_until_complete(
                main(
                    store=store,
                    recent_only=not args.all,
                    save_self_destructing=not args.dont_save_self_destructing,
                )
            )
    store.close()
    logger.debug("Fin.")
