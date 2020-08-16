import datetime
import json
import logging
import os
import shutil
import sqlite3

import pytz
from cached_property import cached_property

from util import dict_factory, parse_time, file_hash

DB_FIELDS = ['id', 'datetime', 'text', 'sender', 'media']
logger = logging.getLogger(__name__)
DATETIME_FIELDS = ('datetime', 'edit_date', 'read_time')
TIME_FORMAT = '%Y-%m-%d %H:%M:%S%z'


class Store:
    def __init__(self, filename='store.sqlite'):
        """ create a database connection to a SQLite database """
        self.conn = sqlite3.connect(filename or ':memory:')
        self.cur = self.conn.cursor()
        self.create_tables()
        self.changed = False

    def create_tables(self):
        self.cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        tables = [r[0] for r in self.cur.fetchall()]

        if 'dialogs' not in tables:
            logger.info("Creating table dialogs")
            create_dialogs = """
                create table dialogs
                    (
                        id int not null
                            constraint dialogs_pk
                                primary key,
                        name text not null,
                        folder text not null
                    );
                """
            self.cur.execute(create_dialogs)
        if 'messages' not in tables:
            logger.info("Creating table messages")
            create_messages = """
                create table messages
                (
                    dialog int not null,
                    id int not null,
                    datetime datetime not null,
                    text text,
                    sender int
                        constraint sender
                            references dialogs,
                    media text,
                    extra json,
                    
                    PRIMARY KEY (dialog, id)
                );
            """
            self.cur.execute(create_messages)

    def add_msg(self, dialog_id, msg):
        msg = msg.copy()  # we change the datetime

        if 'datetime' not in msg:
            logger.warning("Missing datetime on message - skipping", msg)
            return

        for field in DATETIME_FIELDS:
            if not msg.get(field):
                continue
            if isinstance(msg[field], float):
                msg[field] = datetime.datetime.fromtimestamp(msg[field])
            if isinstance(msg[field], datetime.datetime):
                if msg[field].tzinfo != datetime.timezone.utc:
                    msg[field] = pytz.utc.localize(msg[field])
                msg[field] = msg[field].strftime(TIME_FORMAT)

        extra = {k: v
                 for k, v in msg.items()
                 if k not in DB_FIELDS}

        params = (
                [dialog_id] +
                [msg.get(k)
                 for k in DB_FIELDS] +
                [json.dumps(extra)])

        query = f"""
            INSERT OR REPLACE INTO messages
                (dialog,{",".join(DB_FIELDS)},extra)
            VALUES
                ({",".join(["?"] * len(params))});
        """
        # print(query)
        # print(params)
        self.cur.execute(query,
                         params)
        self.changed = True

    def add_dialog(self, dialog_id, name, folder):
        params = (dialog_id, name, folder)
        query = f"""
                    INSERT OR REPLACE INTO dialogs
                        (id, name, folder)
                    VALUES
                        ({",".join(["?"] * len(params))});
                """
        self.cur.execute(query, params)
        self.dialog_names[dialog_id] = dict(name=name,  # we know the new dialog
                                            folder=folder)
        self.changed = True

    def save(self):
        if self.changed:
            self.conn.commit()
            self.changed = False

    def close(self):
        self.save()
        if self.conn:
            self.conn.close()

    @cached_property
    def dialog_names(self):
        query = """
            SELECT id, name, folder
            from dialogs
        """
        self.cur.execute(query)
        return {r[0]: dict(name=r[1],
                           folder=r[2])
                for r in self.cur.fetchall()}

    def get_messages_from_cursor(self):
        """ Return the record out of the cursor """

        def get_msg(r):
            msg = dict(
                id=r[0],
                datetime=r[1],
                text=r[2],
                sender=r[3],
                media=r[4],
                **json.loads(r[5]),
            )
            if len(r) > 6:
                msg['dialog'] = r[6]
            for field in DATETIME_FIELDS:
                if field not in msg:
                    continue
                tz_field = msg[field]
                if isinstance(tz_field, str):
                    msg[field] = parse_time(tz_field)
            return {k: v for k, v in msg.items() if v}  # get rid of Falsey

        return {
            r[0]: get_msg(r)
            for r in self.cur.fetchall()
        }

    def known_messages(self, dialog_id):
        query = """
                    SELECT id, datetime, text, sender, media, extra
                    from messages
                    where dialog=?
                """
        self.cur.execute(query, (dialog_id,))
        return self.get_messages_from_cursor()

    def known_message(self, message_id):
        query = """
                    SELECT id, datetime, text, sender, media, extra, dialog
                    from messages
                    where id=?
                """
        self.cur.execute(query, (message_id,))
        records = self.get_messages_from_cursor()
        return records[message_id] if records else None

    def log(self, number=10):
        """ Display the last messages """
        query = """
                    SELECT datetime, text, sender, media, extra, d.name as dialog_name
                    from messages m
                    left join dialogs d on m.sender = d.id
                    order by datetime desc
                    limit ?
                """
        self.cur.execute(query, (number,))

        def get_msg(r):
            msg = dict_factory(self.cur, r)
            extra = json.loads(msg['extra'])  # add the content of extra
            msg.update(extra)

            tokens = [
                f"{msg['datetime']}",
                f"- {'me' if msg['sender'] is None else msg['dialog_name']} -",
                f"{msg['text'] or msg['media']}"
            ]
            if msg.get('silent'):
                tokens.append("[silent]")
            if msg.get('scheduled'):
                tokens.append("[scheduled]")
            if msg.get('edit_date'):
                tokens.append("[edited]")

            msg = " ".join(tokens)

            return msg

        for r in self.cur.fetchall():
            print(get_msg(r))

    def consolidate_media(self, commit=False):
        media_hash_to_path = {}
        for dialog_id, dialog in self.dialog_names.items():
            logger.debug(f"Dialog: {dialog['name']}")
            messages = self.known_messages(dialog_id).items()

            if not messages:
                dialog_path = os.path.join('store', dialog['folder'], )
                if os.path.isdir(dialog_path):
                    logger.info(f"Removing dialog {dialog_path}")
                    if commit:
                        shutil.rmtree(dialog_path)

            for msgid, message in messages:
                path = message.get('media')
                changed = False
                if path:
                    if not os.path.isfile("store/" + path):
                        # path is not absolute - let's change it
                        new_path = path.replace('store/', '').replace("/media/", "/")

                        if os.path.isfile("store/" + new_path):
                            # set the absolute path
                            path = new_path
                            message['media'] = new_path
                            changed = True

                    if not os.path.isfile("store/" + path):
                        logger.error(f"Media not found: {path}")
                        continue

                    # here the path is valid
                    if 'hash' not in message:
                        changed = True
                        message['hash'] = file_hash("store/" + path)

                    if 'size' not in message:
                        message['size'] = os.path.getsize("store/" + path)
                        changed = True

                    mediaid = message['hash']

                    if mediaid in media_hash_to_path:
                        known_hash_path = media_hash_to_path[mediaid]
                        if path != known_hash_path:
                            logger.info(f"I know this media as {known_hash_path}- let's change this message")
                            if os.path.isfile('store/' + known_hash_path):
                                # and delete the file
                                message['media'] = known_hash_path
                                changed = True
                                if os.path.isfile('store/' + path):
                                    os.makedirs('duplicates', exist_ok=True)
                                    shutil.copy('store/' + path, 'duplicates')
                                    if commit:
                                        os.remove('store/' + path)
                    else:
                        media_hash_to_path[mediaid] = path

                    if changed:
                        logger.debug(f"Changed: {message}")
                        if commit:
                            self.add_msg(dialog_id, message)

    def scan_unreferenced_media(self, media_hash_to_path, commit=False):
        existing_media_files = set()
        for root, dirs, files in os.walk('store'):
            for filename in files:
                existing_media_files.add(
                    os.path.join(root, filename)
                )
        referenced_media = set(media_hash_to_path.values())
        stale_files = existing_media_files - referenced_media
        if stale_files:
            logger.debug(f"We have {len(stale_files)} stale files")
            for stale_filename in stale_files:
                if commit:
                    logger.info(f"Deleting stale {stale_filename}")
                    os.remove(stale_filename)
                else:
                    logger.debug(f"Stale {stale_filename}")

    def known_media_hash(self, hash):
        query = """
            select media from messages
            where json_extract(extra, '$.hash')=?
        """
        self.cur.execute(query, (hash,))
        known = [r[0] for r in self.cur.fetchall()]
        return known


def json_to_sqlite():
    for folder in os.listdir('store'):
        file_path = os.path.join("store", folder, 'store.json')
        try:
            dialog_id = int(folder)
        except ValueError:
            logger.debug(f"New format folder {folder}")
            if os.path.exists(file_path):
                logger.info("Deleting the store.json")
                os.remove(file_path)
            continue

        if os.path.exists(file_path):
            logger.debug(f"Importing dialog {folder}")
            with open(file_path) as f:
                data = json.load(f)
                for msg_id, msg in data.items():
                    store.add_msg(dialog_id,
                                  msg={**msg,
                                       "id": msg_id})


def convert_msg_to_utc(from_tz):
    tz = pytz.timezone(from_tz)
    utc = pytz.utc
    for dialog_id, desc in store.dialog_names.items():
        known = store.known_messages(dialog_id)
        for msg_id, msg in known.items():
            for field in ('datetime', 'edit_date'):
                if msg.get(field):
                    # convert from CET to UTC
                    tz_field = msg[field]
                    if isinstance(tz_field, str):
                        tz_field = parse_time(tz_field)
                    elif isinstance(tz_field, float):
                        tz_field = datetime.datetime.fromtimestamp(tz_field)
                    if not tz_field.tzinfo:
                        utc_date = tz_field.replace(tzinfo=tz).astimezone(utc)
                        msg[field] = utc_date

            store.add_msg(
                dialog_id, msg
            )

        print(desc)


if __name__ == '__main__':
    store = Store()
    logging.basicConfig(
        level=logging.DEBUG
    )

    # json_to_sqlite()
    # convert_msg_to_utc()
    # store.log()
    store.consolidate_media(
        # commit=True
    )
    store.close()
