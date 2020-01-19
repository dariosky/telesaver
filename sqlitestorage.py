import datetime
import json
import logging
import os
import sqlite3

import pytz
from cached_property import cached_property
from dateutil.parser import parse

DB_FIELDS = ['id', 'datetime', 'text', 'sender', 'media']
logger = logging.getLogger(__name__)
DATETIME_FIELDS = ('datetime', 'edit_date')
TIME_FORMAT = '%Y-%m-%d %H:%M:%S'


class Store:
    def __init__(self, filename='store.sqlite'):
        """ create a database connection to a SQLite database """
        self.conn = sqlite3.connect(filename)
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
            logger.warning("Missing datetime on message", msg)
            return
        for field in DATETIME_FIELDS:
            if not msg.get(field):
                continue
            if isinstance(msg[field], float):
                msg[field] = datetime.datetime.fromtimestamp(msg[field])
            if isinstance(msg[field], datetime.datetime):
                assert msg[field].tzinfo == datetime.timezone.utc
                msg[field] = msg[field].strftime(TIME_FORMAT + "%z")
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
        self.cur.execute(query,
                         params)
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

    def known_messages(self, dialog_id):
        query = """
                    SELECT id, datetime, text, sender, media, extra
                    from messages
                    where dialog=?
                """
        self.cur.execute(query, (dialog_id,))

        def get_msg(r):
            msg = dict(
                id=r[0],
                datetime=r[1],
                text=r[2],
                sender=r[3],
                media=r[4],
                **json.loads(r[5])
            )
            for field in DATETIME_FIELDS:
                if field not in msg:
                    continue
                tz_field = msg[field]
                if isinstance(tz_field, str):
                    msg[field] = parse(tz_field)
            return {k: v for k, v in msg.items() if v}  # get rid of Falsey

        return {
            r[0]: get_msg(r)
            for r in self.cur.fetchall()
        }


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


def convert_msg_to_utc():
    tz = pytz.timezone('CET')
    utc = pytz.utc
    for dialog_id, desc in store.dialog_names.items():
        known = store.known_messages(dialog_id)
        for msg_id, msg in known.items():
            for field in ('datetime', 'edit_date'):
                if msg.get(field):
                    # convert from CET to UTC
                    tz_field = msg[field]
                    if isinstance(tz_field, str):
                        tz_field = parse(tz_field)
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

    json_to_sqlite()
    convert_msg_to_utc()
    store.close()
