import datetime
import json
import logging
import os
import sqlite3

DB_FIELDS = ['id', 'datetime', 'text', 'sender', 'media']
logger = logging.getLogger(__name__)


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
                        name text not null
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
        if isinstance(msg['datetime'], float):
            msg['datetime'] = datetime.datetime.fromtimestamp(msg['datetime'])
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
                ({",".join(["?"] * (len(params)))});
        """
        # print(query)
        # print(params)
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


def json_to_sqlite():
    for folder in os.listdir('store'):
        logger.debug(f"Importing dialog {folder}")
        with open(os.path.join("store", folder, 'store.json')) as f:
            data = json.load(f)
            for msg_id, msg in data.items():
                store.add_msg(dialog_id=int(folder),
                              msg={**msg,
                                   "id": msg_id})


if __name__ == '__main__':
    store = Store()
    logging.basicConfig(
        level=logging.DEBUG
    )

    json_to_sqlite()

    store.close()
