import logging

from telethon import events
from telethon.tl.types import Dialog

logger = logging.getLogger(__name__)


def filter_event(e):
    if isinstance(e, Dialog):
        name = e.name
    else:
        name = e.title
    if e.is_channel:
        logger.debug(f"Skipping channel {name}")
    elif e.archived:
        logger.debug(f"Skipping archived {name}")
    else:
        return True


async def wait_for_updates(saver):
    """
    :type saver: DialogSaver
    """
    client = saver.client

    @client.on(events.NewMessage())
    @client.on(events.MessageEdited())
    async def new_message_handler(event):
        if not filter_event(event):
            return
        await saver.process_message(event.message)

    @client.on(events.MessageRead())
    async def message_read_handler(event):
        print(event)
        if not filter_event(event):
            return

    @client.on(events.MessageDeleted())
    async def message_read_handler(event):
        print(event)
        if not filter_event(event):
            return

    logger.info("Catching up")
    await client.catch_up()

    logger.info("Waiting for updates...")
    await client.run_until_disconnected()
