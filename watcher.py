import logging
from asyncio import sleep
from datetime import datetime

from aiocache import cached
from telethon import events
from telethon.tl.custom import Dialog
from telethon.utils import get_display_name

logger = logging.getLogger(__name__)


@cached(ttl=60 * 15)  # cache them for 15'
async def get_dialogs(client):
    """ Get the archived dialogs """
    logger.debug("Getting archived Dialogs")
    return {
        dialog.id: dialog
        for dialog in await client.get_dialogs()
    }


async def filter_event(event):
    if not isinstance(event, Dialog):
        if event is None or event.chat_id is None:
            # the deletion events have no chat we move them ahead
            return True
    if event.is_channel:  # event if event is a Dialog will have this
        logger.debug("Skipping channel")
        return False
    if not isinstance(event, Dialog):
        dialogs = await get_dialogs(event.client)
        try:
            dialog = dialogs[event.chat_id]
        except Exception as e:
            logger.error(f"Can't get the dialog: {e}")
            return True
    else:
        dialog = event

    if dialog.archived:
        logger.debug(f"Skipping archived {get_display_name(dialog)}")
    else:
        return True


async def wait_for_updates(saver):
    """
    :type saver: DialogSaver
    """
    client = saver.client

    @client.on(events.NewMessage())
    @client.on(events.MessageEdited())
    async def message_updated_handler(event):
        await event.get_chat()
        await event.get_sender()
        if await filter_event(event):
            saver.save_dialog(event.chat.id, get_display_name(event.chat))  # the dialog
            await saver.process_message(message=event.message,
                                        dialog_id=event.chat_id)

    @client.on(events.MessageRead())
    async def message_read_handler(event):
        if await filter_event(event):
            read_time = datetime.utcnow()
            message_ids = event._message_ids or [event._message_id]
            for message_id in message_ids:
                saver.set_message_attributes(message_id,
                                             {"read_time": read_time},
                                             commit=False)
            saver.commit()

    @client.on(events.MessageDeleted())
    async def message_delete_handler(event):
        if await filter_event(event):
            for message_id in event.deleted_ids:
                saver.set_message_attributes(message_id,
                                             {"deleted": True},
                                             commit=False)
            saver.commit()

    while True:
        try:
            logger.info("Catching up")
            await client.catch_up()

            logger.info("Waiting for updates...")
            await client.run_until_disconnected()
        except ConnectionError:
            await sleep(10)
