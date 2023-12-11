# Telegram saver

## A bot to keep your Telegram History

This is a bot that works as a Telegram client for your account 
and keep track of the messages in your chats.

### Install

This works as a CLI

* create a Telegram application
* set the environment variables TELEGRAM_API_ID and TELEGRAM_API_HASH
* run it

There are a few options:

* `--all` Go ahead to the beginning of history, don't stop at the last known message
* `--dontsaveselfdestructing` Avoid forwarding new self-destructing messages to yourself

By default it saves everything on the filesystem - a `store` subfolder with
all the media attachments of the chats - a Sqlite DB with the messages.

As you may guess with the 2nd option - when he finds a new self-destructing-message
it will forward it to you unless you ask otherwise. 
