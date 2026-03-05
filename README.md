# Telegram saver

## A bot to keep your Telegram History

This is a bot that works as a Telegram client for your account
and keeps track of messages in your chats.

### Install

This works as a CLI

* create a Telegram application
* set the environment variables `TELEGRAM_API_ID` and `TELEGRAM_API_HASH`
* run it

### Commands

* `telesaver.py` (default `sync` mode): scan messages and save history/media
* `telesaver.py sync`: explicit `sync` mode
* `telesaver.py status`: check Telegram presence status of dialogs and update DB

### Options (sync mode)

* `--all`: go to the beginning of history, don't stop at the last known message
* `--dontsaveselfdestructing`: avoid forwarding new self-destructing messages to yourself
* `--listen`: keep listening for updates
* `--log`: print latest messages
* `--config`: choose another session file (default `secret/.session`)
* `--debug`: verbose logs

### Storage

By default, it saves:

* filesystem media under `store/`
* SQLite DB `store.sqlite` for messages and dialogs

### How `status` works

When you run `telesaver.py status`, the tool:

* iterates dialogs (using the same dialog filter already used by the app)
* checks Telegram user presence (`online`, `offline`, `recently`, `within a week`, `within a month`, `empty`, `long time ago`)
* updates `dialogs.telegram_online_status` and `dialogs.last_online`

`last_online` inference rules:

* `online`: set `last_online` to now
* `offline`: set `last_online` to Telegram `was_online` timestamp
* `recently`: set `last_online` with `max(previous_last_online, now - 3 days)`
* `within a week`: set `last_online` with `max(previous_last_online, now - 7 days)`
* `within a month`: set `last_online` with `max(previous_last_online, now - 1 calendar month)`
* `long time ago`: no minimum-date clamp is applied (kept as is)

At the end, it prints a table with top 20 dialogs sorted by `last_online` desc, with a secondary order for same timestamps:

1. `recently`
2. `within a week`
3. `within a month`
4. `long time ago`

`last_update` in the table is shown relative to now (for example `10 minutes ago`, `10 hours ago`, `3 weeks ago`).

As you may guess with the 2nd option - when he finds a new self-destructing-message
it will forward it to you unless you ask otherwise.
