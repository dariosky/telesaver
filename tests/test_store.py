from sqlitestorage import Store


def test_know_dialogs():
    store = Store(filename=None)
    assert store.changed is False

    assert store.dialog_names == {}
    store.add_dialog("id", "name", "folder/")
    assert "id" in store.dialog_names
    assert store.dialog_names["id"]["name"] == "name"
    assert store.dialog_names["id"]["folder"] == "folder/"
    assert store.dialog_names["id"].get("telegram_online_status") is None
    assert store.dialog_names["id"].get("last_online") is None


def test_dialog_schema_migrated_columns_exist():
    store = Store(filename=None)
    store.cur.execute("PRAGMA table_info(dialogs)")
    columns = {row[1] for row in store.cur.fetchall()}
    assert "telegram_online_status" in columns
    assert "last_online" in columns


def test_add_dialog_preserves_status_fields():
    store = Store(filename=None)
    store.add_dialog("id", "old", "old-folder")
    store.set_dialog_status("id", telegram_online_status="recently")

    store.add_dialog("id", "new", "new-folder")

    assert store.dialog_names["id"]["name"] == "new"
    assert store.dialog_names["id"]["folder"] == "new-folder"
    assert store.dialog_names["id"]["telegram_online_status"] == "recently"


def test_top_dialogs_by_last_online_sorts_desc():
    store = Store(filename=None)
    store.add_dialog("1", "older", "f1")
    store.add_dialog("2", "newer", "f2")

    store.set_dialog_status(
        "1", telegram_online_status="offline", last_online="2024-01-01 00:00:00+0000"
    )
    store.set_dialog_status(
        "2", telegram_online_status="online", last_online="2025-01-01 00:00:00+0000"
    )

    top = store.top_dialogs_by_last_online(limit=2)
    assert [d["id"] for d in top] == [2, 1]
