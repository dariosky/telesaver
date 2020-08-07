from sqlitestorage import Store


def test_know_dialogs():
    store = Store(filename=None)
    assert store.changed is False

    assert store.dialog_names == {}
    store.add_dialog('id', 'name', 'folder/')
    assert 'id' in store.dialog_names
    assert store.dialog_names['id'] == dict(
        name='name',
        folder='folder/'
    )
