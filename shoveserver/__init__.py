import eventlet
from shoveserver import server
from shoveserver import stores


def spawn_server(addr, store, **kwargs):
    if not isinstance(store, stores.BaseStore):
        store = stores.Store(store, **kwargs)
    sock = eventlet.listen(addr)
    eventlet.spawn(server.serve_store, sock, store)

