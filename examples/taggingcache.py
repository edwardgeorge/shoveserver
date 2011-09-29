"""Tagging store example for shoveserver.

implements the commands added in http://code.google.com/p/memcached-tag/

 - tag_add <tag> <key>
 - tag_delete <tag>

"""
import shoveserver
from shoveserver import exceptions

TAG_STORED = 'TAG_STORED'
TAG_DELETED = 'TAG_DELETED'
TAG_NOT_FOUND = 'TAG_NOT_FOUND'


class TagNotFoundError(exceptions.MemcacheProtocolException):
    response = TAG_NOT_FOUND


class TaggingStore(shoveserver.stores.Store):
    EXTRA_CMDS = [
      (['tag_add'], r'^(?P<tag>\S{1,250})\s(?P<key>\S{1,250})$', TAG_STORED),
      (['tag_delete'], r'^(?P<tag>\S{1,250})$', TAG_DELETED), ]

    def __init__(self, *args, **kwargs):
        super(TaggingStore, self).__init__(*args, **kwargs)
        self.tags = {}
        self.key_tags = {}

    def tag_add(self, file, tag, key):
        if not key in self.store:
            raise TagNotFoundError(key)
        self.tags.setdefault(tag, set()).add(key)
        self.key_tags.setdefault(key, set()).add(tag)

    def tag_delete(self, file, tag):
        try:
            tagged = self.tags.pop(tag)
        except KeyError, e:
            raise TagNotFoundError(tag)
        else:
            for key in tagged:
                del self.store[key]
                self.key_tags[key].remove(tag)

    def delete(self, file, key, *args, **kwargs):
        super(TaggingStore, self).delete(file, key, *args, **kwargs)
        tags = self.key_tags.pop(key, [])
        for tag in tags:
            tagkeys = self.tags[tag]
            tagkeys.remove(key)
            if not tagkeys:
                del self.tags[tag]


def taggingserver(store):
    server = shoveserver.server.MemcacheServer(store)
    return server


def serve(d, host='127.0.0.1', port=11211):
    store = TaggingStore(d)
    sock = eventlet.listen((host, port))
    server = taggingserver(store)
    shoveserver.server.serve_store(sock, store, server=server)


if __name__ == '__main__':
    serve({})
