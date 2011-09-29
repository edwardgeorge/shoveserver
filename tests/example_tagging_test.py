import functools

from examples import taggingcache
from tests import server_request


def test_tagging_key():
    data = {}
    store = taggingcache.TaggingStore(data, writeable=True)
    server = taggingcache.taggingserver(store)
    resp = server_request(server, 'set testkey 0 0 4\r\n'
                                  'blah\r\n')
    assert resp == 'STORED\r\n'

    resp = server_request(server, 'tag_add testtag testkey\r\n')
    assert resp == 'TAG_STORED\r\n'


def test_deleting_tag():
    data = {}
    store = taggingcache.TaggingStore(data, writeable=True)
    server = taggingcache.taggingserver(store)
    resp = server_request(server, 'set testkey 0 0 4\r\n'
                                  'blah\r\n')
    assert resp == 'STORED\r\n'

    resp = server_request(server, 'tag_add testtag testkey\r\n')
    assert resp == 'TAG_STORED\r\n'

    resp = server_request(server, 'tag_delete testtag\r\n')
    assert resp == 'TAG_DELETED\r\n', resp

    resp = server_request(server, 'get testkey\r\n')
    assert resp == 'END\r\n', resp


def test_deleting_tag_with_multiple_keys():
    data = {}
    store = taggingcache.TaggingStore(data, writeable=True)
    server = taggingcache.taggingserver(store)
    resp = server_request(server, 'set testkey1 0 0 4\r\n'
                                  'blah\r\n')
    assert resp == 'STORED\r\n'

    resp = server_request(server, 'set testkey2 0 0 4\r\n'
                                  'blah\r\n')
    assert resp == 'STORED\r\n'

    resp = server_request(server, 'tag_add testtag testkey1\r\n')
    assert resp == 'TAG_STORED\r\n'

    resp = server_request(server, 'tag_add testtag testkey2\r\n')
    assert resp == 'TAG_STORED\r\n'

    resp = server_request(server, 'tag_delete testtag\r\n')
    assert resp == 'TAG_DELETED\r\n', resp

    resp = server_request(server, 'get testkey1\r\n')
    assert resp == 'END\r\n', resp

    resp = server_request(server, 'get testkey2\r\n')
    assert resp == 'END\r\n', resp


def test_deleting_nonexistent_tag():
    data = {}
    store = taggingcache.TaggingStore(data, writeable=True)
    server = taggingcache.taggingserver(store)

    resp = server_request(server, 'tag_delete testtag\r\n')
    assert resp == 'TAG_NOT_FOUND\r\n', resp


def test_adding_tag_to_nonexistant_key():
    data = {}
    store = taggingcache.TaggingStore(data, writeable=True)
    server = taggingcache.taggingserver(store)

    resp = server_request(server, 'tag_add testtag testkey\r\n')
    assert resp == 'TAG_NOT_FOUND\r\n', resp


def test_deleting_key_with_associated_tag():
    data = {}
    store = taggingcache.TaggingStore(data, writeable=True)
    server = taggingcache.taggingserver(store)

    resp = server_request(server, 'set testkey 0 0 4\r\n'
                                  'blah\r\n')
    assert resp == 'STORED\r\n'

    resp = server_request(server, 'tag_add testtag testkey\r\n')
    assert resp == 'TAG_STORED\r\n'

    resp = server_request(server, 'delete testkey\r\n')
    assert resp == 'DELETED\r\n', resp

    assert 'testkey' not in store.key_tags
    assert 'testtag' not in store.tags
