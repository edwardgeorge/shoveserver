import eventlet
from shoveserver import server
from shoveserver import stores

from tests import server_request

dict_tests = [
    # get a single non-existant key
    [('get testkey\r\n', 'END\r\n'), ],
    # get multiple non-existant key
    [('get testkey1 testkey2\r\n', 'END\r\n'), ],
    # set a key and retrieve
    [('set testkey1 0 0 6\r\nfoobar\r\n', 'STORED\r\n'),
        ('get testkey1', 'VALUE testkey1 0 6\r\nfoobar\r\nEND\r\n')],
    # set a single key and retrieve multiple (of which the others don't exist)
    [('set testkey1 0 0 6\r\nfoobar\r\n', 'STORED\r\n'),
        ('get testkey1 testkey2 testkey3',
         'VALUE testkey1 0 6\r\nfoobar\r\nEND\r\n')],
    # set two keys and get 3 - the 2 inserted and a third nonexistant one
    [('set testkey1 0 0 6\r\nfoobar\r\n', 'STORED\r\n'),
        ('set testkey2 0 0 6\r\nbarbaz\r\n', 'STORED\r\n'),
        ('get testkey1 testkey2 testkey3', 'VALUE testkey1 0 6\r\nfoobar\r\n'
                                           'VALUE testkey2 0 6\r\nbarbaz\r\n'
                                           'END\r\n')],
    # delete a non-existant key
    [('delete testkey\r\n', 'NOT_FOUND\r\n'), ],
    # delete a key
    [('set testkey 0 0 9\r\ntest data\r\n', 'STORED\r\n'),
        ('delete testkey\r\n', 'DELETED\r\n'),
        ('get testkey\r\n', 'END\r\n'), ],
    # incrementing...
    [('set testkey 0 0 1\r\n5\r\n\r', 'STORED\r\n'),
        ('incr testkey 1\r\n', '6\r\n'),
        ('get testkey\r\n', 'VALUE testkey 0 1\r\n6\r\nEND\r\n'), ],
    [('set testkey 0 0 1\r\n3\r\n\r', 'STORED\r\n'),
        ('incr testkey 4\r\n', '7\r\n'),
        ('get testkey\r\n', 'VALUE testkey 0 1\r\n7\r\nEND\r\n'), ],
    # increment non-existing key
    [('incr testkey 1\r\n', 'NOT_FOUND\r\n'), ],
    # increment a non-integer value
    [('set testkey 0 0 3\r\nfoo\r\n\r', 'STORED\r\n'),
        ('incr testkey 4\r\n', '4\r\n'), ],
    # increment with a non-integer value
    [('set testkey 0 0 1\r\n4\r\n\r', 'STORED\r\n'),
        ('incr testkey bah\r\n', 'CLIENT_ERROR invalid args\r\n'), ],
    # decrementing...
    [('set testkey 0 0 1\r\n5\r\n\r', 'STORED\r\n'),
        ('decr testkey 1\r\n', '4\r\n'),
        ('get testkey\r\n', 'VALUE testkey 0 1\r\n4\r\nEND\r\n'), ],
    [('set testkey 0 0 1\r\n5\r\n\r', 'STORED\r\n'),
        ('decr testkey 4\r\n', '1\r\n'),
        ('get testkey\r\n', 'VALUE testkey 0 1\r\n1\r\nEND\r\n'), ],
    # decrement non-existant key
    [('decr testkey 1\r\n', 'NOT_FOUND\r\n'), ],
    # decrement a value of zero
    [('set testkey 0 0 1\r\n0\r\n\r', 'STORED\r\n'),
        ('decr testkey 1\r\n', '0\r\n'),
        ('get testkey\r\n', 'VALUE testkey 0 1\r\n0\r\nEND\r\n'), ],
    # decrement a non-integer value
    [('set testkey 0 0 3\r\nfoo\r\n\r', 'STORED\r\n'),
        ('decr testkey 4\r\n', '0\r\n'), ],
    # decrement with a non-integer value
    [('set testkey 0 0 1\r\n4\r\n\r', 'STORED\r\n'),
        ('decr testkey bah\r\n', 'CLIENT_ERROR invalid args\r\n'), ],
    # appending...
    [('set testkey 0 0 3\r\nfoo\r\n', 'STORED\r\n'),
        ('append testkey 0 0 3\r\nbar\r\n', 'STORED\r\n'),
        ('get testkey\r\n', 'VALUE testkey 0 6\r\nfoobar\r\nEND\r\n'), ],
    # prepending...
    [('set testkey 0 0 3\r\nfoo\r\n', 'STORED\r\n'),
        ('prepend testkey 0 0 3\r\nbar\r\n', 'STORED\r\n'),
        ('get testkey\r\n', 'VALUE testkey 0 6\r\nbarfoo\r\nEND\r\n'), ],
    # append to a non-existant key
    [('append testkey 0 0 2\r\nla\r\n', 'STORED\r\n'),
        ('get testkey\r\n', 'VALUE testkey 0 2\r\nla\r\nEND\r\n'), ],
    # prepend to a non-existant key
    [('prepend testkey 0 0 2\r\nla\r\n', 'STORED\r\n'),
        ('get testkey\r\n', 'VALUE testkey 0 2\r\nla\r\nEND\r\n'), ],
    # succesful add
    [('add testkey 0 0 2\r\nla\r\n', 'STORED\r\n'), ],
    # add when key exists
    [('set testkey 0 0 2\r\nla\r\n', 'STORED\r\n'),
        ('add testkey 0 0 2\r\nla\r\n', 'NOT_STORED\r\n'), ],
    # replacing a value
    [('set testkey 0 0 2\r\nla\r\n', 'STORED\r\n'),
        ('replace testkey 0 0 3\r\nfoo\r\n', 'STORED\r\n'),
        ('get testkey\r\n', 'VALUE testkey 0 3\r\nfoo\r\nEND\r\n'), ],
    # replacing a non-existant value
    [('replace testkey 0 0 3\r\nfoo\r\n', 'NOT_STORED\r\n'), ],
    # non-existant command
    [('yoink testkey\r\n', 'ERROR\r\n'), ],
    # noreply tests
    [('set testkey 0 0 4 noreply\r\nblah\r\n', ''), ],
    [('delete testkey noreply\r\n', ''), ],
    [('set testkey 0 0 4\r\nblah\r\n', 'STORED\r\n'),
        ('delete testkey noreply\r\n', ''), ],
    [('incr testkey 4 noreply\r\n', ''), ],
    [('decr testkey 4 noreply\r\n', ''), ],
    [('set testkey 0 0 1\r\n2\r\n', 'STORED\r\n'),
        ('incr testkey 4 noreply\r\n', ''), ],
    [('set testkey 0 0 1\r\n2\r\n', 'STORED\r\n'),
        ('decr testkey 4 noreply\r\n', ''), ],
]

readonly_tests = [
    # readonly tests
    [('set testkey 0 0 4\r\nblah\r\n', 'ERROR\r\n'), ],
    [('replace testkey 0 0 4\r\nblah\r\n', 'ERROR\r\n'), ],
    [('add testkey1 0 0 4\r\nblah\r\n', 'ERROR\r\n'), ],
    [('prepend testkey 0 0 4\r\nblah\r\n', 'ERROR\r\n'), ],
    [('append testkey 0 0 4\r\nblah\r\n', 'ERROR\r\n'), ],
    [('delete testkey\r\n', 'ERROR\r\n'), ],
    [('incr testkey 4\r\n', 'ERROR\r\n'), ],
    [('decr testkey 4\r\n', 'ERROR\r\n'), ],
]

tests = [
    (dict_tests, stores.Store, {}, {'writeable': True}),
    (readonly_tests, stores.Store, {'testkey': 'testdata'}, {}),
]


def test_generator():
    for testspecs, storeclass, initdata, args in tests:
        for test in testspecs:
            yield docheck, test, storeclass, initdata, args


def docheck(commands, storeclass=stores.Store,
                      initialdata={},
                      storeargs={}):
    store = storeclass(initialdata.copy(), **storeargs)
    serverfunc = server.MemcacheServer(store)
    for request, response in commands:
        data = server_request(serverfunc, request)
        assert data == response, '%r != expected %r' % (data, response)
