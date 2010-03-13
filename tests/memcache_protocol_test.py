import eventlet
from shoveserver import server
from shoveserver import stores

tests = [
    # get a single non-existant key
    [('get testkey\r\n', 'END\r\n'),],
    # get multiple non-existant ket
    [('get testkey1 testkey2\r\n', 'END\r\n'),],
    # set a key and retrieve
    [('set testkey1 0 0 6\r\nfoobar\r\n', 'STORED\r\n'),
        ('get testkey1', 'VALUE testkey1 0 6\r\nfoobar\r\nEND\r\n')],
    # set a single key and retrieve multiple (of which the others don't exist)
    [('set testkey1 0 0 6\r\nfoobar\r\n', 'STORED\r\n'),
        ('get testkey1 testkey2 testkey3', 'VALUE testkey1 0 6\r\nfoobar\r\nEND\r\n')],
    # set two keys and get 3 - the 2 inserted and a third nonexistant one
    [('set testkey1 0 0 6\r\nfoobar\r\n', 'STORED\r\n'),
        ('set testkey2 0 0 6\r\nbarbaz\r\n', 'STORED\r\n'),
        ('get testkey1 testkey2 testkey3', 'VALUE testkey1 0 6\r\nfoobar\r\n'
                                           'VALUE testkey2 0 6\r\nbarbaz\r\n'
                                           'END\r\n')],
]

def test_generator():
    for test in tests:
        yield docheck, test

def docheck(commands):
    from StringIO import StringIO
    store = stores.DictStore({})
    serverfunc = server.MemcacheServer(store)
    for request, response in commands:
        request = StringIO(request)
        respio = StringIO()
        serverfunc.handle_connection(request, respio)
        data = respio.getvalue()
        assert data == response, '%r != expected %r' % (data, response)

