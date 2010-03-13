import operator
import re

from shoveserver import exceptions

def compile(*commands):
    a = lambda exp: re.compile(exp).search
    b = lambda key: operator.attrgetter(key)
    c = lambda i, j, k: (a(j), b(i), k)
    d = ((key, c(key, j, k)) for i,j,k in commands for key in i)
    return dict(d)

ERROR = 'ERROR'
SERVER_ERROR = 'SERVER_ERROR %s\r\n'
CLIENT_ERROR = 'CLIENT_ERROR %s\r\n'

STORED = 'STORED'
NOT_FOUND = 'NOT_FOUND'
NOT_STORED = 'NOT_STORED'
DELETED = 'DELETED'
OK = 'OK'

VALUE_FORMAT = 'VALUE %(key)s %(flags)d %(length)d\r\n%(data)s\r\n'
VALUE_END = 'END'

def format_values(values):
    data = map(lambda d: VALUE_FORMAT % d, values)
    return '%s%s' % (
        ''.join(data),
        VALUE_END
    )

SUPPORTED_CMDS = compile(
    (['set', 'replace', 'append', 'prepend', 'add'], r'^(?P<key>\S{1,250}) (?P<flags>\d+) (?P<exptime>\d+) (?P<bytes>\d+)(?: (?P<noreply>noreply))?$', STORED),
    (['get', 'gets'], r'^(?P<keys>\S{1,250}(?:\s\S{1,250})*)$', format_values),
    (['delete'], r'^(?P<key>\S{1,250})(?: (?P<time>\d+))?(?: (?P<noreply>noreply))?$', DELETED),
    (['incr', 'decr'], r'^(?P<key>\S{1,250}) (?P<value>\d+)(?: (?P<noreply>noreply))?$', None),
    (['flush_all'], r'^(?P<delay>\d+)?$', OK),
)

class MemcacheServer(object):
    def __init__(self, store):
        self.store = store

    def __call__(self, sock, addr):
        iofile = sock.makefile('rw')
        try:
            self.handle_connection(iofile, iofile)
        finally:
            iofile.close()
            sock.close()

    def attempt_command(self, command, sfr):
        cmd, _, args = command.partition(' ')
        try:
            rule, func, cb = SUPPORTED_CMDS[cmd]
        except KeyError, e:
            return ERROR

        match = rule(args)
        if not match:
            return CLIENT_ERROR % 'invalid args'

        kwargs = match.groupdict()
        noreply = kwargs.pop('noreply', None) == 'noreply'
        try:
            func = func(self.store)
        except AttributeError, e:
            # not supported
            resp = ERROR

        try:
            resp = func(sfr, **kwargs)
            if cb is not None:
                if isinstance(cb, basestring):
                    resp = cb
                elif callable(cb):
                    resp = cb(resp)
                else:
                    resp = SERVER_ERROR % 'misconfigured server'

        except AssertionError, e:
            resp = CLIENT_ERROR % e
        except exceptions.NotFoundError, e:
            resp = NOT_FOUND
        except exceptions.NotStoredError, e:
            resp = NOT_STORED
        except Exception, e:
            print 'error', e
            resp = SERVER_ERROR % 'unhandled exception'

        return '' if noreply else '%s\r\n' % resp

    def handle_connection(self, ifile, ofile):
        while True:
            command = ifile.readline().rstrip()
            if not command:
                break
            if command == 'quit':
                break
            response = self.attempt_command(command, ifile)
            if response:
                ofile.write(response)
                ofile.flush()


if __name__ == '__main__':
    import eventlet
    from shoveserver import stores
    sock = eventlet.listen(('0.0.0.0', 5211))
    store = {}
    def statdump(store):
        from datetime import datetime
        while 1:
            print '%s keys: %d' % (datetime.now().isoformat(' '),
                len(store.keys()))
            eventlet.sleep(10)
    eventlet.spawn(statdump, store)
    eventlet.serve(sock, MemcacheServer(stores.DictStore(store)))

