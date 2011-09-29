import operator
import re

import eventlet
from shoveserver import exceptions
from shoveserver.protocol_strings import *


def compile(*commands):
    a = lambda exp: re.compile(exp).search
    b = lambda key: operator.attrgetter(key)
    c = lambda i, j, k: (a(j), b(i), k)
    d = ((key, c(key, j, k)) for i, j, k in commands for key in i)
    return dict(d)


def format_values(values):
    data = map(lambda d: VALUE_FORMAT % d, values)
    return '%s%s' % (
        ''.join(data),
        VALUE_END)

SUPPORTED_CMDS = compile(
    (['set', 'replace', 'append', 'prepend', 'add'],
        r'^(?P<key>\S{1,250}) (?P<flags>\d+) (?P<exptime>\d+)'
        ' (?P<bytes>\d+)(?: (?P<noreply>noreply))?$', STORED),
    (['get', 'gets'], r'^(?P<keys>\S{1,250}(?:\s\S{1,250})*)$', format_values),
    (['delete'],
        r'^(?P<key>\S{1,250})(?: (?P<time>\d+))?(?: (?P<noreply>noreply))?$',
        DELETED),
    (['incr', 'decr'],
        r'^(?P<key>\S{1,250}) (?P<value>\d+)(?: (?P<noreply>noreply))?$',
        None),
    (['flush_all'], r'^(?P<delay>\d+)?$', OK),
)


class MemcacheServer(object):
    def __init__(self, store):
        self.store = store
        self.commands = SUPPORTED_CMDS.copy()
        if hasattr(store, 'EXTRA_CMDS'):
            for cmd in store.EXTRA_CMDS:
                self.add_command(*cmd)

    def __call__(self, sock, addr):
        iofile = sock.makefile('rw')
        try:
            self.handle_connection(iofile, iofile)
        finally:
            iofile.close()
            sock.close()

    def add_command(self, commands, regexp, response):
        if isinstance(commands, basestring):
            commands = [commands]
        newcmds = compile((commands, regexp, response))
        self.commands.update(newcmds)

    def attempt_command(self, command, sfr):
        cmd, _, args = command.partition(' ')
        try:
            rule, func, cb = self.commands[cmd]
        except KeyError, e:
            return '%s\r\n' % ERROR

        match = rule(args)
        if not match:
            return '%s\r\n' % (CLIENT_ERROR % 'invalid args')

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
        except exceptions.MemcacheProtocolException, e:
            resp = e.response
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


def serve_store(sock, store, server=None):
    if isinstance(sock, tuple):
        sock = eventlet.listen(sock)
    if not server:
        server = MemcacheServer(store)
    eventlet.serve(sock, server)

if __name__ == '__main__':
    import eventlet
    from shoveserver import stores
    store = {}

    def statdump(store):
        from datetime import datetime
        while 1:
            print '%s keys: %d' % (datetime.now().isoformat(' '),
                len(store.keys()))
            eventlet.sleep(10)

    eventlet.spawn(statdump, store)
    serve_store(('0.0.0.0', 11211), stores.Store(store, writeable=True))
