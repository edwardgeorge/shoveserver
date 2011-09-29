import hashlib
import functools
import struct
import time
import uuid

import eventlet
from shoveserver import exceptions

__all__ = ['BaseStore', 'Store']

TS_HORIZON = 2592000  # 30 days into the future


def consumedata(file, bytes):
    data = file.read(bytes)
    assert file.read(2) == '\r\n', 'error parsing data'
    return data


class BaseStore(object):
    def __init__(self, store, writeable=False):
        self.store = store
        self.readonly = not writeable

    def unpackage(self, key, data):
        return 0, 0, data

    def package(self, key, data, flags, exptime):
        return data

    def checkexpiry(self, exptime):
        return True

    def _get_individual(self, keys):
        for key in keys.split(' '):
            try:
                data = self.store[key]
            except KeyError, e:
                pass
            else:
                flags, exptime, data = self.unpackage(key, data)
                if self.checkexpiry(exptime):
                    yield key, flags, data

    def get(self, file, keys):
        format = 'VALUE %s %d %d\r\n%s\r\n'
        data = self._get_individual(keys)
        return [{'key': key,
                 'flags': flags,
                 'length': len(data),
                 'data': data} for key, flags, data in data]

    gets = get

    def set(self, file, key, flags, exptime, bytes,
                replace=False, prepend=False, append=False, add=False):
        data = consumedata(file, int(bytes))
        if self.readonly:
            raise exceptions.UnsupportedCommandError()
        if replace and key not in self.store:
            raise exceptions.NotStoredError(key)
        if add and key in self.store:
            raise exceptions.NotStoredError(key)
        if prepend:
            data = '%s%s' % (data, self.store.get(key, ''))
        if append:
            data = '%s%s' % (self.store.get(key, ''), data)
        self._set(key, data, flags, exptime)

    def _set(self, key, data, flags, exptime):
        data = self.package(key, data, flags, exptime)
        self.store[key] = data

    def delete(self, file, key, time=None):
        if self.readonly:
            raise exceptions.UnsupportedCommandError()
        try:
            del self.store[key]
        except KeyError, e:
            # relies on backend to raise if non-existant
            raise exceptions.NotFoundError(key)

    def replace(self, file, key, flags, exptime, bytes):
        return self.set(file, key, flags, exptime, bytes, replace=True)

    def add(self, file, key, flags, exptime, bytes):
        return self.set(file, key, flags, exptime, bytes, add=True)

    def append(self, file, key, flags, exptime, bytes):
        return self.set(file, key, flags, exptime, bytes, append=True)

    def prepend(self, file, key, flags, exptime, bytes):
        return self.set(file, key, flags, exptime, bytes, prepend=True)

    def incr(self, file, key, value):
        if self.readonly:
            raise exceptions.UnsupportedCommandError()
        assert value.isdigit(), 'value must be an integer'
        value = int(value)
        try:
            prev = int(self.store[key])
        except KeyError, e:
            raise exceptions.NotFoundError(key)
        except (TypeError, ValueError), e:
            prev = 0
        val = self.store[key] = str(prev + value)
        return val

    def decr(self, file, key, value):
        if self.readonly:
            raise exceptions.UnsupportedCommandError()
        assert value.isdigit(), 'value must be an integer'
        value = int(value)
        try:
            prev = int(self.store[key])
        except KeyError, e:
            raise exceptions.NotFoundError(key)
        except (TypeError, ValueError), e:
            prev = 0
        val = self.store[key] = str(max(prev - value, 0))
        return val

    def flush_all(self, file, delay=0):
        if self.readonly:
            raise exceptions.UnsupportedCommandError()
        delay = int(delay)
        if delay > 0:
            eventlet.spawn_after(delay, self.store.clear)
        else:
            self.store.clear()


class Store(BaseStore):
    pass
