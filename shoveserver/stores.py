import hashlib
import functools
import struct
import time
import uuid

import eventlet
from shoveserver import exceptions

TS_HORIZON = 2592000 # 30 days into the future

def consumedata(file, bytes):
    data = file.read(bytes)
    assert file.read(2) == '\r\n', 'error parsing data'
    return data

class DictStore(object):
    def __init__(self, store, flagsupport=True, expirysupport=True):
        self.store = store
        self.flagsupport = flagsupport
        self.expirysupport = expirysupport

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
                 'data': data
                } for key, flags, data in data]

    gets = get
        
    def set(self, file, key, flags, exptime, bytes,
                replace=False, prepend=False, append=False, add=False):
        data = consumedata(file, int(bytes))
        if replace and key not in self.store:
            raise exceptions.NotStoredError(key)
        if add and key in self.store:
            raise exceptions.NotStoredError(key)
        if prepend:
            data = '%s%s' % (data, self.store.get(key, ''))
        if append:
            data = '%s%s' % (self.store.get(key, ''), data)
        data = self.package(key, data, flags, exptime)
        self.store[key] = data
    
    def delete(self, file, key, time=None):
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
        delay = int(delay)
        if delay > 0:
            eventlet.spawn_after(delay, self.store.clear)
        else:
            self.store.clear()
            

class MCEmulationStore(DictStore):
    dataheader = struct.Struct('!Li')

    def package(self, key, data, flags, exptime):
        exptime = int(exptime)
        flags = int(flags)
        if 0 < exptime <= TS_HORIZON:
            # if less than ts_horizon consider relative
            exptime = int(time.time()) + exptime
        header = self.dataheader.pack(exptime, flags)
        unique = uuid.uuid1().bytes
        unique = hashlib.sha1(unique).digest()[-8:]
        return '%s%s%s' % (header, unique, data)

    def unpackage(self, key, data):
        hsize = self.dataheader.size
        exptime, flags = self.dataheader.unpack(data[:hsize])
        unique, data = data[:8], data[8:]
        return flags, exptime, data[hsize:]
        
    def checkexpiry(self, exptime):
        return exptime == 0 or exptime > time.time()


