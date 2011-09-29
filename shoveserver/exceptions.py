from shoveserver.protocol_strings import *


class MemcacheProtocolException(Exception):
    response = SERVER_ERROR % 'undefined error occurred'


class NotFoundError(MemcacheProtocolException):
    response = NOT_FOUND


class NotStoredError(MemcacheProtocolException):
    response = NOT_STORED


class UnsupportedCommandError(MemcacheProtocolException):
    response = ERROR
