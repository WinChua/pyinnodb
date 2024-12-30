from enum import Enum
from collections import namedtuple
from datetime import datetime, date, timedelta


class DDColumnType(Enum):
    """from sql/dd/types/column.h::enum_column_type"""

    DECIMAL = 1
    TINY = 2
    SHORT = 3
    LONG = 4
    FLOAT = 5
    DOUBLE = 6
    TYPE_NULL = 7
    TIMESTAMP = 8
    LONGLONG = 9
    INT24 = 10
    DATE = 11
    TIME = 12
    DATETIME = 13
    YEAR = 14
    NEWDATE = 15
    VARCHAR = 16
    BIT = 17
    TIMESTAMP2 = 18
    DATETIME2 = 19
    TIME2 = 20
    NEWDECIMAL = 21
    ENUM = 22
    SET = 23
    TINY_BLOB = 24
    MEDIUM_BLOB = 25
    LONG_BLOB = 26
    BLOB = 27
    VAR_STRING = 28
    STRING = 29
    GEOMETRY = 30
    JSON = 31
    VECTOR = 32

    def is_int_number(self):
        return self in _int_number_type

    @classmethod
    def is_number(cls, t):
        return cls(t) in _number_type

    @classmethod
    def is_var(cls, t, mysqld_version=None):
        tt = cls(t)
        if tt != DDColumnType.STRING:
            return tt in _var_type
        else:
            if mysqld_version is None:
                return False
            elif mysqld_version < 80000:
                return False
            else:
                return True

    @classmethod
    def is_string(cls, t):
        tt = cls(t)
        return tt in _string_type

    @classmethod
    def is_big(cls, t):
        return cls(t) in _big_type


_int_number_type = [
    DDColumnType.TINY,
    DDColumnType.SHORT,
    DDColumnType.LONG,
    DDColumnType.LONGLONG,
    DDColumnType.INT24,
]

_number_type = [
    DDColumnType.DECIMAL,
    DDColumnType.TINY,
    DDColumnType.SHORT,
    DDColumnType.LONG,
    DDColumnType.FLOAT,
    DDColumnType.DOUBLE,
]

_var_type = [
    DDColumnType.VARCHAR,
    DDColumnType.MEDIUM_BLOB,
    DDColumnType.LONG_BLOB,
    DDColumnType.BLOB,
    # DDColumnType.STRING,
    DDColumnType.JSON,
    DDColumnType.TINY_BLOB,
    DDColumnType.GEOMETRY,
    DDColumnType.VECTOR,
]

_string_type = [
    DDColumnType.VARCHAR,
    DDColumnType.STRING,
    DDColumnType.VAR_STRING,
    DDColumnType.BLOB,
    DDColumnType.MEDIUM_BLOB,
    DDColumnType.LONG_BLOB,
    DDColumnType.TINY_BLOB,
]

_big_type = [
    DDColumnType.MEDIUM_BLOB,
    DDColumnType.LONG_BLOB,
    DDColumnType.BLOB,
    DDColumnType.JSON,
    DDColumnType.GEOMETRY,
    DDColumnType.VECTOR,
]

DDColConf = namedtuple("DDColConf", "type size pytype")

nop = namedtuple("nop", "")

class DDColConf(DDColConf, Enum):
    DECIMAL = DDColumnType.DECIMAL, 0, float
    TINY = DDColumnType.TINY, 1, int
    SHORT = DDColumnType.SHORT, 2, int
    LONG = DDColumnType.LONG, 4, int
    FLOAT = DDColumnType.FLOAT, 4, float
    DOUBLE = DDColumnType.DOUBLE, 8, float
    TYPE_NULL = DDColumnType.TYPE_NULL, 0, int
    TIMESTAMP = DDColumnType.TIMESTAMP, 0, int
    LONGLONG = DDColumnType.LONGLONG, 8, int
    INT24 = DDColumnType.INT24, 3, int
    DATE = DDColumnType.DATE, 0, date
    TIME = DDColumnType.TIME, 0, timedelta
    DATETIME = DDColumnType.DATETIME, 0, datetime
    YEAR = DDColumnType.YEAR, 1, int
    NEWDATE = DDColumnType.NEWDATE, 3, date
    VARCHAR = DDColumnType.VARCHAR, 0, str
    BIT = DDColumnType.BIT, 0, int
    TIMESTAMP2 = DDColumnType.TIMESTAMP2, 0, int
    DATETIME2 = DDColumnType.DATETIME2, 0, datetime
    TIME2 = DDColumnType.TIME2, 0, timedelta
    NEWDECIMAL = DDColumnType.NEWDECIMAL, 0, float
    ENUM = DDColumnType.ENUM, 0, str
    SET = DDColumnType.SET, 0, set
    TINY_BLOB = DDColumnType.TINY_BLOB, 0, str
    MEDIUM_BLOB = DDColumnType.MEDIUM_BLOB, 0, str
    LONG_BLOB = DDColumnType.LONG_BLOB, 0, str
    BLOB = DDColumnType.BLOB, 0, str
    VAR_STRING = DDColumnType.VAR_STRING, 0, str
    STRING = DDColumnType.STRING, 0, str
    GEOMETRY = DDColumnType.GEOMETRY, 0, nop
    JSON = DDColumnType.JSON, 0, str
    VECTOR = DDColumnType.VECTOR, 0, list

    @classmethod
    def get_col_type_conf(cls, type) -> DDColConf:
        if getattr(cls, "_map", None) is None:
            cls._map = {}
            for e in cls:
                cls._map[e.value.type.value] = e.value
        return cls._map[type]
