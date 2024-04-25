from enum import Enum
from collections import namedtuple


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

    def is_int_number(self):
        return self in _int_number_type

    @classmethod
    def is_number(cls, t):
        return cls(t) in _number_type

    @classmethod
    def is_var(cls, t):
        return cls(t) in _var_type

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
    DDColumnType.STRING,
    DDColumnType.JSON,
    DDColumnType.TINY_BLOB,
]

_big_type = [
    DDColumnType.MEDIUM_BLOB,
    DDColumnType.LONG_BLOB,
    DDColumnType.BLOB,
    DDColumnType.JSON,
]

DDColConf = namedtuple("DDColConf", "type size")


class DDColConf(DDColConf, Enum):
    DECIMAL = DDColumnType.DECIMAL, 0
    TINY = DDColumnType.TINY, 1
    SHORT = DDColumnType.SHORT, 2
    LONG = DDColumnType.LONG, 4
    FLOAT = DDColumnType.FLOAT, 4
    DOUBLE = DDColumnType.DOUBLE, 8
    TYPE_NULL = DDColumnType.TYPE_NULL, 0
    TIMESTAMP = DDColumnType.TIMESTAMP, 0
    LONGLONG = DDColumnType.LONGLONG, 8
    INT24 = DDColumnType.INT24, 3
    DATE = DDColumnType.DATE, 0
    TIME = DDColumnType.TIME, 0
    DATETIME = DDColumnType.DATETIME, 0
    YEAR = DDColumnType.YEAR, 1
    NEWDATE = DDColumnType.NEWDATE, 3
    VARCHAR = DDColumnType.VARCHAR, 0
    BIT = DDColumnType.BIT, 0
    TIMESTAMP2 = DDColumnType.TIMESTAMP2, 0
    DATETIME2 = DDColumnType.DATETIME2, 0
    TIME2 = DDColumnType.TIME2, 0
    NEWDECIMAL = DDColumnType.NEWDECIMAL, 0
    ENUM = DDColumnType.ENUM, 0
    SET = DDColumnType.SET, 0
    TINY_BLOB = DDColumnType.TINY_BLOB, 0
    MEDIUM_BLOB = DDColumnType.MEDIUM_BLOB, 0
    LONG_BLOB = DDColumnType.LONG_BLOB, 0
    BLOB = DDColumnType.BLOB, 0
    VAR_STRING = DDColumnType.VAR_STRING, 0
    STRING = DDColumnType.STRING, 0
    GEOMETRY = DDColumnType.GEOMETRY, 0
    JSON = DDColumnType.JSON, 0

    @classmethod
    def get_col_type_conf(cls, type) -> DDColConf:
        if getattr(cls, "_map", None) is None:
            cls._map = {}
            for e in cls:
                cls._map[e.value.type.value] = e.value
        return cls._map[type]
