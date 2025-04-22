from enum import Enum
import time
import random
from collections import namedtuple
from datetime import datetime, date, timedelta
from base64 import b64decode


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

DDColConf = namedtuple("DDColConf", "type size pytype rand_func")

nop = namedtuple("nop", "")

def rand_none(col):
    return None

class DDColConf(DDColConf, Enum):
    DECIMAL = DDColumnType.DECIMAL, 0, float, lambda col: random.random()
    TINY = DDColumnType.TINY, 1, int, lambda col: random.randint(0, 2**8 - 1) if col.is_unsigned else random.randint(-2**7, 2**7-1)
    SHORT = DDColumnType.SHORT, 2, int, lambda col: random.randint(0, 2**16 - 1) if col.is_unsigned else random.randint(-2**15, 2**15-1)
    LONG = DDColumnType.LONG, 4, int, lambda col: random.randint(0, 2**32 - 1) if col.is_unsigned else random.randint(-2**31, 2**31-1)
    FLOAT = DDColumnType.FLOAT, 4, float, lambda col: random.random()
    DOUBLE = DDColumnType.DOUBLE, 8, float, lambda col: random.random()
    TYPE_NULL = DDColumnType.TYPE_NULL, 0, int, rand_none
    TIMESTAMP = DDColumnType.TIMESTAMP, 0, int, lambda col: datetime.fromtimestamp(int(time.time()) + random.randint(-100, 100)).strftime("%Y-%m-%d %H:%M:%S")
    LONGLONG = DDColumnType.LONGLONG, 8, int, lambda col: random.randint(0,2**64-1) if col.is_unsigned else random.randint(-2**63, 2**63 - 1)
    INT24 = DDColumnType.INT24, 3, int, lambda col: random.randint(0, 2**24 - 1) if col.is_unsigned else random.randint(-2**23, 2**23-1)
    DATE = DDColumnType.DATE, 0, date, lambda col: date(random.randint(0, 2000), random.randint(1, 12), random.randint(1, 28))
    TIME = DDColumnType.TIME, 0, timedelta, lambda col: timedelta(random.randint(0, 100))
    DATETIME = DDColumnType.DATETIME, 0, datetime, lambda col: datetime(random.randint(1, 10), random.randint(1, 10), random.randint(1, 10))
    YEAR = DDColumnType.YEAR, 1, int, lambda col: random.randint(1901, 2155)
    NEWDATE = DDColumnType.NEWDATE, 3, date, lambda col: date(random.randint(0, 2000), random.randint(1, 12), random.randint(1, 28))
    VARCHAR = DDColumnType.VARCHAR, 0, str, lambda col: random.randbytes(col.varchar_size).hex()
    BIT = DDColumnType.BIT, 0, int, lambda col: random.randint(0,1)
    TIMESTAMP2 = DDColumnType.TIMESTAMP2, 0, int, lambda col: datetime.fromtimestamp(int(time.time()) + random.randint(-100, 100)).strftime("%Y-%m-%d %H:%M:%S")
    DATETIME2 = DDColumnType.DATETIME2, 0, datetime, lambda col: datetime(random.randint(1, 10), random.randint(1, 10), random.randint(1, 10))
    TIME2 = DDColumnType.TIME2, 0, timedelta, lambda col: f"{random.randint(-838, 838)}:{random.randint(0,59):02}:{random.randint(0,59):02}.{random.randint(0,30000)}"
    NEWDECIMAL = DDColumnType.NEWDECIMAL, 0, float, lambda col: random.random()
    ENUM = DDColumnType.ENUM, 0, str, lambda col : b64decode(random.choice(col.elements).name).decode(errors="replace")
    SET = DDColumnType.SET, 0, set, lambda col : b64decode(random.choice(col.elements).name).decode(errors="replace")
    TINY_BLOB = DDColumnType.TINY_BLOB, 0, str, lambda col: ""
    MEDIUM_BLOB = DDColumnType.MEDIUM_BLOB, 0, str, lambda col: ""
    LONG_BLOB = DDColumnType.LONG_BLOB, 0, str, lambda col: ""
    BLOB = DDColumnType.BLOB, 0, str, lambda col: ""
    VAR_STRING = DDColumnType.VAR_STRING, 0, str, lambda col: ""
    STRING = DDColumnType.STRING, 0, str, lambda col: ""
    GEOMETRY = DDColumnType.GEOMETRY, 0, nop, rand_none
    JSON = DDColumnType.JSON, 0, str, lambda col: "{}"
    VECTOR = DDColumnType.VECTOR, 0, list, lambda col: []

    @classmethod
    def get_col_type_conf(cls, type) -> DDColConf:
        if getattr(cls, "_map", None) is None:
            cls._map = {}
            for e in cls:
                cls._map[e.value.type.value] = e.value
        return cls._map[type]

    @classmethod
    def rand_data(cls, type):
        pass
