from enum import Enum



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

    @classmethod
    def is_number(cls, t):
        return cls(t) in _number_type

    @classmethod
    def is_var(cls, t):
        return cls(t) in _var_type

    @classmethod
    def is_big(cls, t):
        return cls(t) in _big_type


_number_type = [
    DDColumnType.DECIMAL,
    DDColumnType.TINY ,
    DDColumnType.SHORT ,
    DDColumnType.LONG ,
    DDColumnType.FLOAT ,
    DDColumnType.DOUBLE ,
]

_var_type = [
    DDColumnType.VARCHAR,
    DDColumnType.MEDIUM_BLOB,
    DDColumnType.LONG_BLOB,
    DDColumnType.BLOB,
    DDColumnType.STRING,
    DDColumnType.JSON,
]

_big_type = [
    DDColumnType.MEDIUM_BLOB,
    DDColumnType.LONG_BLOB,
    DDColumnType.BLOB,
    DDColumnType.JSON,
]
