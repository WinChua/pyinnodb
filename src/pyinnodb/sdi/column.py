import decimal
import io
import struct
import sys
import typing
import re

if sys.version_info.minor >= 9:
    from functools import cache
else:
    cache = lambda x: x
import dataclasses
from base64 import b64decode
from collections import namedtuple
from dataclasses import dataclass, field

from .. import const
from ..const.dd_column_type import DDColConf, DDColumnType, nop
from ..disk_struct.data import MDate, MDatetime, MGeo, MTime2, MTimestamp
from ..disk_struct.json import MJson
from ..disk_struct.rollback import MRollbackPointer
from ..disk_struct.varsize import OffPagePointer
from .util import modify_init

decimal_leftover_part = {
    0: 0,
    1: 1,
    2: 1,
    3: 2,
    4: 2,
    5: 3,
    6: 3,
    7: 4,
    8: 4,
    9: 4,
}

column_type_size = re.compile("[^(]*[(]([^)]*)[)]")

@modify_init
@dataclass(eq=False)
class IndexElement:
    ordinal_position: int = 0
    length: int = 0
    order: int = 0
    hidden: bool = False
    column_opx: int = 0

@modify_init
@dataclass(eq=False)
class ColumnElement:
    name: str = ""  ## BINARY VARBINARY
    index: int = 0

NewDecimalSize = namedtuple(
    "NewDecimalSize", "intg frac intg0 intg0x frac0 frac0x total"
)

column_spec_size = {"DB_ROW_ID": 6, "DB_TRX_ID": 6, "DB_ROLL_PTR": 7}


@modify_init
@dataclass(eq=False)
class Column:
    name: str = ""
    type: int = 0
    is_nullable: bool = False
    is_zerofill: bool = False
    is_unsigned: bool = False
    is_auto_increment: bool = False
    is_virtual: bool = False
    hidden: int = 0
    ordinal_position: int = 0
    char_length: int = 0
    numeric_precision: int = 0
    numeric_scale: int = 0
    numeric_scale_null: bool = False
    datetime_precision: int = 0
    datetime_precision_null: int = 0
    has_no_default: bool = False
    default_value_null: bool = False
    srs_id_null: bool = False
    srs_id: int = 0
    default_value: str = ""  # binary
    default_value_utf8_null: bool = False
    default_value_utf8: str = ""
    default_option: str = ""
    update_option: str = ""
    comment: str = ""
    generation_expression: str = ""
    generation_expression_utf8: str = ""
    options: str = ""  # properties
    se_private_data: str = ""  # properties
    engine_attribute: str = ""
    secondary_engine_attribute: str = ""
    column_key: int = 0
    column_type_utf8: str = ""
    elements: typing.List[ColumnElement] = dataclasses.field(default_factory=list)
    collation_id: int = 0
    is_explicit_collation: bool = False

    @property
    @cache
    def pytype(self):
        return DDColConf.get_col_type_conf(self.type).pytype

    @property
    @cache
    def dfield(self):
        kw_only = False
        default = dataclasses.MISSING
        if self.pytype == nop:
            kw_only  = True
            default = None
        return field(
            default=default,
            kw_only=kw_only,
            metadata={"col": self},
        )

    def index_prefix(self, ie: IndexElement):
        if ie.length == 4294967295:
            return 0, False
        varlen, prekey_len = 1, 0
        if DDColumnType.is_var(
            self.type
        ):  ## TODO: judge prefix key
            if self.collation_id == 255:
                varlen = 4
            elif DDColumnType(self.type) in [
                DDColumnType.VARCHAR,
                DDColumnType.STRING,
            ] and not self.column_type_utf8.startswith("varb"):
                varlen = 3
            if (
                self.char_length > ie.length
            ):  # the index field data length must small than the original field
                prekey_len = int(ie.length / varlen)
                return prekey_len, True
            else:
                return 0, False
        else:
            return 0, False

    @cache
    def version_valid(self, data_schema_version) -> bool:
        va = int(self.private_data.get("version_added", 0))
        vd = int(self.private_data.get("version_dropped", 0))
        if data_schema_version < va:
            return False
        if vd != 0 and data_schema_version >= vd:
            return False
        return True

    @property
    @cache
    def is_hidden_from_user(self):
        return (
            const.column_hidden_type.ColumnHiddenType(self.hidden)
            != const.column_hidden_type.ColumnHiddenType.HT_VISIBLE
        )

    @property
    @cache
    def is_instant_col(self):
        return (
            "version_added" in self.private_data
            or "version_dropped" in self.private_data
        )

    @property
    @cache
    def is_instant_col_80017(self):
        return "default_null" in self.private_data or "default" in self.private_data

    def get_instant_default(self):
        # if self.default_value_utf8_null:
        #     return "NULL"
        # else:
        #     return self.default_value_utf8
        data = self.private_data.get("default", None)
        if data is None:
            return None
        buf = io.BytesIO(bytes.fromhex(data))
        return self.read_data(buf, len(data))

    @property
    @cache
    def private_data(self):
        data = const.line_to_dict(self.se_private_data, ";", "=")
        return data

    @property
    @cache
    def varchar_size(self):
        s = column_type_size.match(self.column_type_utf8)
        if s is not None:
            return int(int(s.group(1))/2 - 1)
        return int(self.char_length/2) - 1

    def __post_init__(self):
        ce: typing.List[ColumnElement] = [ColumnElement(**e) for e in self.elements]
        self.elements = ce

    def get_collation(self):
        coll = const.get_collation_by_id(self.collation_id)
        return coll

    def gen_sql(self):
        sql = f"`{self.name}` {self.column_type_utf8}"
        if self.collation_id != 255 and self.collation_id != 63:
            collation = const.get_collation_by_id(self.collation_id)
            if DDColumnType.is_string(self.type):
                pass
        sql += f"{'' if self.is_nullable or self.is_virtual else ' NOT NULL'}"
        sql += f"{' AUTO_INCREMENT' if self.is_auto_increment else ''}"
        if self.default_option != "":
            sql += f" DEFAULT ({self.default_option})"
        elif not self.default_value_utf8_null:
            sql += f" DEFAULT '{self.default_value_utf8}'"
        elif len(self.generation_expression) != 0:
            sql += f" GENERATED ALWAYS AS ({self.generation_expression}) {'VIRTUAL' if self.is_virtual else 'STORED'}"
        elif self.default_value_utf8_null and self.is_nullable:
            sql += " DEFAULT NULL"
        if self.update_option != "":
            sql += f" ON UPDATE {self.update_option}"
        sql += " COMMENT '" + self.comment + "'" if self.comment else ""

        if not self.srs_id_null:
            sql += f" /*!80003 SRID {self.srs_id} */"
        if self.is_hidden_from_user:
            sql += " /*!80023 INVISIBLE */"
        if self.engine_attribute != "":
            sql += " /*!80021 ENGINE_ATTRIBUTE */"
        if self.secondary_engine_attribute != "":
            sql += " /*!80021 SECONDARY_ENGINE_ATTRIBUTE */"
        return sql

    @property
    @cache
    def size(self):
        dtype = DDColumnType(self.type)
        if self.name in column_spec_size:
            return column_spec_size[self.name]
        elif dtype in [DDColumnType.TIME2]:
            return 3 + int(self.datetime_precision / 2 + 0.5)  # ceil
        elif dtype == DDColumnType.DATETIME2:
            return 5 + int(self.datetime_precision / 2 + 0.5)
        elif dtype == DDColumnType.TIMESTAMP2:
            return 4 + int(self.datetime_precision / 2 + 0.5)
        elif dtype == DDColumnType.BIT:
            return int((self.numeric_precision + 7) / 8)
        elif dtype == DDColumnType.ENUM:  # value is index
            if len(self.elements) > 0xFF:
                return 2
            return 1
        elif dtype == DDColumnType.SET:  # bit mask
            return int((len(self.elements) + 7) / 8)

        elif dtype == DDColumnType.STRING:  # if column don't have varsize
            sizes = column_type_size.findall(self.column_type_utf8)
            if len(sizes) == 0:
                return 0
            else:
                return int(sizes[0])

        else:
            return DDColConf.get_col_type_conf(self.type).size
            ## if dtype == DDColumnType.FLOAT:
            ##     ## https://dev.mysql.com/doc/refman/8.0/en/fixed-point-types.html
            ##     ## https://dev.mysql.com/doc/refman/8.0/en/floating-point-types.html
            ##     ## https://stackoverflow.com/questions/10993501/mysql-float-storage-size
            ##     return 4
            ##     return 4 if self.numeric_precision <= 24 else 8
            ## else:
            ##     return DDColConf.get_col_type_conf(self.type).size

    def _read_int(self, stream, size, signed=None):
        byte_data = stream.read(size)
        if signed is None:
            should_signed = self.name not in column_spec_size and not self.is_unsigned
        else:
            should_signed = signed

        if should_signed:
            byte_data = (byte_data[0] ^ 0x80).to_bytes(1, "big") + byte_data[1:]
        return int.from_bytes(byte_data, "big", signed=should_signed)

    @property
    @cache
    def element_map(self):
        data = {}
        for e in self.elements:
            data[e.index] = e.name
        return data

    @property
    @cache
    def new_decimal_size(self) -> NewDecimalSize:
        intg = self.numeric_precision - self.numeric_scale
        frac = self.numeric_scale
        intg0, intg0x = int(intg / 9), decimal_leftover_part[intg % 9]
        frac0, frac0x = (
            int(self.numeric_scale / 9),
            decimal_leftover_part[self.numeric_scale % 9],
        )
        total = intg0 * 4 + intg0x + frac0 * 4 + frac0x
        return NewDecimalSize(intg, frac, intg0, intg0x, frac0, frac0x, total)

    def _read_new_decimal(
        self, stream
    ):  # from mysys/decimal.cc:bin2decimal && decimal2string
        byte_data = stream.read(self.new_decimal_size.total)
        mask = 0 if byte_data[0] & 0x80 else -1
        negative = mask != 0
        byte_data = (byte_data[0] ^ 0x80).to_bytes(1, "big") + byte_data[1:]
        byte_stream = io.BytesIO(byte_data)

        integer = "" if not negative else "-"
        if self.new_decimal_size.intg0x > 0:
            d = byte_stream.read(self.new_decimal_size.intg0x)
            integer += str(int.from_bytes(d, "big", signed=True) ^ mask)
        for i in range(self.new_decimal_size.intg0):
            d = byte_stream.read(4)
            integer += str(int.from_bytes(d, "big", signed=True) ^ mask)

        if self.new_decimal_size.frac > 0:
            integer += "."

        for i in range(self.new_decimal_size.frac0):
            d = byte_stream.read(4)
            integer += str(int.from_bytes(d, "big", signed=True) ^ mask)

        if self.new_decimal_size.frac0x > 0:
            d = byte_stream.read(self.new_decimal_size.frac0x)
            integer += str(int.from_bytes(d, "big", signed=True) ^ mask)

        return decimal.Decimal(integer)

    def _read_decimal(self, stream):  # deprecate for old decimal
        integer_part = self.numeric_precision - self.numeric_scale
        fractional_part = self.numeric_scale
        integer_part_size = int(
            integer_part / 9
        )  # + decimal_leftover_part[integer_part % 9]
        fractional_part_size = int(
            fractional_part / 9
        )  # + decimal_leftover_part[fractional_part_size%9]

        total_size = (
            integer_part_size * 4
            + fractional_part_size * 4
            + decimal_leftover_part[integer_part % 9]
            + decimal_leftover_part[fractional_part % 9]
        )
        byte_data = stream.read(total_size)
        positive = byte_data[0] & 0x80 > 0
        if not positive:
            byte_data = bytes(~b & 0xFF for b in byte_data)
        byte_data = (byte_data[0] ^ 0x80).to_bytes(1, "big") + byte_data[1:]

        integer = ""
        consume = decimal_leftover_part[integer_part % 9]
        if consume > 0:
            integer = str(int.from_bytes(byte_data[:consume], "big"))

        for i in range(integer_part_size):
            # integer *= 1000
            integer += str(int.from_bytes(byte_data[consume : consume + 4], "big"))
            consume += 4

        fractional = ""
        for i in range(fractional_part_size):
            fractional += str(int.from_bytes(byte_data[consume : consume + 4], "big"))
            consume += 4

        fractional_consume = decimal_leftover_part[fractional_part % 9]
        if fractional_consume > 0:
            fractional += str(int.from_bytes(byte_data[consume:], "big"))

        if len(fractional) > 0:
            return f"{integer}.{fractional}"
        else:
            return f"{integer}"

    def _read_varchar(self, stream, size):
        if (
            size > 8096
        ):  # with test on 8.0.17 when data size larger than 8096, the col will be store off-page
            # will the varsize record in record header is 16404
            data = stream.read(20)
            cur = stream.tell()
            pointer = OffPagePointer.parse_stream(io.BytesIO(data))
            real_data = pointer.read_data(stream)
            stream.seek(cur)
            return real_data
        else:
            data = stream.read(size)
            return data

    def read_data(self, stream, size=None, quick=True):
        if self.name == "DB_ROLL_PTR":
            return MRollbackPointer.parse_stream(stream)
        dtype = DDColumnType(self.type)
        if size is not None:
            dsize = size
        else:
            dsize = self.size
        if dtype.is_int_number():
            return self._read_int(stream, dsize)
        elif dtype == DDColumnType.VECTOR:
            if quick:
                return stream.read(dsize)
            else:
                vec = []
                for i in range(int(dsize / 4)):
                    byte_data = stream.read(4)
                    vec.append(struct.unpack("f", byte_data)[0])
                return vec
        elif dtype == DDColumnType.FLOAT:
            byte_data = stream.read(dsize)
            if dsize == 4:
                return struct.unpack("f", byte_data)[0]
            if dsize == 8:
                return struct.unpack("d", byte_data)[0]
        elif dtype == DDColumnType.DOUBLE:
            byte_data = stream.read(dsize)
            return struct.unpack("d", byte_data)[0]
        ## https://dev.mysql.com/doc/refman/8.0/en/precision-math-decimal-characteristics.html
        elif dtype == DDColumnType.DECIMAL or dtype == DDColumnType.NEWDECIMAL:
            return self._read_new_decimal(stream)
        elif dtype == DDColumnType.STRING:
            return self._read_varchar(stream, dsize).decode(errors="replace").strip()
        elif dtype == DDColumnType.VARCHAR:
            return self._read_varchar(stream, dsize).decode(errors="replace")
        elif dtype in [
            DDColumnType.LONG_BLOB,
            DDColumnType.MEDIUM_BLOB,
            DDColumnType.BLOB,
        ]:
            return self._read_varchar(stream, dsize).decode(errors="replace")
        elif dtype == DDColumnType.TINY_BLOB:
            return self._read_varchar(stream, dsize).decode(errors="replace")
        elif dtype == DDColumnType.TIME2:
            time_data = MTime2.parse_stream(stream)
            time_data.parse_fsp(stream, dsize - 3)  # 3 = MTime2.sizeof()
            return time_data.to_timedelta()
        elif dtype == DDColumnType.DATETIME2:
            datetime_data = MDatetime.parse_stream(stream)
            datetime_data.parse_fsp(stream, dsize - 5)  # 5 is MDatetime.sizeof()
            try:
                return datetime_data.to_datetime()
            except:
                return datetime_data
        elif dtype == DDColumnType.NEWDATE:
            return MDate.parse_stream(stream).to_date()
        elif dtype == DDColumnType.TIMESTAMP2:
            ts_data = MTimestamp.parse_stream(stream)
            ts_data.parse_fsp(stream, dsize - 4)
            return ts_data.to_time()
        elif dtype == DDColumnType.YEAR:
            return int.from_bytes(stream.read(dsize), "big") + 1900
        elif dtype == DDColumnType.BIT:
            return self._read_int(stream, dsize, False)
        elif dtype == DDColumnType.ENUM:
            idx = self._read_int(stream, dsize, False)
            if idx in self.element_map:
                return b64decode(self.element_map[idx])
            else:
                return idx
        elif dtype == DDColumnType.SET:
            mask = self._read_int(stream, dsize, False)
            r = []
            for m, v in self.element_map.items():
                if mask & (1 << (m - 1)):
                    r.append(b64decode(v).decode(errors="replace"))
            return ",".join(r)
        elif dtype == DDColumnType.JSON:
            # data = stream.read(dsize)
            data = self._read_varchar(stream, dsize)
            try:
                if len(data) == 0:
                    return "null"
                v = MJson.parse_stream(io.BufferedReader(io.BytesIO(data)))
                return v.get_json()
            except Exception:
                return data
        elif dtype == DDColumnType.GEOMETRY:
            if quick:
                return stream.read(dsize)
            else:
                data = MGeo.parse_stream(stream)
                logging.debug("geometry data is %s, size is %d", data, dsize)
                return data

@modify_init
@dataclass(eq=False)
class Index:
    name: str = ""
    hidden: bool = False
    is_generated: bool = False
    ordinal_position: int = 0
    comment: str = ""
    options: str = ""
    se_private_data: str = ""
    type: int = 0  ## sql/dd/types/index.h:enum_index_type
    algorithm: int = 0
    is_algorithm_explicit: bool = False
    is_visible: bool = False
    engine: str = ""
    engine_attribute: str = ""
    secondary_engine_attribute: str = ""
    elements: typing.List[IndexElement] = dataclasses.field(default_factory=list)

    @property
    @cache
    def private_data(self):
        data = const.line_to_dict(self.se_private_data, ";", "=")
        return data

    def __post_init__(self):
        c: typing.List[IndexElement] = [IndexElement(**e) for e in self.elements]
        self.elements = c

    def get_effect_element(self) -> typing.List[IndexElement]:
        return [e for e in self.elements if e.length != 4294967295 and not e.hidden]

    def get_index_type(self):
        it = const.index_type.IndexType(self.type)
        if it == const.index_type.IndexType.IT_PRIMARY:
            return "PRIMARY "
        elif it == const.index_type.IndexType.IT_UNIQUE:
            return "UNIQUE "
        elif it == const.index_type.IndexType.IT_FULLTEXT:
            return "FULLTEXT "
        elif it == const.index_type.IndexType.IT_MULTIPLE:
            return ""
        elif it == const.index_type.IndexType.IT_SPATIAL:
            return "SPATIAL "
