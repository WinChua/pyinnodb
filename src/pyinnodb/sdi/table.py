import io
import typing
import struct
import decimal
import dataclasses
import re

import sys
if sys.version_info.minor >= 9:
    from functools import cache
else:
    cache = lambda x: x
from dataclasses import dataclass
from collections import namedtuple
from base64 import b64decode

from .. import const
from ..const.dd_column_type import DDColumnType, DDColConf
from ..disk_struct.varsize import VarSize, OffPagePointer

from ..disk_struct.data import MTime2, MDatetime, MDate, MTimestamp
from ..disk_struct.json import MJson
from ..disk_struct.rollback import MRollbackPointer
from ..disk_struct.record import MRecordHeader

from ..disk_struct.index import MIndexPage

import logging

logger = logging.getLogger(__name__)

column_type_size = re.compile("[^(]*[(]([^)]*)[)]")

class Lob:
    def __init__(self, data, off_page):
        self.data = data
        self.off_page = off_page

    def __str__(self):
        return f"<Lob length:{len(self.data)} preview:{self.data[:5] + b'..' + self.data[-5:]} off_page:{self.off_page}>"


NewDecimalSize = namedtuple(
    "NewDecimalSize", "intg frac intg0 intg0x frac0 frac0x total"
)


def modify_init(cls):
    old_init = cls.__init__
    field_names = [f.name for f in dataclasses.fields(cls)]

    def __init__(self, **kwargs):
        for_old_init = {}
        for_new_init = {}
        for k, v in kwargs.items():
            if k in field_names:
                for_old_init[k] = v
            else:
                for_new_init[k] = v
        old_init(self, **for_old_init)
        for k, v in for_new_init.items():
            setattr(self, k, v)

    cls.__init__ = __init__
    return cls


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

    def index_prefix(self, ie: IndexElement):
        if ie.length == 4294967295:
            return 0, False
        varlen, prekey_len = 1, 0
        if const.dd_column_type.DDColumnType.is_var(
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
    def version_valid(self, data_schema_version) -> bool :
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
        return "version_added" in self.private_data or "version_dropped" in self.private_data
        #return "default_null" in self.private_data or "default" in self.private_data

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

    def __post_init__(self):
        ce: typing.List[ColumnElement] = [ColumnElement(**e) for e in self.elements]
        self.elements = ce

    def get_collation(self):
        coll = const.get_collation_by_id(self.collation_id)
        return coll

    def gen_sql(self):
        sql = f"`{self.name}` {self.column_type_utf8}{'' if self.is_nullable or self.is_virtual else ' NOT NULL'}"
        sql += f"{' AUTO_INCREMENT' if self.is_auto_increment else ''}"
        if self.default_option != "":
            sql += f" DEFAULT ({self.default_option})"
        elif not self.default_value_utf8_null:
            sql += f" DEFAULT '{self.default_value_utf8}'"
        elif len(self.generation_expression_utf8) != 0:
            sql += f" GENERATED ALWAYS AS ({self.generation_expression_utf8}) {'VIRTUAL' if self.is_virtual else 'STORED'}"
        elif self.default_value_utf8_null and self.is_nullable:
            sql += f" DEFAULT NULL"
        if self.update_option != "":
            sql += f" ON UPDATE {self.update_option}"
        sql += " COMMENT '" + self.comment + "'" if self.comment else ""
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

        elif dtype == DDColumnType.STRING: # if column don't have varsize 
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
            if len(data) > 200:
                return Lob(data, False)
            else:
                return data

    def read_data(self, stream, size=None):
        if self.name == "DB_ROLL_PTR":
            return MRollbackPointer.parse_stream(stream)
        dtype = DDColumnType(self.type)
        if size is not None:
            dsize = size
        else:
            dsize = self.size
        if dtype.is_int_number():
            return self._read_int(stream, dsize)
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
            return self._read_varchar(stream, dsize).decode(errors='replace').strip()
        elif dtype == DDColumnType.VARCHAR:
            return self._read_varchar(stream, dsize).decode(errors='replace')
        elif dtype in [DDColumnType.LONG_BLOB, DDColumnType.MEDIUM_BLOB, DDColumnType.BLOB]:
            return self._read_varchar(stream, dsize).decode(errors='replace')
        elif dtype == DDColumnType.TINY_BLOB:
            return self._read_varchar(stream, dsize).decode(errors='replace')
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
                if mask & (1 << (m-1)):
                    r.append(b64decode(v).decode(errors='replace'))
            return ','.join(r)
        elif dtype == DDColumnType.JSON:
            # data = stream.read(dsize)
            data = self._read_varchar(stream, dsize)
            if isinstance(data, Lob):
                data = data.data
            try:
                if len(data) == 0:
                    return 'null'
                v = MJson.parse_stream(io.BufferedReader(io.BytesIO(data)))
                return v.get_json()
            except Exception as e:
                return data


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


@modify_init
@dataclass(eq=False)
class CheckCons:
    name: str = ""
    state: int = None
    check_clause: str = ""  # write binary
    check_clause_utf8: str = ""


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


@modify_init
@dataclass(eq=False)
class ForeignElement:
    # column_opx: Column = None
    ordinal_position: str = ""
    referenced_column_name: str = ""
    pass


@modify_init
@dataclass(eq=False)
class ForeignKeys:
    name: str = ""
    match_option: int = None
    update_rule: int = None
    delete_rule: int = None
    unique_constraint_name: str = ""
    referenced_table_catalog_name: str = ""
    referenced_table_schema_name: str = ""
    referenced_table_name: str = ""
    elements: typing.List[ForeignElement] = None


@modify_init
@dataclass(eq=False)
class PartitionValue:
    max_value: bool = False
    null_value: bool = False
    list_num: int = 0
    column_num: int = 0
    value_utf8: str = ""


@modify_init
@dataclass(eq=False)
class PartitionIndex:
    options: str = ""  # properties
    se_private_data: str = ""  # properties
    index_opx: int = 0


@modify_init
@dataclass(eq=False)
class Partition:
    name: str = ""
    parent_partition_id: int = 0
    number: int = 0
    se_private_id: int = 0
    description_utf8: str = ""
    engine: str = ""
    comment: str = ""
    options: str = ""
    se_private_data: str = ""
    values: typing.List[PartitionValue] = None
    indexes: typing.List[PartitionIndex] = None
    subpartitions: typing.List["Partition"] = None

    def __post_init__(self):
        vs: typing.List[PartitionValue] = [PartitionValue(**v) for v in self.values]
        self.values = vs
        idx: typing.List[PartitionIndex] = [PartitionIndex(**v) for v in self.indexes]
        self.indexes = idx
        sp: typing.List["Partition"] = [Partition(**v) for v in self.subpartitions]
        self.subpartitions = sp


@modify_init
@dataclass(eq=False)
class Table:
    # from abstract table
    name: str = ""
    mysql_version_id: int = 0
    created: int = 0
    last_altered: int = 0
    hidden: int = 0
    options: str = 0
    columns: typing.List[Column] = dataclasses.field(default_factory=list)
    schema_ref: str = ""
    # from Table
    se_private_id: int = 0
    engine: str = ""
    last_checked_for_upgrade_version_id: int = 0
    comment: str = ""
    se_private_data: str = ""  # properties
    engine_attribute: str = ""
    secondary_engine_attribute: str = ""
    row_format: int = 0
    partition_type: int = 0
    partition_expression: str = ""
    partition_expression_utf8: str = ""
    default_partitioning: int = 0
    subpartition_type: int = 0
    subpartition_expression: str = ""
    subpartition_expression_utf8: str = ""
    default_subpartitioning: int = 0
    indexes: typing.List[Index] = dataclasses.field(default_factory=list)
    foreign_keys: typing.List[ForeignKeys] = dataclasses.field(default_factory=list)
    check_constraints: typing.List[CheckCons] = dataclasses.field(default_factory=list)
    partitions: typing.List[Partition] = dataclasses.field(default_factory=list)
    collation_id: int = 0
    # tablespace_ref: ?


    @property
    @cache
    def private_data(self):
        data = const.line_to_dict(self.se_private_data, ";", "=")
        return data

    @property
    @cache
    def DataClassHiddenCol(self):
        cols = []
        for c in self.columns:
            # if c.name in ["DB_ROW_ID", "DB_TRX_ID", "DB_ROLL_PTR"]:
            #     continue
            if c.private_data.get("version_dropped", None) is not None:
                continue
            cols.append(c.name)

        return namedtuple(self.name, " ".join(cols))

    @property
    @cache
    def DataClass(self):
        cols = []
        for c in self.columns:
            if c.name in ["DB_ROW_ID", "DB_TRX_ID", "DB_ROLL_PTR"]:
                continue
            if c.is_hidden_from_user:
                continue
            if c.private_data.get("version_dropped", None) is not None:
                continue
            if c.is_virtual or c.generation_expression_utf8 != "":
                continue
            cols.append(c.name)

        return namedtuple(self.name, " ".join(cols))

    @property
    @cache
    def null_col_count(self):
        return sum(1 if c.is_nullable else 0 for c in self.columns)

    @property
    @cache
    def var_col(self):
        return [
            c
            for c in self.columns
            if DDColumnType.is_big(c.type) or DDColumnType.is_var(c.type)
        ]

    @property
    @cache
    def nullcol_bitmask_size(self):
        null_col = [c for c in self.columns if c.is_nullable]
        # null_col = [c for c in null_col if "default_null" not in c.private_data and "default" not in c.private_data]
        return int((len(null_col) + 7) / 8), null_col

    @property
    @cache
    def instant_col(self):
        instant_col = [c for c in self.columns if c.is_instant_col]
        return instant_col

    def get_column(self, cond: typing.Callable[[Column], bool]) -> typing.List[Column]:
        return [c for c in self.columns if cond(c)]

    @cache
    def get_column_schema_version(self, version) -> typing.List[Column]:
        cols = []
        for col in self.columns:
            va = int(col.private_data.get("version_added", 0))
            vd = int(col.private_data.get("version_dropped", 0))
            if version < va:  # data was inserted before this col add to table
                continue
            if (
                vd != 0 and version >= vd
            ):  # data was inserted after this col add to table
                continue
            cols.append(col)
        return cols

    @cache
    def get_disk_data_layout(self):
        phsical_post_exists = False
        for c in self.columns:
            if "phsical_pos" in c.private_data:
                phsical_post_exists = True
                break
        logger.debug("has physical %s", phsical_post_exists)
        if phsical_post_exists:
            c_l = {}
            for idx in self.indexes:
                if idx.name != "PRIMARY" and len(self.indexes) != 1:
                    continue
                for ie in idx.elements:
                    col = self.columns[ie.column_opx]
                    prekey_len, ok = col.index_prefix(ie)
                    if ok:
                        c_l[ie.column_opx] = prekey_len
                    else:
                        c_l[ie.column_opx] = ie.length
            data_layout_col = []
            for i, c in enumerate(self.columns):
                data_layout_col.append((c, c_l.get(i, 4294967295)))
            data_layout_col.sort(key = lambda c: c[0].private_data.get("physical_pos", 0))
            return data_layout_col

        data_layout_col = []
        for idx in self.indexes:
            if (
                idx.name == "PRIMARY" or len(self.indexes) == 1
                # const.index_type.IndexType(idx.type)
                # == const.index_type.IndexType.IT_PRIMARY
            ):
                for ie in idx.elements:
                    col = self.columns[ie.column_opx]
                    prekey_len, ok = col.index_prefix(ie)
                    if ok: # prefix
                        data_layout_col.append((col, prekey_len))
                    else:
                        data_layout_col.append((col, ie.length))

        if len(data_layout_col) == 0:
            for idx in self.indexes:
                for ie in idx.elements:
                    col = self.columns[ie.column_opx]
                    prekey_len, ok = col.index_prefix(ie)
                    if ok: # prefix
                        data_layout_col.append((col, prekey_len))
                    else:
                        data_layout_col.append((col, ie.length))
                break

        return data_layout_col

    def build_primary_key_bytes(self, values) -> bytes:
        cols = self.get_disk_data_layout()
        cols = [c for c in cols if c[1] != ~0&0xffffffff]
        buf = io.BytesIO()
        for i, c in enumerate(cols):
            if DDColumnType(c[0].type).is_int_number():
                if c[0].is_unsigned:
                    buf.write(const.encode_mysql_unsigned(values[i], c[0].size))
                else:
                    buf.write(const.encode_mysql_int(values[i], c[0].size))
            else:
                raise "not int primary"
        return buf.getvalue()

    def get_primary_key_col(self) -> typing.List[Column]:
        primary_col = []
        for idx in self.indexes:
            if (
                const.index_type.IndexType(idx.type)
                == const.index_type.IndexType.IT_PRIMARY
            ):
                for ie in idx.get_effect_element():
                    primary_col.append(self.columns[ie.column_opx])

                return primary_col

        return self.get_column(
            lambda col: col.name == "DB_ROW_ID"
        )  # for table with no primary

    def get_default_DB_col(self) -> typing.List[Column]:
        return [c for c in self.columns if c.name in ["DB_TRX_ID", "DB_ROLL_PTR"]]

    def search(self, f, primary_key, hidden_col):
        root_page_no = int(self.indexes[0].private_data.get("root", 4))
        f.seek(root_page_no * const.PAGE_SIZE)
        root_index_page = MIndexPage.parse_stream(f)
        first_leaf_page = root_index_page.get_first_leaf_page(f, self.get_primary_key_col())
        if isinstance(primary_key, tuple):
            primary_key = (self.build_primary_key_bytes(primary_key))
        else:
            primary_key = (self.build_primary_key_bytes((primary_key,)))
        value_parser = MIndexPage.default_value_parser(self, hidden_col=hidden_col, transfter=lambda id: id)

        while first_leaf_page != 4294967295:
            f.seek(first_leaf_page * const.PAGE_SIZE)
            index_page = MIndexPage.parse_stream(f)
            f.seek(first_leaf_page * const.PAGE_SIZE)
            page_dir_idx, match = index_page.binary_search_with_page_directory(primary_key, f)
            f.seek(first_leaf_page * const.PAGE_SIZE + index_page.page_directory[page_dir_idx] - 5)
            end_rh = MRecordHeader.parse_stream(f)
            if match and const.RecordType(end_rh.record_type) == const.RecordType.Conventional: # the key
                return value_parser(end_rh, f)
            elif match and const.RecordType(end_rh.record_type) == const.RecordType.NodePointer:
                record_key = f.read(len(primary_key))
                page_num = f.read(4)
                first_leaf_page = int.from_bytes(page_num, "big")
            else:
                f.seek(first_leaf_page * const.PAGE_SIZE + index_page.page_directory[page_dir_idx+1] - 5)
                start_rh = MRecordHeader.parse_stream(f)
                owned = end_rh.num_record_owned
                first_leaf_page = 4294967295 # no match if cur page is leaf then break loop
                for i in range(owned+1):
                    cur = f.tell()
                    if const.RecordType(start_rh.record_type) == const.RecordType.Conventional:
                        record_primary_key = f.read(len(primary_key))
                        if record_primary_key == primary_key:
                            f.seek(-len(primary_key), 1)
                            v = value_parser(start_rh, f)
                            return v
                    elif const.RecordType(start_rh.record_type) == const.RecordType.NodePointer:
                        record_key = f.read(len(primary_key))
                        if record_key > primary_key:
                            if i == 1:
                                page_num = f.read(4)
                            first_leaf_page = int.from_bytes(page_num, "big")
                            break
                        elif record_key == primary_key:
                            page_num = f.read(4)
                            first_leaf_page = int.from_bytes(page_num, "big")
                            break
                        page_num = f.read(4)
                    f.seek(cur)
                    f.seek(start_rh.next_record_offset - 5, 1)
                    start_rh = MRecordHeader.parse_stream(f)


    def iter_record(self, f, hidden_col=False, garbage=False, transfter=None):
        root_page_no = int(self.indexes[0].private_data.get("root", 4))
        f.seek(root_page_no * const.PAGE_SIZE)
        root_index_page = MIndexPage.parse_stream(f)
        first_leaf_page = root_index_page.get_first_leaf_page(f, self.get_primary_key_col())
        if first_leaf_page is None:
            return

        default_value_parser = MIndexPage.default_value_parser(self, hidden_col=hidden_col, transfter=transfter)

        result = []
        while first_leaf_page != 4294967295:
            f.seek(first_leaf_page * const.PAGE_SIZE)
            index_page = MIndexPage.parse_stream(f)
            result.extend(index_page.iterate_record_header(f, value_parser = default_value_parser, garbage=garbage))
            first_leaf_page = index_page.fil.next_page


        return result


    def __post_init__(self):
        cols: typing.List[Column] = [Column(**c) for c in self.columns]
        self.columns = cols
        idxs: typing.List[Index] = [Index(**i) for i in self.indexes]
        self.indexes = idxs
        fors: typing.List[ForeignKeys] = [ForeignKeys(**f) for f in self.foreign_keys]
        self.foreign_keys = fors
        cons: typing.List[CheckCons] = [CheckCons(**c) for c in self.check_constraints]
        self.check_constraints = cons
        pars: typing.List[Partition] = [Partition(**p) for p in self.partitions]
        self.partitions = pars

    def tosql_gen_column(self):
        hidden_column_name = ["DB_TRX_ID", "DB_ROLL_PTR", "DB_ROW_ID"]
        for c in self.columns:
            if c.name in hidden_column_name:
                continue
            DDColumnType(c.type) != DDColumnType.LONG
        pass

    def gen_sql_for_index(self, idx: Index) -> str:
        cols_name = []
        for ie in idx.get_effect_element():
            col = self.columns[ie.column_opx]
            varlen, prekey_len = 1, 0
            if const.dd_column_type.DDColumnType.is_var(
                col.type
            ):  ## TODO: judge prefix key
                if col.collation_id == 255:
                    varlen = 4
                elif DDColumnType(col.type) in [
                    DDColumnType.VARCHAR,
                    DDColumnType.STRING,
                ] and not col.column_type_utf8.startswith("varb"):
                    varlen = 3
                if (
                    col.char_length > ie.length
                ):  # the index field data length must small than the original field
                    prekey_len = int(ie.length / varlen)
            prefix_part = f"({prekey_len})" if prekey_len != 0 else ""
            cols_name.append(f"`{col.name}`{prefix_part}")
        if len(cols_name) == 0:
            return ""

        idx_type_part = f"{idx.get_index_type()}KEY "
        idx_name_part = f"`{idx.name}` " if idx.name != "PRIMARY" else ""
        key_part = ",".join(cols_name)
        comment = f" COMMENT '{idx.comment}'" if idx.comment else ""
        return f"{idx_type_part}{idx_name_part}({key_part}){comment}"

    def gen_sql_for_partition(self) -> str:
        pt = const.partition.PartitionType(self.partition_type)
        if pt == const.partition.PartitionType.PT_RANGE:
            p = f"/*!50100 PARTITION BY RANGE({self.partition_expression_utf8}) (\n    "
            parts = []
            for par in self.partitions:
                parts.append(
                    f"PARTITION {par.name} VALUES LESS THAN ({par.description_utf8})"
                )
            parts = ",\n    ".join(parts) + "\n"
            return f"{p}{parts})*/"
        elif pt == const.partition.PartitionType.PT_HASH:
            return f"/*!50100 PARTITION BY HASH ({self.partition_expression_utf8}) PARTITIONS ({len(self.partitions)})*/"
        elif pt == const.partition.PartitionType.PT_KEY_55:
            return f"/*!50100 PARTITION BY KEY ({self.partition_expression_utf8}) PARTITIONS ({len(self.partitions)})*/"
        elif pt == const.partition.PartitionType.PT_LIST:
            p = f"/*!50100 PARTITION BY LIST ({self.partition_expression_utf8}) (\n    "
            parts = []
            for par in self.partitions:
                parts.append(f"PARTITION {par.name} VALUES IN ({par.description_utf8})")
            parts = ",\n    ".join(parts) + "\n"
            return f"{p}{parts})*/"


def should_ext():
    return True


"""
static inline bool page_zip_rec_needs_ext(ulint rec_size, ulint comp,
                                          ulint n_fields,
                                          const page_size_t &page_size) {
  ut_ad(rec_size > (comp ? REC_N_NEW_EXTRA_BYTES : REC_N_OLD_EXTRA_BYTES));
  ut_ad(comp || !page_size.is_compressed());
  if (rec_size >= REC_MAX_DATA_SIZE) {  # REC_MAX_DATA_SIZE 16384
    return true;
  }
  if (page_size.is_compressed()) {
    ut_ad(comp);
    /* On a compressed page, there is a two-byte entry in
    the dense page directory for every record.  But there
    is no record header.  There should be enough room for
    one record on an empty leaf page.  Subtract 1 byte for
    the encoded heap number.  Check also the available space
    on the uncompressed page. */
    return (rec_size - (REC_N_NEW_EXTRA_BYTES - 2 - 1) >=
                page_zip_empty_size(n_fields, page_size.physical()) ||
            rec_size >= page_get_free_space_of_empty(true) / 2);
  }
  return (rec_size >= page_get_free_space_of_empty(comp) / 2);
}    
"""
"""
static inline ulint page_get_free_space_of_empty(
    bool comp) /*!< in: nonzero=compact page layout */
{
  if (comp) {
    return ((ulint)(UNIV_PAGE_SIZE - PAGE_NEW_SUPREMUM_END - PAGE_DIR -
                    2 * PAGE_DIR_SLOT_SIZE));
  }
  return ((ulint)(UNIV_PAGE_SIZE - PAGE_OLD_SUPREMUM_END - PAGE_DIR -
                  2 * PAGE_DIR_SLOT_SIZE));
}  
"""
"""
UNIV_PAGE_SIZE: 1 << 14 => 16 KB
PAGE_OLD_SUPREMUM_END = PAGE_DATA + 2 + 2 * REC_N_OLD_EXTRA_BYTES + 8 + 9
PAGE_DIR = 8
PAGE_DIR_SLOT_SIZE = 2
REC_N_OLD_EXTRA_BYTES = 6
PAGE_DATA = 38 + 36 + 2 * 10

16 * 1024 - 38 - 36 - 20 - 2 - 2 * 6 - 8 - 9  - 8 - 2 * 2
"""

table_opts = [
    "avg_row_length",
    "checksum",
    "compress",
    "connection_string",
    "delay_key_write",
    "encrypt_type",
    "explicit_tablespace",
    "key_block_size",
    "keys_disabled",
    "max_rows",
    "min_rows",
    "pack_keys",
    "pack_record",
    "plugin_version",
    "row_type",
    "secondary_engine",
    "secondary_load",
    "server_i_s_table",
    "server_p_s_table",
    "stats_auto_recalc",
    "stats_persistent",
    "stats_sample_pages",
    "storage",
    "tablespace",
    "timestamp",
    "view_valid",
    "gipk",
]

column_spec_size = {"DB_ROW_ID": 6, "DB_TRX_ID": 6, "DB_ROLL_PTR": 7}

def get_sys_col(name, pos):
    if name == "DB_TRX_ID":
        return Column(
            name = "DB_TRX_ID",
            type = 10,
            hidden = 2,
            ordinal_position = pos,
            char_length = 6,
            has_no_default = False,
            default_value = "",
            default_value_utf8_null = True,
            collation_id = 63,
            is_explicit_collation = False)
    elif name == "DB_ROLL_PTR":
        return Column(
            name = name,
            type = 9,
            hidden = 2,
            ordinal_position = pos,
            char_length = 6,
            has_no_default = False,
            default_value = "",
            default_value_utf8_null = True,
            collation_id = 63,
            is_explicit_collation = False)
        pass
    elif name == "DB_ROW_ID":
        pass
    return

if __name__ == "__main__":
    import json

    with open("t1.json", "rb") as f:
        data = json.loads(f.read())
        print(data)
        table_sdi = Table(**data["dd_object"])
