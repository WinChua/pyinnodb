import io
import typing
import struct
from dataclasses import dataclass
import dataclasses
from functools import cache
from collections import namedtuple
from .. import const
from ..const.dd_column_type import DDColumnType, DDColConf
import decimal

NewDecimalSize = namedtuple("NewDecimalSize", "intg frac intg0 intg0x frac0 frac0x total")

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
class ColumnElement:
    name: str = "" ## BINARY VARBINARY
    index: str = ""

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
    default_value: str = "" # binary
    default_value_utf8_null: bool = False
    default_value_utf8: str  = ""
    default_option: str = ""
    update_option: str = ""
    comment: str = ""
    generation_expression: str = ""
    generation_expression_utf8: str = ""
    options: str = "" # properties
    se_private_data: str = "" # properties
    engine_attribute: str = ""
    secondary_engine_attribute: str = ""
    column_key: int = 0
    column_type_utf8: str = ""
    elements: typing.List[ColumnElement] = dataclasses.field(default_factory=list)
    collation_id: int = 0
    is_explicit_collation: bool = False

    def __post_init__(self):
        ce: typing.List[ColumnElement] = [ColumnElement(**e) for e in self.elements]
        self.elements = ce

    def get_collation(self):
        coll = const.get_collation_by_id(self.collation_id)
        return coll

    def gen_sql(self):
        sql = f"`{self.name}` {self.column_type_utf8} {'NULL' if self.is_nullable else 'NOT NULL'}"
        sql += f"{' AUTO_INCREMENT' if self.is_auto_increment else ''}"
        if self.default_option != "":
            sql += f" DEFAULT ({self.default_option})"
        elif not self.default_value_utf8_null:
            sql += f" DEFAULT '{self.default_value_utf8}'"
        if self.update_option != "":
            sql += f" ON UPDATE {self.update_option}"
        sql += ' COMMENT \'' + self.comment + '\'' if self.comment else ''
        return sql

    def size(self):
        if self.name in column_spec_size:
            return column_spec_size[self.name]
        else:
            dtype = DDColumnType(self.type)
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
            byte_data = (byte_data[0] ^ 0x80).to_bytes(1) + byte_data[1:]
        return int.from_bytes(byte_data, "big", signed=should_signed)

    @property
    @cache
    def new_decimal_size(self) -> NewDecimalSize :
        intg = self.numeric_precision - self.numeric_scale
        frac = self.numeric_scale
        intg0, intg0x = int(intg / 9), decimal_leftover_part[intg % 9]
        frac0, frac0x = int(self.numeric_scale/9), decimal_leftover_part[self.numeric_scale % 9]
        total = intg0*4 + intg0x + frac0*4 + frac0x
        return NewDecimalSize(intg, frac, intg0, intg0x, frac0, frac0x, total)

    def _read_new_decimal(self, stream):
        byte_data = stream.read(self.new_decimal_size.total)
        mask = 0 if byte_data[0] & 0x80 else -1
        negative = mask != 0
        byte_data = (byte_data[0] ^ 0x80).to_bytes(1) + byte_data[1:]
        byte_stream = io.BytesIO(byte_data)

        integer = '' if not negative else '-'
        if self.new_decimal_size.intg0x > 0:
            d = byte_stream.read(self.new_decimal_size.intg0x)
            integer += str(int.from_bytes(d, signed=True) ^ mask)
        for i in range(self.new_decimal_size.intg0):
            d = byte_stream.read(4)
            integer += str(int.from_bytes(d, signed=True) ^ mask)

        if self.new_decimal_size.frac > 0:
            integer += "."
        
        for i in range(self.new_decimal_size.frac0):
            d = byte_stream.read(4)
            integer += str(int.from_bytes(d, signed=True) ^ mask)

        if self.new_decimal_size.frac0x > 0:
            d = byte_stream.read(self.new_decimal_size.frac0x)
            integer += str(int.from_bytes(d, signed=True) ^ mask)

        return decimal.Decimal(integer)


    def _read_decimal(self, stream):
        integer_part = self.numeric_precision - self.numeric_scale
        fractional_part = self.numeric_scale
        integer_part_size = int(integer_part / 9) # + decimal_leftover_part[integer_part % 9]
        fractional_part_size = int(fractional_part/9) # + decimal_leftover_part[fractional_part_size%9]

        total_size = integer_part_size * 4 + fractional_part_size * 4 + decimal_leftover_part[integer_part % 9] + decimal_leftover_part[fractional_part % 9]
        byte_data = stream.read(total_size)
        positive = byte_data[0] & 0x80 > 0
        if not positive:
            byte_data = bytes(~b & 0xff for b in byte_data)
        byte_data = (byte_data[0] ^ 0x80).to_bytes(1) + byte_data[1:]

        integer = ''
        consume = decimal_leftover_part[integer_part%9]
        if consume > 0:
            integer = str(int.from_bytes(byte_data[:consume], "big"))
    
        for i in range(integer_part_size):
            # integer *= 1000
            integer += str(int.from_bytes(byte_data[consume:consume+4], "big"))
            consume += 4

        fractional = ''
        for i in range(fractional_part_size):
            fractional += str(int.from_bytes(byte_data[consume:consume+4], "big"))
            consume += 4

        fractional_consume = decimal_leftover_part[fractional_part % 9]
        if fractional_consume > 0:
            fractional += str(int.from_bytes(byte_data[consume:], "big"))

        if len(fractional) > 0:
            return f"{integer}.{fractional}"
        else:
            return f"{integer}"


    def read_data(self, stream):
        dtype = DDColumnType(self.type)
        dsize = self.size()
        if dtype.is_int_number():
            return self._read_int(stream, dsize)
        elif dtype == DDColumnType.FLOAT:
            byte_data = stream.read(dsize)
            if dsize == 4:
                return struct.unpack('f', byte_data)[0]
            if dsize == 8:
                return struct.unpack('d', byte_data)[0]
        elif dtype == DDColumnType.DOUBLE:
            byte_data = stream.read(dsize)
            return struct.unpack('d', byte_data)[0]
        ## https://dev.mysql.com/doc/refman/8.0/en/precision-math-decimal-characteristics.html
        elif dtype == DDColumnType.DECIMAL or dtype == DDColumnType.NEWDECIMAL:
            return self._read_new_decimal(stream)
        # if dtype == DDColumnType.JSON:
        #     size = const.parse_var_size(stream)
        # if dtype.is_var():
        #     size = const.parse_var_size(stream)
        pass

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
    check_clause: str = "" # write binary
    check_clause_utf8: str = ""

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
class Index:
    name: str = ""
    hidden: bool = False
    is_generated: bool = False
    ordinal_position: int = 0
    comment: str = ""
    options: str = ""
    se_private_data: str = ""
    type: int = 0 ## sql/dd/types/index.h:enum_index_type
    algorithm: int = 0
    is_algorithm_explicit: bool = False
    is_visible: bool = False
    engine: str = ""
    engine_attribute: str = ""
    secondary_engine_attribute: str = ""
    elements: typing.List[IndexElement] = dataclasses.field(default_factory=list)

    def __post_init__(self):
        c: typing.List[IndexElement] = [IndexElement(**e) for e in self.elements]
        self.elements = c

    def get_effect_element(self) -> typing.List[IndexElement]:
        return [e for e in self.elements if e.length != 4294967295 and not e.hidden]

    def get_index_type(self):
        it = const.index_type.IndexType(self.type)
        if it == const.index_type.IndexType.IT_PRIMARY:
            return 'PRIMARY '
        elif it == const.index_type.IndexType.IT_UNIQUE:
            return 'UNIQUE '
        elif it == const.index_type.IndexType.IT_FULLTEXT:
            return 'FULLTEXT '
        elif it == const.index_type.IndexType.IT_MULTIPLE:
            return ''
        elif it == const.index_type.IndexType.IT_SPATIAL:
            return 'SPATIAL '


@modify_init
@dataclass(eq=False)
class ForeignElement:
    #column_opx: Column = None
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
    options: str = "" # properties
    se_private_data: str = "" # properties
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
        vs : typing.List[PartitionValue] = [PartitionValue(**v) for v in self.values]
        self.values = vs
        idx : typing.List[PartitionIndex] = [PartitionIndex(**v) for v in self.indexes]
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
    se_private_data: str = "" # properties
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

    def get_column(self, cond: typing.Callable[[Column], bool]) -> typing.List[Column]:
        return [c for c in self.columns if cond(c)]

    def get_primary_key_col(self) -> typing.List[Column]:
        primary_col = []
        for idx in self.indexes:
            if const.index_type.IndexType(idx.type) == const.index_type.IndexType.IT_PRIMARY:
                for ie in idx.get_effect_element():
                    primary_col.append(self.columns[ie.column_opx])

                return primary_col

    def get_default_DB_col(self) -> typing.List[Column]:
        return [c for c in self.columns if c.name in ["DB_TRX_ID", "DB_ROLL_PTR"]]


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
        hidden_column_name = ['DB_TRX_ID', 'DB_ROLL_PTR', 'DB_ROW_ID']
        for c in self.columns:
            if c.name in hidden_column_name:
                continue
            DDColumnType(c.type) != DDColumnType.LONG
        pass

    def gen_sql_for_index(self, idx: Index) -> str :
        cols_name = []
        for ie in idx.get_effect_element():
            col = self.columns[ie.column_opx]
            varlen, prekey_len = 1, 0
            if const.dd_column_type.DDColumnType.is_var(col.type): ## TODO: judge prefix key
                if col.collation_id == 255:
                    varlen = 4
                elif DDColumnType(col.type) in [DDColumnType.VARCHAR, DDColumnType.STRING] and not col.column_type_utf8.startswith("varb"):
                    varlen = 3
                if col.char_length > ie.length: # the index field data length must small than the original field
                    prekey_len = int(ie.length / varlen)
            prefix_part = f"({prekey_len})" if prekey_len != 0 else ""
            cols_name.append(f"`{col.name}`{prefix_part}")
        if len(cols_name) == 0:
            return ""

        idx_type_part = f"{idx.get_index_type()}KEY "
        idx_name_part = f"`{idx.name}` " if idx.name != "PRIMARY" else ''
        key_part = ",".join(cols_name)
        comment = f" COMMENT '{idx.comment}'" if idx.comment else ''
        return f"{idx_type_part}{idx_name_part}({key_part}){comment}"

    def gen_sql_for_partition(self) -> str:
        pt = const.partition.PartitionType(self.partition_type)
        if pt == const.partition.PartitionType.PT_RANGE:
            p = f"/*!50100 PARTITION BY RANGE({self.partition_expression_utf8}) (\n    "
            parts = []
            for par in self.partitions:
                parts.append(f"PARTITION {par.name} VALUES LESS THAN ({par.description_utf8})")
            return f"{p}{',\n    '.join(parts)}\n)*/"
        elif pt == const.partition.PartitionType.PT_HASH:
            return f"/*!50100 PARTITION BY HASH ({self.partition_expression_utf8}) PARTITIONS ({len(self.partitions)})*/"
        elif pt == const.partition.PartitionType.PT_KEY_55:
            return f"/*!50100 PARTITION BY KEY ({self.partition_expression_utf8}) PARTITIONS ({len(self.partitions)})*/"
        elif pt == const.partition.PartitionType.PT_LIST:
            p = f"/*!50100 PARTITION BY LIST ({self.partition_expression_utf8}) (\n    "
            parts = []
            for par in self.partitions:
                parts.append(f"PARTITION {par.name} VALUES IN ({par.description_utf8})")
            return f"{p}{',\n    '.join(parts)}\n)*/"



table_opts=[
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
"gipk"]

column_spec_size = {
    "DB_TRX_ID":6, "DB_ROLL_PTR":7
}

if __name__ == "__main__":
    import json
    with open("t1.json", "rb") as f:
        data = json.loads(f.read())
        print(data)
        table_sdi = Table(**data['dd_object'])
