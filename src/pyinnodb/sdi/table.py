import dataclasses
import io
import decimal
import re
import sys
import json
import typing

if sys.version_info.minor >= 9:
    from functools import cache
else:
    cache = lambda x: x
import logging
from collections import namedtuple
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from .. import const
from ..const.dd_column_type import DDColumnType, DDColConf, rand_none
from ..disk_struct.data import MGeo, MTime2
from ..disk_struct.index import MIndexPage
from ..disk_struct.record import MRecordHeader
from .column import Column, Index
from .util import modify_init
from ..frm import frm as mfrm

logger = logging.getLogger(__name__)

column_type_size = re.compile("[^(]*[(]([^)]*)[)]")


class Lob:
    def __init__(self, data, off_page):
        self.data = data
        self.off_page = off_page

    def __str__(self):
        return f"<Lob length:{len(self.data)} preview:{self.data[:5] + b'..' + self.data[-5:]} off_page:{self.off_page}>"



@modify_init
@dataclass(eq=False)
class CheckCons:
    name: str = ""
    state: int = None
    check_clause: str = ""  # write binary
    check_clause_utf8: str = ""

    def gen(self):
        return f"CONSTRAINT `{self.name}` CHECK ({self.check_clause_utf8})"




@modify_init
@dataclass(eq=False)
class ForeignElement:
    column_opx: int = None
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

    def __post_init__(self):
        elements: typing.List[ForeignElement] = [
            ForeignElement(**c) for c in self.elements
        ]
        self.elements = elements

    def gen(self, column_name: typing.List[str]):
        cols = ",".join([f"`{column_name[c.column_opx]}`" for c in self.elements])
        rcols = ",".join([f"`{c.referenced_column_name}`" for c in self.elements])
        return f"CONSTRAINT `{self.name}` FOREIGN KEY ({cols}) REFERENCES `{self.referenced_table_schema_name}`.`{self.referenced_table_name}` ({rcols})"


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
    #

    def gen_ddl(self, schema):
        table_name = f"`{self.schema_ref}`.`{self.name}`" if schema else f"`{self.name}`"
        column_desc = []
        for c in self.columns:
            if c.hidden == const.column_hidden_type.ColumnHiddenType.HT_HIDDEN_SE.value:
                continue
            column_desc.append(c.gen_sql())
        for idx in self.indexes:
            if idx.hidden:
                continue
            column_desc.append(self.gen_sql_for_index(idx))
        column_desc.extend(self.gen_check_constraints())
        column_desc.extend(self.gen_foreign_key())
        column_desc = "\n  " + ",\n  ".join(column_desc) + "\n"
        parts = self.gen_sql_for_partition()
        collation = const.get_collation_by_id(self.collation_id)
        desc = f"ENGINE={self.engine} DEFAULT CHARSET={collation.CHARACTER_SET_NAME} COLLATE={collation.COLLATION_NAME}"
        comment = (
            "\nCOMMENT '" + self.comment + "'"
            if self.comment
            else ""
        )
        return f"CREATE TABLE {table_name} ({column_desc}) {desc}{parts}{comment}"

    def update_with_frm(self, frm):
        with open(frm, "rb") as f:
            frm_header = mfrm.MFrm.parse_stream(f)
            self.columns = []
            for i, col in enumerate(frm_header.cols):
                self.columns.append(col.to_dd_column(col.name, i, frm_header.column_labels))

            keys, key_name, key_comment = frm_header.keys[0]

            idx = keys.to_dd_index(key_name.decode(), frm_header.cols)

            idx.se_private_data = "root=3"

            self.columns.append(get_sys_col("DB_TRX_ID", len(self.columns)))
            self.columns.append(get_sys_col("DB_ROLL_PTR", len(self.columns)))

            self.indexes = [idx]

    
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

    def keys(self, no_primary=False, for_rand=False):
        v =  [f.name for f in dataclasses.fields(self.DataClass)] 
        if not no_primary and not for_rand:
            return  v
        primary_key_name = [f.name for f in self.get_primary_key_col()]
        v = [f for f in v if f not in primary_key_name]
        if not for_rand:
            return v
        target = [f.name for f in dataclasses.fields(self.DataClass) if DDColConf.get_col_type_conf(f.metadata['col'].type).rand_func != rand_none]

        return [f for f in v if f in target]

    def gen_rand_data_sql(self, size):
        rand_key = self.keys(for_rand=True)
        values = []
        for dc in self.gen_rand_data(size):
            values.append("(" + ",".join(self.transfer(dc, rand_key)) + ")")

        return f"INSERT INTO `{self.schema_ref}`.`{self.name}`({','.join(rand_key)}) values {', '.join(values)}"

    def gen_rand_data(self, size):
        keys = self.keys(for_rand=True)
        vs = []
        for i in range(size):
            v = []
            for f in dataclasses.fields(self.DataClass):
                if f.name not in keys:
                    continue
                func = DDColConf.get_col_type_conf(f.metadata['col'].type).rand_func
                if func:
                    v.append(func(f.metadata['col']))
            vs.append(v)
        return vs

    def wrap_transfer(self, rh, dc):
        return self.transfer(dc)
    
    def transfer(self, dc, keys=None):
        vs = []
        if keys is None:
            value = [getattr(dc, f.name) for f in dataclasses.fields(dc)]
        elif isinstance(dc, self.DataClass):
            value = [getattr(dc, k) for k in keys]
        elif isinstance(dc, list):
            value = dc

        for f in value:
            if isinstance(f, dict) or isinstance(f, list):
                vs.append(repr(json.dumps(f)))
            elif f is None:
                vs.append("NULL")
            elif isinstance(f, MTime2):
                vs.append(f.to_str())
            elif (
                isinstance(f, date)
                or isinstance(f, datetime)
            ):
                vs.append(f"'{str(f)}'")
            elif isinstance(f, MGeo):
                d = f.build().hex()  # .zfill(50)
                vs.append("0x" + d)
            elif isinstance(f, bytes):
                vs.append("0x"+f.hex())
            elif isinstance(f, decimal.Decimal):
                vs.append(str(f))
            else:
                vs.append(repr(f))
        return vs
        

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
            cols.append([c.name, c.pytype, c.dfield])

        return dataclasses.make_dataclass(self.name, cols)

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
            if "physical_pos" in c.private_data:
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
                data_layout_col.append((c, c_l.get(i, const.FFFFFFFF)))
            data_layout_col.sort(key=lambda c: int(c[0].private_data.get("physical_pos", 0)))
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
                    if ok:  # prefix
                        data_layout_col.append((col, prekey_len))
                    else:
                        data_layout_col.append((col, ie.length))

        if len(data_layout_col) == 0:
            for idx in self.indexes:
                for ie in idx.elements:
                    col = self.columns[ie.column_opx]
                    prekey_len, ok = col.index_prefix(ie)
                    if ok:  # prefix
                        data_layout_col.append((col, prekey_len))
                    else:
                        data_layout_col.append((col, ie.length))
                break

        return data_layout_col

    def build_primary_key_bytes(self, values) -> bytes:
        cols = self.get_disk_data_layout()
        cols = [c for c in cols if c[1] != ~0 & 0xFFFFFFFF]
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
        first_leaf_page = root_index_page.get_first_leaf_page(
            f, self.get_primary_key_col()
        )
        if isinstance(primary_key, tuple):
            primary_key = self.build_primary_key_bytes(primary_key)
        else:
            primary_key = self.build_primary_key_bytes((primary_key,))
        value_parser = MIndexPage.default_value_parser(
            self, hidden_col=hidden_col, # transfter=lambda rh, data: data,
            quick=False,
        )

        while first_leaf_page != const.FFFFFFFF:
            f.seek(first_leaf_page * const.PAGE_SIZE)
            index_page = MIndexPage.parse_stream(f)
            f.seek(first_leaf_page * const.PAGE_SIZE)
            page_dir_idx, match = index_page.binary_search_with_page_directory(
                primary_key, f
            )
            f.seek(
                first_leaf_page * const.PAGE_SIZE
                + index_page.page_directory[page_dir_idx]
                - 5
            )
            end_rh = MRecordHeader.parse_stream(f)
            logging.debug("end_rh is %s, match is %s", end_rh, match)
            if (
                match
                and const.RecordType(end_rh.record_type)
                == const.RecordType.Conventional
            ):  # the key
                return value_parser(end_rh, f)
            elif (
                match
                and const.RecordType(end_rh.record_type) == const.RecordType.NodePointer
            ):
                record_key = f.read(len(primary_key))
                page_num = f.read(4)
                first_leaf_page = int.from_bytes(page_num, "big")
            else:
                f.seek(
                    first_leaf_page * const.PAGE_SIZE
                    + index_page.page_directory[page_dir_idx + 1]
                    - 5
                )
                start_rh = MRecordHeader.parse_stream(f)
                owned = end_rh.num_record_owned
                first_leaf_page = (
                    const.FFFFFFFF  # no match if cur page is leaf then break loop
                )
                for i in range(owned + 1):
                    cur = f.tell()
                    if (
                        const.RecordType(start_rh.record_type)
                        == const.RecordType.Conventional
                    ):
                        record_primary_key = f.read(len(primary_key))
                        if record_primary_key == primary_key:
                            f.seek(-len(primary_key), 1)
                            v = value_parser(start_rh, f)
                            return v
                    elif (
                        const.RecordType(start_rh.record_type)
                        == const.RecordType.NodePointer
                    ):
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

            logging.debug("index_page.fil.next_page is %s", index_page.fil.next_page)
            if first_leaf_page == const.FFFFFFFF and index_page.fil.next_page != const.FFFFFFFF:
                first_leaf_page = index_page.fil.next_page

    def iter_record(self, f, hidden_col=False, garbage=False, transfer=None):
        root_page_no = int(self.indexes[0].private_data.get("root", 4))
        f.seek(root_page_no * const.PAGE_SIZE)
        root_index_page = MIndexPage.parse_stream(f)
        first_leaf_page = root_index_page.get_first_leaf_page(
            f, self.get_primary_key_col()
        )
        if first_leaf_page is None:
            return

        default_value_parser = MIndexPage.default_value_parser(
            self, hidden_col=hidden_col, transfer=transfer
        )

        result = []
        while first_leaf_page != const.FFFFFFFF:
            f.seek(first_leaf_page * const.PAGE_SIZE)
            index_page = MIndexPage.parse_stream(f)
            result.extend(
                index_page.iterate_record_header(
                    f, value_parser=default_value_parser, garbage=garbage
                )
            )
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

    def gen_foreign_key(self) -> typing.List[str]:
        column_name = [c.name for c in self.columns]
        return [f.gen(column_name) for f in self.foreign_keys]

    def gen_check_constraints(self) -> typing.List[str]:
        return [c.gen() for c in self.check_constraints]

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
            return "\n" + f"{p}{parts})*/"
        elif pt == const.partition.PartitionType.PT_HASH:
            return "\n" + f"/*!50100 PARTITION BY HASH ({self.partition_expression_utf8}) PARTITIONS ({len(self.partitions)})*/"
        elif pt == const.partition.PartitionType.PT_KEY_55:
            return "\n" + f"/*!50100 PARTITION BY KEY ({self.partition_expression_utf8}) PARTITIONS ({len(self.partitions)})*/"
        elif pt == const.partition.PartitionType.PT_LIST:
            p = f"/*!50100 PARTITION BY LIST ({self.partition_expression_utf8}) (\n    "
            parts = []
            for par in self.partitions:
                parts.append(f"PARTITION {par.name} VALUES IN ({par.description_utf8})")
            parts = ",\n    ".join(parts) + "\n"
            return "\n" + f"{p}{parts})*/"
        else:
            return ""


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


def get_sys_col(name, pos):
    if name == "DB_TRX_ID":
        return Column(
            name="DB_TRX_ID",
            type=10,
            hidden=2,
            ordinal_position=pos,
            char_length=6,
            has_no_default=False,
            default_value="",
            default_value_utf8_null=True,
            collation_id=63,
            is_explicit_collation=False,
        )
    elif name == "DB_ROLL_PTR":
        return Column(
            name=name,
            type=9,
            hidden=2,
            ordinal_position=pos,
            char_length=6,
            has_no_default=False,
            default_value="",
            default_value_utf8_null=True,
            collation_id=63,
            is_explicit_collation=False,
        )
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
