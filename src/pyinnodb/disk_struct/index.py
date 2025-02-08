from .list import MPointer
from .fil import MFil, MFilTrailer
from .record import MRecordHeader
from ..mconstruct import *
from .inode import MInodeEntry


from .. import const
from ..const.dd_column_type import DDColumnType

from typing import TYPE_CHECKING
import typing

Table = typing.Type["Table"]
if TYPE_CHECKING:
    from ..sdi.table import Table


import logging
import json
import zlib

logger = logging.getLogger(__name__)


class MIndexHeader(CC):  # page0types.h call page header page_header_t
    dir_slot_number: int = cfield(cs.Int16ub)
    heap_top_pos: int = cfield(cs.Int16ub)
    record_format: int = cfield(cs.BitsInteger(1))
    heap_records_number: int = cfield(cs.BitsInteger(15))  # inf + sup + garbage
    # pointer to first garbage entry, and form link list by next_record
    first_garbage: int = cfield(cs.Int16ub)
    garbage_space: int = cfield(cs.Int16ub)
    last_insert_pos: int = cfield(cs.Int16ub)  # byte offset
    page_dir: int = cfield(cs.Int16ub)
    page_dir_insert_number: int = cfield(cs.Int16ub)
    record_number: int = cfield(cs.Int16ub)  # non-deleted user record
    max_trx_id: int = cfield(cs.Int64ub)
    page_level: int = cfield(cs.Int16ub)
    index_id: int = cfield(cs.Int64ub)


class MFsegHeader(CC):
    leaf_space_id: int = cfield(cs.Int32ub)
    # pointer INODE entry in INODE page
    leaf_pointer: MPointer = cfield(MPointer)
    internal_space_id: int = cfield(cs.Int32ub)
    # pointer INODE entry in INODE page
    internal_pointer: MPointer = cfield(MPointer)

    # should not use this way to determine the first leaf page number
    # as off-page may allocate first
    # def get_first_leaf_page(self, f):
    #     if self.leaf_pointer.page_number != 4294967295:
    #         f.seek(self.leaf_pointer.seek_loc())
    #         inode_entry = MInodeEntry.parse_stream(f)
    #         fp = inode_entry.first_page()
    #         if fp is not None:
    #             return fp

    #     f.seek(self.internal_pointer.seek_loc())
    #     inode_entry = MInodeEntry.parse_stream(f)
    #     return inode_entry.first_page()


class MSystemRecord(CC):
    info_flags: int = cfield(cs.BitsInteger(4))
    record_owned_num: int = cfield(cs.BitsInteger(4))
    order: int = cfield(cs.BitsInteger(13))
    record_type: int = cfield(cs.BitsInteger(3))
    next_record_offset: int = cfield(cs.Int16sb)
    marker: str = cfield(cstring(8))

    def get_current_offset(self):
        if const.RecordType(self.record_type) == const.RecordType.Infimum:
            return 99
        else:
            return 112


class MIndexSystemRecord(CC):
    infimum: MSystemRecord = cfield(MSystemRecord)
    supremum: MSystemRecord = cfield(MSystemRecord)


def get_rec_insert_state(rh: MRecordHeader, dd_object: Table):
    if rh.instant_version != 0:  ## is_versioned
        # if version == 0:
        #     return INSERTED_AFTER_UPGRADE_BEFORE_INSTANT_ADD_NEW_IMPLEMENTATION
        # else:
        #     return INSERTED_AFTER_INSTANT_ADD_NEW_IMPLEMENTATION
        pass
    elif rh.instant != 0:
        # return INSERTED_AFTER_INSTANT_ADD_OLD_IMPLEMENTATION
        pass
    else:
        pass
        ## is_upgraded_instant
        # if dd_object.private_data.get("instant_cols", None):
        #     return INSERTED_BEFORE_INSTANT_ADD_OLD_IMPLEMENTATION
        # else:
        #     return INSERTED_BEFORE_INSTANT_ADD_NEW_IMPLEMENTATION


class MIndexPage(CC):
    fil: MFil = cfield(MFil)
    index_header: MIndexHeader = cfield(MIndexHeader)
    fseg_header: MFsegHeader = cfield(MFsegHeader)
    system_records: MIndexSystemRecord = cfield(MIndexSystemRecord)

    @classmethod
    def default_value_parser(cls, dd_object: Table, transfter=None, hidden_col=False, quick=True):
        primary_data_layout_col = dd_object.get_disk_data_layout()

        def value_parser(rh: MRecordHeader, f):
            cur = f.tell()
            logger.debug(
                "-------start parse-----------rh: %s, @cur: %d/(%d, %d)",
                rh,
                cur,
                int(cur / const.PAGE_SIZE),
                cur % const.PAGE_SIZE,
            )
            if const.RecordType(rh.record_type) == const.RecordType.NodePointer:
                next_page_no = const.parse_mysql_int(f.read(4))
                return

            # data scheme version
            data_schema_version = 0
            f.seek(-MRecordHeader.sizeof(), 1)
            if rh.instant_version == 1:
                f.seek(-1, 1)
                data_schema_version = int.from_bytes(f.read(1), "big")

                logger.debug(
                    "record header is instant, with data version: %d",
                    data_schema_version,
                )

            cols_disk_layout = [
                d
                for d in primary_data_layout_col
                if d[0].version_valid(data_schema_version)
            ]
            logger.debug(
                "primary data layout is %s",
                ",".join(
                    f"{c[0].name}({c[0].ordinal_position})"
                    for c in primary_data_layout_col
                ),
            )

            if rh.instant == 1:
                f.seek(-1, 1)
                extra_byte = int.from_bytes(f.read(1), "big")
                cols_disk_layout = cols_disk_layout[:extra_byte]
                logger.debug(
                    "instant col extra byte is %s, &0x80 is %s, len(cols) is %d",
                    hex(extra_byte),
                    extra_byte & 0x80,
                    len(cols_disk_layout),
                )

            nullable_cols = [
                d[0]
                for d in cols_disk_layout
                if d[1] == 4294967295 and d[0].is_nullable
            ]

            logger.debug(
                "cols_disk_layout is %s", ",".join(c[0].name for c in cols_disk_layout)
            )
            logger.debug("nullable_cols is %s", ",".join(c.name for c in nullable_cols))

            if rh.instant == 0 and rh.instant_version == 0:
                nullable_cols = [c for c in nullable_cols if not c.is_instant_col_80017]
                cols_disk_layout = [
                    d for d in cols_disk_layout if not d[0].is_instant_col_80017
                ]

            nullcol_bitmask_size = int((len(nullable_cols) + 7) / 8)
            f.seek(-nullcol_bitmask_size - rh.instant_version - rh.instant, 1)
            null_bitmask = f.read(nullcol_bitmask_size)
            null_col_data = {}
            null_mask = int.from_bytes(null_bitmask, "big", signed=False)
            for i, c in enumerate(nullable_cols):
                if null_mask & (1 << i):
                    null_col_data[c.ordinal_position] = 1
            logger.debug(
                "null_col_data is %s, null_col size is %s, null_mask is %s",
                null_col_data,
                len(nullable_cols),
                null_bitmask,
            )
            may_var_col = [
                (i, c[0])
                for i, c in enumerate(cols_disk_layout)
                if DDColumnType.is_big(c[0].type)
                or DDColumnType.is_var(
                    c[0].type, mysqld_version=dd_object.mysql_version_id
                )
            ]
            logger.debug(
                "may_var_col is %s",
                ",".join(
                    f"({i})({c.ordinal_position}){c.name}" for i, c in may_var_col
                ),
            )

            ## read var
            f.seek(-nullcol_bitmask_size, 1)
            var_size = {}
            for i, c in may_var_col:
                if c.ordinal_position in null_col_data:
                    continue
                var_size[i] = const.parse_var_size(f)

            logger.debug("var_size is %s", var_size)
            disk_data_parsed = {}
            f.seek(cur)

            for i, (col, size_spec) in enumerate(cols_disk_layout):
                col_value = None
                vs = var_size.get(i, None)
                cur_before = f.tell()
                try:
                    if col.ordinal_position in null_col_data:
                        col_value = None
                    else:
                        col_value = col.read_data(f, vs, quick=quick)
                except Exception as e:
                    print("cur before is ", cur_before, vs, col)
                    raise e
                finally:
                    p_value = col_value
                    if isinstance(p_value, bytes):
                        if len(p_value) > 100:
                            p_value = p_value[:10] + b"..." + p_value[-10:]
                    elif isinstance(p_value, str):
                        if len(p_value) > 100:
                            p_value = p_value[:10] + "..." + p_value[-10:]

                    logger.debug(
                        "read_data: col[%s], col.type[%s], value[%s], i[%d], op[%d], vs[%s], from[%s],to[%s]",
                        col.name,
                        col.type,
                        p_value,
                        i,
                        col.ordinal_position,
                        vs,
                        cur_before % const.PAGE_SIZE,
                        f.tell() % const.PAGE_SIZE,
                    )
                if col.generation_expression_utf8 != "":
                    continue
                disk_data_parsed[col.name] = col_value

            for col in dd_object.columns:
                if (
                    col.name in ["DB_ROW_ID", "DB_TRX_ID", "DB_ROLL_PTR"]
                    and not hidden_col
                ) or col.private_data.get("version_dropped", 0) != 0:
                    if col.name in disk_data_parsed:
                        disk_data_parsed.pop(col.name)
                    continue
                if col.is_virtual or col.generation_expression_utf8 != "":
                    continue
                if col.name not in disk_data_parsed:
                    disk_data_parsed[col.name] = col.get_instant_default()

            klass = dd_object.DataClassHiddenCol if hidden_col else dd_object.DataClass
            if transfter is None:
                return klass(**disk_data_parsed)
            else:
                return transfter(klass(**disk_data_parsed))
            return

        return value_parser

    def _post_parsed(self, stream, context, path):
        n = self.index_header.dir_slot_number
        size = self.sizeof()
        stream.seek(-size + const.PAGE_SIZE - 8 - (2 * n), 1)
        self.page_directory = carray(n, cs.Int16sb).parse_stream(stream, **context)

    def get_first_leaf_page(self, stream, primary_cols):
        infimum_offset = self.system_records.infimum.get_current_offset()
        next_page = self.fil.offset
        stream.seek(
            next_page * const.PAGE_SIZE
            + infimum_offset
            + self.system_records.infimum.next_record_offset
        )
        stream.seek(-MRecordHeader.sizeof(), 1)
        rh = MRecordHeader.parse_stream(stream)
        rht = const.RecordType(rh.record_type)
        if rht == const.RecordType.Conventional:
            return next_page
        elif rht == const.RecordType.NodePointer:
            for c in primary_cols:
                c.read_data(stream)

            next_page = int.from_bytes(stream.read(4), "big")
            stream.seek(next_page * const.PAGE_SIZE)
            next_index_page = MIndexPage.parse_stream(stream)
            return next_index_page.get_first_leaf_page(stream, primary_cols)

    def iterate_record_header(self, f, value_parser=None, garbage=False):
        page_no = self.fil.offset
        result = []
        infimum_offset = self.system_records.infimum.get_current_offset()
        if not garbage:
            f.seek(page_no * const.PAGE_SIZE + infimum_offset)
            next_offset = self.system_records.infimum.next_record_offset
        else:
            if self.index_header.first_garbage == 0:
                return result
            f.seek(page_no * const.PAGE_SIZE)
            next_offset = self.index_header.first_garbage
        while True:
            if next_offset == 0:
                break
            f.seek(next_offset - MRecordHeader.sizeof(), 1)
            rh = MRecordHeader.parse_stream(f)
            if const.RecordType(rh.record_type) == const.RecordType.Supremum:
                break
            if value_parser is not None:
                cur = f.tell()
                result.append(value_parser(rh, f))
                f.seek(cur)
            next_offset = rh.next_record_offset

        return result

    def binary_search_with_page_directory(self, key, stream):
        # stream should be the start of a page
        cur_post = stream.seek(0, 1)
        if cur_post % const.PAGE_SIZE != 0:
            return None, False
        key_len = len(key)
        low, high = 0, len(self.page_directory) - 1
        cnt = 0
        logger.debug(
            "page dir is %s, low: %d, high: %d",
            ",".join(map(str, self.page_directory)),
            low,
            high,
        )
        while high > low + 1:
            target = int((high + low) / 2)
            stream.seek(cur_post + self.page_directory[target])
            record_key = stream.read(key_len)
            logger.debug(
                "low: %d, high: %d, target: %d, record_key: %s, key: %s, dir: %s",
                low,
                high,
                target,
                const.parse_mysql_int(record_key),
                const.parse_mysql_int(key),
                self.page_directory[target],
            )
            if record_key == key:
                return target, True
            elif key > record_key:
                high = target
            else:
                low = target
            cnt += 1
        return low, False


class MDDL(CC):
    dtype: int = cfield(cs.Int32ub)
    did: int = cfield(cs.Int64ub)
    dtrx: int = cfield(IntFromBytes(6))
    dundo: int = cfield(IntFromBytes(7))
    unzip_len: int = cfield(cs.Int32ub)
    zip_len: int = cfield(cs.Int32ub)


class MSDIPage(CC):
    fil: MFil = cfield(MFil)
    index_header: MIndexHeader = cfield(MIndexHeader)
    fseg_header: MFsegHeader = cfield(MFsegHeader)
    system_records: MIndexSystemRecord = cfield(MIndexSystemRecord)

    def _post_parsed(self, stream, context, path):
        size = self.sizeof()
        stream.seek(const.PAGE_SIZE - size - 8, 1)
        self.fil_tailer = MFilTrailer.parse_stream(stream)
        # self.ddl = next(self.iterate_sdi_record(stream))

    def ddl(self, stream, idx):
        return list(self.iterate_sdi_record(stream))[idx]
        return next(self.iterate_sdi_record(stream))

    def iterate_sdi_record(self, stream):
        cur_page_num = self.fil.offset
        while True:
            stream.seek(const.PAGE_SIZE * cur_page_num)
            fil = MFil.parse_stream(stream)
            index_header = MIndexHeader.parse_stream(stream)
            if index_header.page_level == 0:
                break
            fseg_header = MFsegHeader.parse_stream(stream)
            infimum = MSystemRecord.parse_stream(stream)
            stream.seek(-8 + infimum.next_record_offset + 12, 1)
            cur_page_num = int.from_bytes(stream.read(4), byteorder="big")

        while cur_page_num != 4294967295:
            stream.seek(cur_page_num * const.PAGE_SIZE)
            sdi_page = MSDIPage.parse_stream(stream)
            stream.seek(
                cur_page_num * const.PAGE_SIZE
                + sdi_page.system_records.infimum.get_current_offset()
            )

            next_offset = sdi_page.system_records.infimum.next_record_offset
            while next_offset != 0:
                stream.seek(next_offset - MRecordHeader.sizeof(), 1)
                rh = MRecordHeader.parse_stream(stream)
                if const.RecordType(rh.record_type) == const.RecordType.Supremum:
                    break
                next_offset = rh.next_record_offset
                cur = stream.tell()
                ddl = MDDL.parse_stream(stream)
                data = stream.read(ddl.zip_len)
                yield json.loads(zlib.decompress(data))
                stream.seek(cur)
            cur_page_num = sdi_page.fil.next_page

    def _get_first_record(self, stream):
        stream.seek(
            -const.PAGE_SIZE
            + self.fil.sizeof()
            + self.index_header.sizeof()
            + self.fseg_header.sizeof()
            + 3
            + 2
            + self.system_records.infimum.next_record_offset,
            1,
        )
        ddl_field = MDDL.parse_stream(stream)
        zipdata = stream.read(ddl_field.zip_len)
        json_data = json.loads(zlib.decompress(zipdata))
        return json_data
