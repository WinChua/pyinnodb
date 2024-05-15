from .list import MPointer
from .fil import MFil, MFilTrailer
from .record import MRecordHeader
from ..mconstruct import *
from .inode import MInodeEntry


from .. import const
from ..const.dd_column_type import DDColumnType
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


class MIndexPage(CC):
    fil: MFil = cfield(MFil)
    index_header: MIndexHeader = cfield(MIndexHeader)
    fseg_header: MFsegHeader = cfield(MFsegHeader)
    system_records: MIndexSystemRecord = cfield(MIndexSystemRecord)

    @classmethod
    def default_value_parser(cls, dd_object: Table, transfter = None):
        primary_data_layout_col = dd_object.get_disk_data_layout()
        def value_parser(rh: MRecordHeader, f):
            cur = f.tell()
            if const.RecordType(rh.record_type) == const.RecordType.NodePointer:
                next_page_no = const.parse_mysql_int(f.read(4))
                return

            # data scheme version
            data_schema_version = 0
            f.seek(-MRecordHeader.sizeof(), 1)
            if rh.instant == 1:
                f.seek(-1, 1)
                data_schema_version = int.from_bytes(f.read(1))

            cols_disk_layout = [d for d in primary_data_layout_col if d[0].version_valid(data_schema_version)]

            nullable_cols = [d[0] for d in cols_disk_layout if d[1] == 4294967295 and d[0].is_nullable]
            nullcol_bitmask_size = int((len(nullable_cols) + 7) / 8)
            f.seek(-nullcol_bitmask_size - rh.instant, 1)
            null_bitmask = f.read(nullcol_bitmask_size)
            null_col_data = {}
            null_mask = int.from_bytes(null_bitmask, signed=False)
            for i, c in enumerate(nullable_cols):
                if null_mask & (1 << i):
                    null_col_data[c.ordinal_position] = 1
            may_var_col = [
                (i, c[0])
                for i, c in enumerate(cols_disk_layout)
                if DDColumnType.is_big(c[0].type) or DDColumnType.is_var(c[0].type)
            ]

            ## read var
            f.seek(-nullcol_bitmask_size, 1)
            var_size = {}
            for i, c in may_var_col:
                if c.ordinal_position in null_col_data:
                    continue
                var_size[i] = const.parse_var_size(f)

            disk_data_parsed = {}
            f.seek(cur)

            for i, (col, size_spec) in enumerate(cols_disk_layout):
                col_value = None
                if col.ordinal_position in null_col_data:
                    col_value = None
                else:
                    vs = var_size.get(i, None)
                    col_value = col.read_data(f, vs)
                disk_data_parsed[col.name] = col_value

            for col in dd_object.columns:
                if (
                    col.name in ["DB_ROW_ID", "DB_TRX_ID", "DB_ROLL_PTR"]
                    or col.private_data.get("version_dropped", 0) != 0
                ):
                    if col.name in disk_data_parsed:
                        disk_data_parsed.pop(col.name)
                    continue
                if col.name not in disk_data_parsed:
                    disk_data_parsed[col.name] = col.get_instant_default()

            if transfter is None:
                print(dd_object.DataClass(**disk_data_parsed))
            else:
                transfter(dd_object.DataClass(**disk_data_parsed))
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
        stream.seek(next_page * const.PAGE_SIZE + infimum_offset + self.system_records.infimum.next_record_offset)
        stream.seek(-MRecordHeader.sizeof(), 1)
        rh = MRecordHeader.parse_stream(stream)
        rht = const.RecordType(rh.record_type)
        if rht == const.RecordType.Conventional:
            return next_page
        elif rht == const.RecordType.NodePointer:
            for c in primary_cols:
                c.read_data(stream)
            
            next_page = int.from_bytes(stream.read(4))
            stream.seek(next_page * const.PAGE_SIZE)
            next_index_page = MIndexPage.parse_stream(stream)
            return next_index_page.get_first_leaf_page(stream, primary_cols)

    def iterate_record_header(self, f, value_parser=None, garbage=False):
        page_no = self.fil.offset
        infimum_offset = self.system_records.infimum.get_current_offset()
        if not garbage:
            f.seek(page_no * const.PAGE_SIZE + infimum_offset)
            next_offset = self.system_records.infimum.next_record_offset
        else:
            if self.index_header.first_garbage == 0:
                return
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
                value_parser(rh, f)
                f.seek(cur)
            next_offset = rh.next_record_offset

    def binary_search_with_page_directory(self, key, stream):
        # stream should be the start of a page
        cur_post = stream.seek(0, 1)
        if cur_post % const.PAGE_SIZE != 0:
            return None, False
        key_len = len(key)
        low_idx, heigh_idx = 0, len(self.page_directory) - 1
        cnt = 0
        while low_idx < heigh_idx:
            cnt += 1
            if cnt > 100:
                return None, False
            target_idx = int((low_idx + heigh_idx) / 2)  # like ceil
            target_loc = cur_post + self.page_directory[target_idx]
            stream.seek(target_loc)
            target_key = stream.read(key_len)
            # logger.info("lidx: %d, hidx: %d, tidx: %d", low_idx, heigh_idx, target_idx)
            # logger.info(
            #     "cnt %d, key is %s, target_key is %s, result is %d",
            #     cnt,
            #     const.parse_mysql_int(key),
            #     const.parse_mysql_int(target_key),
            #     key < target_key,
            # )
            if key == target_key:
                return target_loc, True
            if key < target_key:
                low_idx = target_idx
            else:
                heigh_idx = target_idx
        return None, False


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
        self.ddl = self._get_first_record(stream)

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
        # logger.debug(
        #     "stream infimum offset: %d, relative: %d",
        #     stream.seek(0, 1),
        #     stream.seek(0, 1) % (const.PAGE_SIZE),
        # )
        ddl_field = MDDL.parse_stream(stream)
        zipdata = stream.read(ddl_field.zip_len)
        json_data = json.loads(zlib.decompress(zipdata))
        # for col in json_data["dd_object"]["columns"]:
        #     logger.debug(f"{col['name']}:{col['type']},{col['column_type_utf8']},")
        return json_data
