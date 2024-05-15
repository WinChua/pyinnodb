from .list import MPointer
from .fil import MFil, MFilTrailer
from .record import MRecordHeader
from ..mconstruct import *

from .. import const

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

    def _post_parsed(self, stream, context, path):
        n = self.index_header.dir_slot_number
        size = self.sizeof()
        stream.seek(-size + const.PAGE_SIZE - 8 - (2 * n), 1)
        self.page_directory = carray(n, cs.Int16sb).parse_stream(stream, **context)

    def iterate_record_header(self, f, value_parser=None):
        page_no = self.fil.offset
        infimum_offset = self.system_records.infimum.get_current_offset()
        f.seek(page_no * const.PAGE_SIZE + infimum_offset)
        next_offset = self.system_records.infimum.next_record_offset
        while next_offset > 0:
            f.seek(next_offset - MRecordHeader.sizeof(), 1)
            rh = MRecordHeader.parse_stream(f)
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
            logger.info("lidx: %d, hidx: %d, tidx: %d", low_idx, heigh_idx, target_idx)
            logger.info(
                "cnt %d, key is %s, target_key is %s, result is %d",
                cnt,
                const.parse_mysql_int(key),
                const.parse_mysql_int(target_key),
                key < target_key,
            )
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
        logger.debug(
            "stream infimum offset: %d, relative: %d",
            stream.seek(0, 1),
            stream.seek(0, 1) % (const.PAGE_SIZE),
        )
        ddl_field = MDDL.parse_stream(stream)
        zipdata = stream.read(ddl_field.zip_len)
        json_data = json.loads(zlib.decompress(zipdata))
        for col in json_data["dd_object"]["columns"]:
            logger.debug(f"{col['name']}:{col['type']},{col['column_type_utf8']},")
        return json_data
