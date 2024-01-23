from .list import MPointer
from .fil import MFil, MFilTrailer
from ..mconstruct import *

from .. import const

import logging
import json
import zlib

logger = logging.getLogger(__name__)


class MIndexHeader(CC):
    dir_slot_number: int = cfield(cs.Int16ub)
    heap_top_pos: int = cfield(cs.Int16ub)
    headp_records_number: int = cfield(cs.Int16ub)
    first_garbage: int = cfield(cs.Int16ub)
    garbage_space: int = cfield(cs.Int16ub)
    last_insert_pos: int = cfield(cs.Int16ub)  # byte offset
    page_dir: int = cfield(cs.Int16ub)
    page_dir_insert_number: int = cfield(cs.Int16ub)
    record_number: int = cfield(cs.Int16ub)
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


class MIndexSystemRecord(CC):
    infimum: MSystemRecord = cfield(MSystemRecord)
    supremum:  MSystemRecord = cfield(MSystemRecord)


class MIndexPage(CC):
    fil: MFil = cfield(MFil)
    index_header: MIndexHeader = cfield(MIndexHeader)
    fseg_header: MFsegHeader = cfield(MFsegHeader)
    system_records: MIndexSystemRecord = cfield(MIndexSystemRecord)

    def _post_parsed(self, stream, context, path):
        n = self.index_header.dir_slot_number
        size = self.sizeof()
        stream.seek(-size+const.PAGE_SIZE - 8 - (2 * n), 1)
        self.page_directory = carray(
            n, cs.Int16sb).parse_stream(stream, **context)


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
            -const.PAGE_SIZE + self.fil.sizeof()
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
        logger.debug("ddl is %s", ddl_field)
        zipdata = stream.read(ddl_field.zip_len)
        json_data = json.loads(zlib.decompress(zipdata))
        for col in json_data["dd_object"]["columns"]:
            logger.debug(
                f"{col['name']}:{col['type']},{col['column_type_utf8']},")
        return json_data
