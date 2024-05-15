from elftools import construct
from .metaclass import Struct, Field, Bits
from .list import Pointer
from .fil import Fil, FilTrailer
from .meta import *

import logging
import json

import zlib

logger = logging.getLogger(__name__)


class IndexHeader(OStruct):
    dir_slot_number = UBInt16
    # slots number, mini:2, which is for infmum, supremum
    # own: each record in slot owns the records between prev slot, up to and include itself
    heap_top_pos = UBInt16
    headp_records_number = UBInt16
    first_garbage = UBInt16
    garbage_space = UBInt16
    last_insert_pos = UBInt16  # byte offset
    page_dir = UBInt16
    page_dir_insert_number = UBInt16
    record_number = UBInt16
    max_trx_id = UBInt64
    page_level = UBInt16
    index_id = UBInt64


class FsegHeader(OStruct):
    leaf_space_id = UBInt32
    leaf_pointer = Pointer  ## pointer INODE entry in INODE page
    internal_space_id = UBInt32
    internal_pointer = Pointer  ## pointer INODE entry in INODE page


class SystemRecord(OStruct):
    info_flags = OBits(4)
    record_owned_num = OBits(4)
    order = OBits(13)
    record_type = OBits(3)
    next_record_offset = SBInt16
    marker = String(8)


class IndexSystemRecord(OStruct):
    infimum = SystemRecord
    supremum = SystemRecord


## index page: fil/index_header/fseg_header/system records/user records/free space
class IndexPage(OStruct):
    ## system records: 26
    ## user records grown up
    ## free space
    ## page directory grown down
    ## fil trailer
    fil = Fil
    index_header = IndexHeader
    fseg_header = FsegHeader
    ## only root index page contains pointer to the fseg, other page, zero-filled
    system_records = IndexSystemRecord

    @classmethod
    def _parse(cls, stream, context=None):
        self = super()._parse(stream, context)
        n = self.index_header.dir_slot_number
        stream.seek(-self._consume_num + 1024 * 16 - 8 - (2 * n), 1)
        parser = Array(n, SBInt16(""))
        v = parser._parse(stream , context)
        self.page_directory = v
        return self


class SDIPage(OStruct):
    fil = Fil
    index_header = IndexHeader
    fseg_header = FsegHeader
    ## only root index page contains pointer to the fseg, other page, zero-filled
    system_records = IndexSystemRecord

    @classmethod
    def _parse(cls, stream, context=None):
        self = super()._parse(stream, context)
        stream.seek(1024 * 16 - self._consume_num - 8, 1)
        logger.debug(
            "from index page, consume %d, seek %d", self._consume_num, stream.seek(0, 1)
        )
        logger.debug(
            "from index page, system_records _consume_num %d",
            self.system_records._consume_num,
        )
        self.fil_tailer = FilTrailer.parse_stream(stream)
        self.ddl = self._get_first_record(stream)
        return self

    def _get_first_record(self, stream):
        stream.seek(
            -16 * 1024
            + self.fil._consume_num
            + self.index_header._consume_num
            + self.fseg_header._consume_num
            + 3
            + 2
            + self.system_records.infimum.next_record_offset,
            1,
        )
        logger.debug(
            "stream infimum offset: %d, relative: %d",
            stream.seek(0, 1),
            stream.seek(0, 1) % (16 * 1024),
        )
        ddl_field = ddl.parse_stream(stream)
        logger.debug("ddl is %s", ddl_field)
        zipdata = stream.read(ddl_field.zip_len)
        json_data = json.loads(zlib.decompress(zipdata))
        for col in json_data["dd_object"]["columns"]:
            logger.debug(f"{col['name']}:{col['type']},{col['column_type_utf8']},")
        return json_data



class ddl(OStruct):
    dtype = UBInt32
    did = UBInt64
    dtrx = intfrombytes(6)
    dundo = intfrombytes(7)
    unzip_len = UBInt32
    zip_len = UBInt32
