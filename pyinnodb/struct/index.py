from elftools import construct
from .metaclass import Struct, Field, Bits
from .list import Pointer
from .fil import Fil, FilTrailer

import logging
import json

import zlib

logger = logging.getLogger(__name__)

class IndexHeader(Struct):
    dir_slot_number = Field(construct.UBInt16) # slots number, mini:2, which is for infmum, supremum
    # own: each record in slot owns the records between prev slot, up to and include itself
    heap_top_pos = Field(construct.UBInt16)
    headp_records_number = Field(construct.UBInt16)
    first_garbage = Field(construct.UBInt16)
    garbage_space = Field(construct.UBInt16)
    last_insert_pos = Field(construct.UBInt16) # byte offset 
    page_dir = Field(construct.UBInt16)
    page_dir_insert_number = Field(construct.UBInt16)
    record_number = Field(construct.UBInt16)
    max_trx_id = Field(construct.UBInt64)
    page_level = Field(construct.UBInt16)
    index_id = Field(construct.UBInt64)

class FsegHeader(Struct):
    leaf_space_id = Field(construct.UBInt32)
    leaf_pointer = Field(Pointer) ## pointer INODE entry in INODE page
    internal_space_id = Field(construct.UBInt32)
    internal_pointer = Field(Pointer) ## pointer INODE entry in INODE page

class SystemRecord(Struct):
    info_flags = Field(Bits(4))
    record_owned_num = Field(Bits(4))
    order = Field(Bits(13))
    record_type = Field(Bits(3))
    next_record_offset = Field(construct.UBInt16)
    marker = Field(lambda name: construct.String(name, 8))

class IndexSystemRecord(Struct):
    infimum = Field(SystemRecord)
    supremum = Field(SystemRecord)

## index page: fil/index_header/fseg_header/system records/user records/free space
class IndexPage(Struct):
    ## system records: 26
    ## user records grown up
    ## free space
    ## page directory grown down
    ## fil trailer
    fil = Field(Fil)
    index_header = Field(IndexHeader)
    fseg_header = Field(FsegHeader) ## only root index page contains pointer to the fseg, other page, zero-filled
    system_records = Field(IndexSystemRecord)

class SDIPage(Struct):
    fil = Field(Fil)
    index_header = Field(IndexHeader)
    fseg_header = Field(FsegHeader) ## only root index page contains pointer to the fseg, other page, zero-filled
    system_records = Field(IndexSystemRecord)

    def _parse(self, stream, context = None):
        stream.seek(1024 * 16 - self._consume_num - 8, 1)
        logger.debug("from index page, consume %d, seek %d", self._consume_num, stream.seek(0, 1))
        logger.debug("from index page, system_records _consume_num %d", self.system_records._consume_num)
        self.fil_tailer = FilTrailer()
        self.fil_tailer.parse_stream(stream)
        self._get_first_record(stream)

    def _get_first_record(self, stream):
        stream.seek(-16 * 1024 + 
                self.fil._consume_num + self.index_header._consume_num + self.fseg_header._consume_num + 3 + 2 + 
                self.system_records.infimum.next_record_offset, 1)
        logger.debug("stream infimum offset: %d, relative: %d", stream.seek(0, 1), stream.seek(0,1) % (16*1024))
        ddl_field = ddl()
        logger.debug("ddl is %s", ddl_field.parse_stream(stream))
        zipdata = stream.read(ddl_field.zip_len)
        json_data = json.loads(zlib.decompress(zipdata))
        for col in json_data["dd_object"]["columns"]:
            logger.debug(f"{col['name']}:{col['type']},{col['column_type_utf8']},") 

class intfrombytes(construct.Construct):
    def __init__(self, length):
        self.length = length

    def _parse(self, stream, context=None):
        return int.from_bytes(stream.read(self.length), "big")

class ddl(Struct):
    dtype = Field(construct.UBInt32)
    did = Field(construct.UBInt64)
    dtrx = Field(lambda name: intfrombytes(6))
    dundo = Field(lambda name: intfrombytes(7))
    unzip_len = Field(construct.UBInt32)
    zip_len = Field(construct.UBInt32)
