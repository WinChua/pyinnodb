from .meta import *
from elftools import construct
from .meta import *
import json
import zlib


class Pointer(OStruct):
    page_number = UBInt32
    page_offset = UBInt16

    def seek_loc(self):
        return self.page_number * 16 * 1024 + self.page_offset

    def inode_idx(self):
        return self.page_number, int(
            (self.page_offset - 50) / 192
        )  # 50: sizeof(fil) + sizeof(list), 192: sizeof(INODE_ENTRY)


class ListBaseNode(OStruct):
    length = UBInt32
    first = Pointer
    last = Pointer


class ListNode(OStruct):
    prev = Pointer
    next = Pointer


class Fil(OStruct):
    checksum = UBInt32
    offset = UBInt32  # page_number, once the page initialized, it'll stored in header => indicator for if the page had been initialized
    pre_page = UBInt32  # pre_page of the same page_type
    next_page = UBInt32  # next_page of the same page_type, usefule for index page to
    # link all the page in the same level, not all page type use it
    lsn = UBInt64
    page_type = UBInt16  # determine how to parse the reset of the page,
    # allocated for filespace/extent management, transaction system
    # data dictionary, undo log, blobs, corse indexes
    flush_lsn = UBInt64  # only for page 0 of space 0
    spaceid = UBInt32


class FilTrailer(OStruct):
    old_checksum = UBInt32
    low_32_bits_lsn = UBInt32


class FspHeader(OStruct):
    space_id = UBInt32
    unused = UBInt32
    highest_page_number = UBInt32  # size, 已经在磁盘上分配的页数
    highest_page_number_init = UBInt32  # 已经初始化, 但不一定在磁盘上分配, 大于该值的page是free
    flags = UBInt32
    free_frag_page_number = UBInt32
    list_base_free = ListBaseNode
    list_base_free_frag = ListBaseNode
    list_base_full_frag = ListBaseNode
    next_seg_id = UBInt64
    list_base_full_inode = ListBaseNode
    list_base_free_inode = ListBaseNode


class IBufEntry2(OStruct):
    free_space_0 = OBits(2)
    buffer_flag_0 = OBits(1)
    change_buffer_flag_0 = OBits(1)
    free_space_1 = OBits(2)
    buffer_flag_1 = OBits(1)
    change_buffer_flag_1 = OBits(1)


class IBufPage(OStruct):
    fil = Fil
    change_buffer_bitmap = Array(4096, IBufEntry2)


class IndexHeader(OStruct):
    dir_slot_number = UBInt16
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
    leaf_pointer = Pointer  # pointer INODE entry in INODE page
    internal_space_id = UBInt32
    internal_pointer = Pointer  # pointer INODE entry in INODE page


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


# index page: fil/index_header/fseg_header/system records/user records/free space
class IndexPage(OStruct):
    # system records: 26
    # user records grown up
    # free space
    # page directory grown down
    # fil trailer
    fil = Fil
    index_header = IndexHeader
    fseg_header = FsegHeader
    # only root index page contains pointer to the fseg, other page, zero-filled
    system_records = IndexSystemRecord

    @classmethod
    def _parse(cls, stream, context=None):
        self = super()._parse(stream, context)
        n = self.index_header.dir_slot_number
        stream.seek(-self._consume_num + 1024 * 16 - 8 - (2 * n), 1)
        parser = Array(n, SBInt16(""))
        v = parser._parse(stream, context)
        self.page_directory = v
        return self


class SDIPage(OStruct):
    fil = Fil
    index_header = IndexHeader
    fseg_header = FsegHeader
    # only root index page contains pointer to the fseg, other page, zero-filled
    system_records = IndexSystemRecord

    @classmethod
    def _parse(cls, stream, context=None):
        self = super()._parse(stream, context)
        stream.seek(1024 * 16 - self._consume_num - 8, 1)
        logger.debug(
            "from index page, consume %d, seek %d", self._consume_num, stream.seek(
                0, 1)
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
            logger.debug(
                f"{col['name']}:{col['type']},{col['column_type_utf8']},")
        return json_data


class ddl(OStruct):
    dtype = UBInt32
    did = UBInt64
    dtrx = intfrombytes(6)
    dundo = intfrombytes(7)
    unzip_len = UBInt32
    zip_len = UBInt32


class InodeEntry(OStruct):
    fseg_id = UBInt64
    not_full_list_used_page = UBInt32
    list_base_free = ListBaseNode  # 当前segment下没有使用的extent
    list_base_not_full = ListBaseNode
    list_base_full = ListBaseNode
    magic_number = UBInt32  # 97937874
    fragment_array = Array(32, UBInt32(""))


class InodePage(OStruct):
    fil_header = Fil
    list_node_inode_page = ListNode
    inodes = Array(85, InodeEntry)
    empty_space = Array(6, UBInt8(""))
    # empty_space = Field(lambda name: construct.Field(name, 6))
    fil_tailer = FilTrailer


class XdesEntry(OStruct):
    fseg_id = UBInt64  # 8
    xdes_list = ListNode  # 12
    state = UBInt32  # 4
    # 用来标记一个extent的分配状态, 由于一个extent管理64个page, 这些page根据分配用途有几种状态:
    # 1: 所有page用于同一用途, 此时extent会属于某一个segment, 挂在inode的某个entry下
    # 2: 不同page分配不同用于, 又根据extent的使用状态分:
    #   free_frag: extent存在部分pagefree, 该extent会挂在fsp_header.list_free_frag
    #   full_frag: extent已经完全被使用, 挂在 fsp_header.list_full_frag
    # 3: free, extent下所有的page没有被使用
    page_state = BitArray(64, 2)  # 16 2bits per page, 1 => free, 2 => clean


class RecordHeader(OStruct):
    info_flags = OBits(4)
    num_record_owned = OBits(4)
    order = OBits(13)  # 本page的插入序号, infimum:0, supremum: 1, 用户数据从2开始递增
    record_type = OBits(3)
    next_record_offset = SBInt16
