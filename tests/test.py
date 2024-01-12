from elftools import construct
from elftools.elf.elffile import ELFFile
from elftools.common.utils import struct_parse


def gf():
    return open("t1.ibd", "rb")


FIL = construct.Struct(
    "FIL",
    construct.UBInt32("checksum"),
    construct.UBInt32("offset"),
    construct.UBInt32("pre_page"),
    construct.UBInt32("next_page"),
    construct.UBInt64("LSN"),
    construct.UBInt16("page_type"),
    construct.UBInt64("flush_LSN"),
    construct.UBInt32("spaceid"),
)


LIST_BASE_NODE = [
    construct.UBInt32("len"),
    construct.UBInt32("first_page_number"),
    construct.UBInt16("first_page_offset"),
    construct.UBInt32("last_page_number"),
    construct.UBInt16("last_page_offset"),
]

LIST_NODE = [
    construct.UBInt32("prev_page_number"),
    construct.UBInt16("prev_page_offset"),
    construct.UBInt32("next_page_number"),
    construct.UBInt16("next_page_offset"),
]

# 一个page有initilized和allocated两种状态
# initilized, 被使用了, 但是有可能磁盘上没有分配
# allocated则是在磁盘上分配且使用了

FSP_HEADER = construct.Struct(
    "FSP_HEADER",
    construct.UBInt32("space_id"),
    construct.UBInt32("unused"),
    construct.UBInt32("highest_page_number"),  # FSP_SIZE
    construct.UBInt32("highest_page_number_init"),  # FSP_FREE_LIMIT
    construct.UBInt32("flags"),
    construct.UBInt32("free_frag_page_number"),
    construct.Struct(
        "list_base_node_free", *LIST_BASE_NODE
    ),  # 所有完全没有使用的extent, 可以完全用作同一个用途
    construct.Struct(
        "list_base_node_free_frag", *LIST_BASE_NODE
    ),  # 所有, 已经被部分使用的extent, 这些extent的page可能有不同用途
    # eg: Page(0)分配用于记录extent信息, 所属的extent剩余的其他page,可以
    # 分配用于其他用途
    construct.Struct(
        "list_base_node_full_frag", *LIST_BASE_NODE
    ),  # free_frag中已经满了的page会移动到这里来
    construct.UBInt64("next_unused_segment_id"),
    construct.Struct("list_base_node_full_inodes", *LIST_BASE_NODE),
    construct.Struct("list_base_node_free_inodes", *LIST_BASE_NODE),
)

XDES = construct.Struct(
    "XDES_ENTRY",
    construct.UBInt64("fseg_id"),
    construct.Struct("list_node_for_xdes", *LIST_NODE),
    construct.UBInt32("state"),
    construct.Array(
        16,
        construct.BitStruct(
            "page_state_bitmap",
            construct.Bits("p0", 2),
            construct.Bits("p1", 2),
            construct.Bits("p2", 2),
            construct.Bits("p3", 2),
        ),
    ),
    # construct.Array(8, construct.Bits("page_state_bitmap", 2)),
)

INODE_ENTRY = construct.Struct(
    "INODE_ENTRY",
    construct.UBInt64("fseg_id"),
    construct.Struct("list_base_node_free", *LIST_BASE_NODE),
    construct.Struct("list_base_node_not_full", *LIST_BASE_NODE),
    construct.Struct("list_base_node_full", *LIST_BASE_NODE),
    construct.UBInt32("magic_number"),
    construct.Array(32, construct.UBInt32("fragment")),
)


FIL_PAGE_INDEX = 17855  # B-tree node */
FIL_PAGE_RTREE = 17854  # R-tree node */
FIL_PAGE_SDI = 17853  # Tablespace SDI Index page */
FIL_PAGE_UNDO_LOG = 2  # Undo log page */
FIL_PAGE_INODE = 3  # Index node */
FIL_PAGE_IBUF_FREE_LIST = 4  # Insert buffer free list */

# File page types introduced in MySQL/InnoDB 5.1.7 */
FIL_PAGE_TYPE_ALLOCATED = 0  # Freshly allocated page */
FIL_PAGE_IBUF_BITMAP = 5  # Insert buffer bitmap */
FIL_PAGE_TYPE_SYS = 6  # System page */
FIL_PAGE_TYPE_TRX_SYS = 7  # Transaction system data */
FIL_PAGE_TYPE_FSP_HDR = 8  # File space header */
FIL_PAGE_TYPE_XDES = 9  # Extent descriptor page */
FIL_PAGE_TYPE_BLOB = 10  # Uncompressed BLOB page */
FIL_PAGE_TYPE_ZBLOB = 11  # First compressed BLOB page */
FIL_PAGE_TYPE_ZBLOB2 = 12  # Subsequent compressed BLOB page */
# In old tablespaces, garbage in FIL_PAGE_TYPE is replaced with this value when flushing pages. */
FIL_PAGE_TYPE_UNKNOWN = 13
FIL_PAGE_COMPRESSED = 14  # Compressed page */
FIL_PAGE_ENCRYPTED = 15  # Encrypted page */
FIL_PAGE_COMPRESSED_AND_ENCRYPTED = 16  # Compressed and Encrypted page */
FIL_PAGE_ENCRYPTED_RTREE = 17  # Encrypted R-tree page */
FIL_PAGE_SDI_BLOB = 18  # Uncompressed SDI BLOB page */
FIL_PAGE_SDI_ZBLOB = 19  # Commpressed SDI BLOB page */
FIL_PAGE_TYPE_UNUSED = 20  # Available for future use */
FIL_PAGE_TYPE_RSEG_ARRAY = 21  # Rollback Segment Array page */
FIL_PAGE_TYPE_LOB_INDEX = 22  # Index pages of uncompressed LOB */
FIL_PAGE_TYPE_LOB_DATA = 23  # Data pages of uncompressed LOB */
FIL_PAGE_TYPE_LOB_FIRST = 24  # The first page of an uncompressed LOB */
FIL_PAGE_TYPE_ZLOB_FIRST = 25  # The first page of a compressed LOB */
FIL_PAGE_TYPE_ZLOB_DATA = 26  # Data pages of compressed LOB */
# Index pages of compressed LOB. This page contains an array of z_index_entry_t objects.*/
FIL_PAGE_TYPE_ZLOB_INDEX = 27
FIL_PAGE_TYPE_ZLOB_FRAG = 28  # Fragment pages of compressed LOB. */
FIL_PAGE_TYPE_ZLOB_FRAG_ENTRY = (
    29  # Index pages of fragment pages (compressed LOB). */
)
FIL_PAGE_TYPE_LAST = FIL_PAGE_TYPE_ZLOB_FRAG_ENTRY

PAGE_TYPE_MAP = {
    FIL_PAGE_INDEX: "INDEX PAGE",
    FIL_PAGE_RTREE: "RTREE PAGE",
    FIL_PAGE_SDI: "SDI INDEX PAGE",
    FIL_PAGE_UNDO_LOG: "UNDO LOG PAGE",
    FIL_PAGE_INODE: "INDEX NODE PAGE",
    FIL_PAGE_IBUF_FREE_LIST: "INSERT BUFFER FREE LIST",
    FIL_PAGE_TYPE_ALLOCATED: "FRESHLY ALLOCATED PAGE",
    FIL_PAGE_IBUF_BITMAP: "INSERT BUFFER BITMAP",
    FIL_PAGE_TYPE_SYS: "SYSTEM PAGE",
    FIL_PAGE_TYPE_TRX_SYS: "TRX SYSTEM PAGE",
    FIL_PAGE_TYPE_FSP_HDR: "FSP HDR",
    FIL_PAGE_TYPE_XDES: "XDES",
    FIL_PAGE_TYPE_BLOB: "UNCOMPRESSED BLOB PAGE",
    FIL_PAGE_TYPE_ZBLOB: "FIRST COMPRESSED BLOB PAGE",
    FIL_PAGE_TYPE_ZBLOB2: "SUBSEQUENT FRESHLY ALLOCATED PAGE",
    FIL_PAGE_TYPE_UNKNOWN: "UNDO TYPE PAGE",
    FIL_PAGE_TYPE_LOB_FIRST: "FIRST PAGE OF UNCOMPRESSED BLOB PAGE",
    FIL_PAGE_TYPE_LOB_INDEX: "INDEX PAGE OF UNCOMPRESSED BLOB PAGE",
    FIL_PAGE_TYPE_LOB_DATA: "DATA PAGE OF UNCOMPRESSED BLOB PAGE",
}

XDES_STATE_NOT_INITED = 0
XDES_STATE_FREE = 1
XDES_STATE_FREE_FRAG = 2
XDES_STATE_FULL_FRAG = 3
XDES_STATE_FSEG = 4
XDES_STATE_FSEG_FRAG = 5

FSP = construct.Struct(
    "FSP",
    FIL,
    FSP_HEADER,
    construct.Array(256, XDES),
)

# with open("t1.ibd", "rb") as f:
#     ibd = struct_parse(FSP, f)
#     print(ibd.FSP_HEADER.highest_page_number)
#     print(len(ibd.XDES_ENTRY))
#     f.seek


def list_page(f):
    page_fils = []
    f.seek(38)
    fsp_header = struct_parse(FSP_HEADER, f)
    for i in range(0, fsp_header.highest_page_number):
        f.seek(i * 16 * 1024)
        page_fils.append(struct_parse(FIL, f))

    for p in page_fils:
        print(
            p.offset,
            p.pre_page,
            p.next_page,
            p.spaceid,
            PAGE_TYPE_MAP.get(p.page_type, "UNKNOW"),
        )


def list_extent(f):
    f.seek(150)
    xdess = []
    for i in range(256):
        xdess.append(struct_parse(XDES, f))

    for x in xdess:
        print(x.fseg_id)


def list_seg(f):
    f.seek(16 * 1024 * 2 + 38)
    list_node = struct_parse(construct.Struct("INODE_LIST", *LIST_NODE), f)
    print("list_node:", list_node)

    for i in range(85):
        print(struct_parse(INODE_ENTRY, f).fseg_id)


# list_seg(open("t1.ibd", "rb"))
# list_page(open("t1.ibd", "rb"))
# list_extent(open("t1.ibd", "rb"))


def list_fsp(f):
    f.seek(38)
    fsp_header = struct_parse(FSP_HEADER, f)
    print(fsp_header)


# list_fsp(open("t1.ibd", "rb"))


def get_seek(list_base_node):
    return (
        list_base_node.first_page_number * 1024 *
        16 + list_base_node.first_page_offset,
        list_base_node.last_page_number * 1024 * 16 + list_base_node.last_page_offset,
    )


def list_inode(f):
    f.seek(38)
    fsp_header = struct_parse(FSP_HEADER, f)
    print(fsp_header.list_base_node_free_inodes)
    first_seek, last_seek = get_seek(fsp_header.list_base_node_free_inodes)
    for i in range(fsp_header.list_base_node_free_inodes.len):
        f.seek(first_seek)
        print(struct_parse(construct.Struct("ln", *LIST_NODE), f))


INDEX_HEADER = construct.Struct(
    "INDEX_HEADER",
    construct.UBInt16("number_directory_slots"),
    construct.UBInt16("heap_top_position"),  # 当前页中, 已经使用了的字节数, 之后的都是free
    construct.UBInt16(
        "number_heap_records"
    ),  # 最高的bit存储, fromat flag, 当前页中records的格式, 可能的值: COMPACT/REDUNDANY
    # 其余存储, 本页的记录数: number(infimum) + num(supremum) + num(deleted)
    construct.UBInt16(
        "first_garbage_record_offset"
    ),  # pointer 指向了garbage records列表的第一个entry
    construct.UBInt16("garbage_space"),  # garbage records list 占用的字节辆
    construct.UBInt16("last_insert_position"),  # 本page中最后一条records插入的位置
    construct.UBInt16(
        "page_direction"
    ),  # LEFT, RIGHT, NO_DIRECTION, 表示当前页插入数据的时候, 插入位置与最后一条插入数据的位置关系
    construct.UBInt16("number_inserts_in_page_direction"),
    construct.UBInt16("number_records"),  # num(non-deleted) user recoreds
    construct.UBInt64("max_trx_id"),  # 修改了当前page中任意record的最大的事务id
    construct.UBInt16("page_level"),
    construct.UBInt32("idx_id"),  # 当前page属于哪个index
)


def inspect_index(f):
    f.seek(1024 * 16 * 2)
    fil = struct_parse(FIL, f)
    print(fil)


# inspect_index(gf())
# list_page(gf())
# list_fsp(gf())

f = gf()

page0_fil = struct_parse(FIL, f)
print(page0_fil, PAGE_TYPE_MAP.get(page0_fil.page_type))
page0_fsp_hdr = struct_parse(FSP_HEADER, f)
print(page0_fsp_hdr)
page0_xdes = struct_parse(XDES, f)
print("xdes:")
print(page0_xdes)
