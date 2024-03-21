from enum import Enum
import crcmod
from .collations import get_collation_by_id

PAGE_SIZE = 16 * 1024


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
FIL_PAGE_TYPE_ZLOB_FRAG_ENTRY = 29  # Index pages of fragment pages (compressed LOB). */
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


def get_page_type_name(typ):
    return PAGE_TYPE_MAP.get(typ, "unknow")


class XDESState(Enum):
    """
    XDES_NOT_INITED = 0 ## extent descriptor is not initialized */
    XDES_FREE = 1 ## extent is in free list of space */
    XDES_FREE_FRAG = 2 ## extent is in free fragment list of space */
    XDES_FULL_FRAG = 3 ## extent is in full fragment list of space */
    XDES_FSEG = 4 ## extent belongs to a segment */
    XDES_FSEG_FRAG = 5 ## fragment extent leased to segment */
    """

    XDES_NOT_INITED = 0  # extent descriptor is not initialized */
    XDES_FREE = 1  # extent is in free list of space */
    XDES_FREE_FRAG = 2  # extent is in free fragment list of space */
    XDES_FULL_FRAG = 3  # extent is in full fragment list of space */
    XDES_FSEG = 4  # extent belongs to a segment */
    XDES_FSEG_FRAG = 5  # fragment extent leased to segment */


class PageType(Enum):
    INDEX = 17855  # B-tree node */
    RTREE = 17854  # R-tree node */
    SDI = 17853  # Tablespace SDI Index page */
    UNDO_LOG = 2  # Undo log page */
    INODE = 3  # Index node */
    IBUF_FREE_LIST = 4  # Insert buffer free list */

    # File page types introduced in MySQL/InnoDB 5.1.7 */
    TYPE_ALLOCATED = 0  # Freshly allocated page */
    IBUF_BITMAP = 5  # Insert buffer bitmap */
    TYPE_SYS = 6  # System page */
    TYPE_TRX_SYS = 7  # Transaction system data */
    TYPE_FSP_HDR = 8  # File space header */
    TYPE_XDES = 9  # Extent descriptor page */
    TYPE_BLOB = 10  # Uncompressed BLOB page */
    TYPE_ZBLOB = 11  # First compressed BLOB page */
    TYPE_ZBLOB2 = 12  # Subsequent compressed BLOB page */
    # In old tablespaces, garbage in TYPE is replaced with this value when flushing pages. */
    TYPE_UNKNOWN = 13
    COMPRESSED = 14  # Compressed page */
    ENCRYPTED = 15  # Encrypted page */
    COMPRESSED_AND_ENCRYPTED = 16  # Compressed and Encrypted page */
    ENCRYPTED_RTREE = 17  # Encrypted R-tree page */
    SDI_BLOB = 18  # Uncompressed SDI BLOB page */
    SDI_ZBLOB = 19  # Commpressed SDI BLOB page */
    TYPE_UNUSED = 20  # Available for future use */
    TYPE_RSEG_ARRAY = 21  # Rollback Segment Array page */
    TYPE_LOB_INDEX = 22  # Index pages of uncompressed LOB */
    TYPE_LOB_DATA = 23  # Data pages of uncompressed LOB */
    TYPE_LOB_FIRST = 24  # The first page of an uncompressed LOB */
    TYPE_ZLOB_FIRST = 25  # The first page of a compressed LOB */
    TYPE_ZLOB_DATA = 26  # Data pages of compressed LOB */
    # Index pages of compressed LOB. This page contains an array of z_index_entry_t objects.*/
    TYPE_ZLOB_INDEX = 27
    TYPE_ZLOB_FRAG = 28  # Fragment pages of compressed LOB. */
    # Index pages of fragment pages (compressed LOB). */
    TYPE_ZLOB_FRAG_ENTRY = 29
    TYPE_LAST = TYPE_ZLOB_FRAG_ENTRY


class RecordType(Enum):
    Conventional = 0
    NodePointer = 1
    Infimum = 2
    Supremum = 3


class PageState:
    Free = 0x01  # not be used
    Clean = 0x02  # for page that has no un-flushed data, currently always 1

    @classmethod
    def is_page_free(cls, state):
        return bool(state & cls.Free)


def parse_mysql_unsigned(data):
    return parse_mysql_int(data, signed=False)


def parse_mysql_int(data, signed=True):
    if signed:
        data = int.to_bytes(data[0] ^ 0x80) + data[1:]
    return int.from_bytes(data, "big", signed=signed)


def encode_mysql_unsigned(data, length):
    return encode_mysql_int(data, length, False)


def encode_mysql_int(value, length, signed=True):
    data = int.to_bytes(value, length, "big", signed=signed)
    if signed:
        data = int.to_bytes(data[0] ^ 0x80) + data[1:]
    return data


crc32c = crcmod.Crc(poly=0x11EDC6F41, rev=True, initCrc=0, xorOut=0xFFFFFFFF)


def page_checksum_crc32c(page_data):
    body = page_data[38 : PAGE_SIZE - 8]
    header = page_data[4:26]
    return crc32c.new(header).crcValue ^ crc32c.new(body).crcValue


def show_start_end_format(start, end):
    return f"{start}" if start == end else f"{start}-{end}"


def show_seq_page_list(page_list):
    page_list = sorted(page_list)
    if len(page_list) == 0:
        return "empty"
    start = page_list[0]
    end = start
    lines = []

    for page_no in page_list[1:]:
        if page_no == end + 1:
            end = page_no
        else:
            lines.append(show_start_end_format(start, end))
            start = page_no
            end = start
    lines.append(show_start_end_format(start, end))
    return "/".join(lines)


COLUMN_IDX_TYPE_NONE = 1
COLUMN_IDX_TYPE_PRIM = 2
COLUMN_IDX_TYPE_UNIQ = 3
COLUMN_IDX_TYPE_MULT = 4
