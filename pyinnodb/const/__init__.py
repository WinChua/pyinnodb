
FIL_PAGE_INDEX = 17855 ## B-tree node */
FIL_PAGE_RTREE = 17854 ## R-tree node */
FIL_PAGE_SDI = 17853 ## Tablespace SDI Index page */
FIL_PAGE_UNDO_LOG = 2 ## Undo log page */
FIL_PAGE_INODE = 3 ## Index node */
FIL_PAGE_IBUF_FREE_LIST = 4 ## Insert buffer free list */

##  File page types introduced in MySQL/InnoDB 5.1.7 */
FIL_PAGE_TYPE_ALLOCATED = 0 ## Freshly allocated page */
FIL_PAGE_IBUF_BITMAP = 5 ## Insert buffer bitmap */
FIL_PAGE_TYPE_SYS = 6 ## System page */
FIL_PAGE_TYPE_TRX_SYS = 7 ## Transaction system data */
FIL_PAGE_TYPE_FSP_HDR = 8 ## File space header */
FIL_PAGE_TYPE_XDES = 9 ## Extent descriptor page */
FIL_PAGE_TYPE_BLOB = 10 ## Uncompressed BLOB page */
FIL_PAGE_TYPE_ZBLOB = 11 ## First compressed BLOB page */
FIL_PAGE_TYPE_ZBLOB2 = 12 ## Subsequent compressed BLOB page */
FIL_PAGE_TYPE_UNKNOWN = 13 ## In old tablespaces, garbage in FIL_PAGE_TYPE is replaced with this value when flushing pages. */
FIL_PAGE_COMPRESSED = 14 ## Compressed page */
FIL_PAGE_ENCRYPTED = 15 ## Encrypted page */
FIL_PAGE_COMPRESSED_AND_ENCRYPTED = 16 ## Compressed and Encrypted page */
FIL_PAGE_ENCRYPTED_RTREE = 17 ## Encrypted R-tree page */
FIL_PAGE_SDI_BLOB = 18 ## Uncompressed SDI BLOB page */
FIL_PAGE_SDI_ZBLOB = 19 ## Commpressed SDI BLOB page */
FIL_PAGE_TYPE_UNUSED = 20 ## Available for future use */
FIL_PAGE_TYPE_RSEG_ARRAY = 21 ## Rollback Segment Array page */
FIL_PAGE_TYPE_LOB_INDEX = 22 ## Index pages of uncompressed LOB */
FIL_PAGE_TYPE_LOB_DATA = 23 ## Data pages of uncompressed LOB */
FIL_PAGE_TYPE_LOB_FIRST = 24 ## The first page of an uncompressed LOB */
FIL_PAGE_TYPE_ZLOB_FIRST = 25 ## The first page of a compressed LOB */
FIL_PAGE_TYPE_ZLOB_DATA = 26 ## Data pages of compressed LOB */
FIL_PAGE_TYPE_ZLOB_INDEX = 27 ## Index pages of compressed LOB. This page contains an array of z_index_entry_t objects.*/
FIL_PAGE_TYPE_ZLOB_FRAG = 28 ## Fragment pages of compressed LOB. */
FIL_PAGE_TYPE_ZLOB_FRAG_ENTRY = 29 ## Index pages of fragment pages (compressed LOB). */
FIL_PAGE_TYPE_LAST = FIL_PAGE_TYPE_ZLOB_FRAG_ENTRY

PAGE_TYPE_MAP = {
  FIL_PAGE_INDEX : "INDEX PAGE",
  FIL_PAGE_RTREE  : "RTREE PAGE",
  FIL_PAGE_SDI  : "SDI INDEX PAGE",
  FIL_PAGE_UNDO_LOG  : "UNDO LOG PAGE",
  FIL_PAGE_INODE  : "INDEX NODE PAGE",
  FIL_PAGE_IBUF_FREE_LIST  : "INSERT BUFFER FREE LIST",
  FIL_PAGE_TYPE_ALLOCATED  : "FRESHLY ALLOCATED PAGE",
  FIL_PAGE_IBUF_BITMAP  : "INSERT BUFFER BITMAP",
  FIL_PAGE_TYPE_SYS  : "SYSTEM PAGE",
  FIL_PAGE_TYPE_TRX_SYS  : "TRX SYSTEM PAGE",
  FIL_PAGE_TYPE_FSP_HDR  : "FSP HDR",
  FIL_PAGE_TYPE_XDES  : "XDES",
  FIL_PAGE_TYPE_BLOB  : "UNCOMPRESSED BLOB PAGE",
  FIL_PAGE_TYPE_ZBLOB  : "FIRST COMPRESSED BLOB PAGE",
  FIL_PAGE_TYPE_ZBLOB2  : "SUBSEQUENT FRESHLY ALLOCATED PAGE",
  FIL_PAGE_TYPE_UNKNOWN  : "UNDO TYPE PAGE",
  FIL_PAGE_TYPE_LOB_FIRST  : "FIRST PAGE OF UNCOMPRESSED BLOB PAGE",
  FIL_PAGE_TYPE_LOB_INDEX  : "INDEX PAGE OF UNCOMPRESSED BLOB PAGE",
  FIL_PAGE_TYPE_LOB_DATA  : "DATA PAGE OF UNCOMPRESSED BLOB PAGE",
}

def get_page_type_name(typ):
    return PAGE_TYPE_MAP.get(typ, "unknow")
