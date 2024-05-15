from .meta import *

# Fil size 38


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
