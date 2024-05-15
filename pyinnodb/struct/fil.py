from io import BytesIO
from elftools import construct
from elftools.common.utils import struct_parse
from .metaclass import Struct, Field


class Fil(Struct):
    checksum  = Field(construct.UBInt32)
    offset    = Field(construct.UBInt32) # page_number, once the page initialized, it'll stored in header => indicator for if the page had been initialized
    pre_page  = Field(construct.UBInt32) # pre_page of the same page_type
    next_page = Field(construct.UBInt32) # next_page of the same page_type, usefule for index page to
                                         # link all the page in the same level, not all page type use it
    lsn       = Field(construct.UBInt64)
    page_type = Field(construct.UBInt16) # determine how to parse the reset of the page,
                                         # allocated for filespace/extent management, transaction system
                                         # data dictionary, undo log, blobs, corse indexes
    flush_lsn = Field(construct.UBInt64) # only for page 0 of space 0
    spaceid   = Field(construct.UBInt32)

class FilTrailer(Struct):
    old_checksum = Field(construct.UBInt32)
    low_32_bits_lsn = Field(construct.UBInt32)
