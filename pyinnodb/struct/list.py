from elftools import construct
from elftools.common.utils import struct_parse
from .metaclass import Struct, Field

class Pointer(Struct):
    page_number = Field(construct.UBInt32)
    page_offset = Field(construct.UBInt16)

    def seek_loc(self):
        return self.page_number * 16 * 1024 + self.page_offset

    def inode_idx(self):
        return self.page_number, int((self.page_offset - 50) / 192) # 50: sizeof(fil) + sizeof(list), 192: sizeof(INODE_ENTRY)


class ListBaseNode(Struct):
    length = Field(construct.UBInt32)
    first = Field(Pointer)
    last  = Field(Pointer)
    # first_page_number = Field(construct.UBInt32)
    # first_page_offset = Field(construct.UBInt16)
    # last_page_number  = Field(construct.UBInt32)
    # last_page_offset  = Field(construct.UBInt16)

class ListNode(Struct):
    prev = Field(Pointer)
    next = Field(Pointer)
    # prev_page_number = Field(construct.UBInt32)
    # prev_page_offset = Field(construct.UBInt16)
    # next_page_number = Field(construct.UBInt32)
    # next_page_offset = Field(construct.UBInt16)

