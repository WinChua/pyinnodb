from elftools import construct
from elftools.common.utils import struct_parse
from .meta import *
from .metaclass import Struct, Field


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
