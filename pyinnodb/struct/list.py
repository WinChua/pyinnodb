from ..mconstruct import *

from .. import const


class MPointer(CC):
    page_number: int = cfield(cs.Int32ub)
    page_offset: int = cfield(cs.Int16ub)

    def seek_loc(self):
        return self.page_number * const.PAGE_SIZE + self.page_offset

    def inode_idx(self):
        return self.page_number, int(
            (self.page_offset - 50) / 192
        )  # 50: sizeof(fil) + sizeof(list), 192: sizeof(INODE_ENTRY)


class MListBaseNode(CC):
    length: int = cfield(cs.Int32ub)
    first: MPointer = cfield(MPointer)
    last: MPointer = cfield(MPointer)


class MListNode(CC):
    prev: MPointer = cfield(MPointer)
    next: MPointer = cfield(MPointer)
