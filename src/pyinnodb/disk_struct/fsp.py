from ..mconstruct import *
from .list import MListBaseNode, MListNode
from .fil import MFil
from .xdes import MXdesEntry

from .. import const

import typing as t


class MFspHeader(CC):
    space_id: int = cfield(cs.Int32ub)
    unused: int = cfield(cs.Int32ub)
    highest_page_number: int = cfield(cs.Int32ub)  # size, 已经在磁盘上分配的页数
    highest_page_number_init: int = cfield(
        cs.Int32ub
    )  # 已经初始化, 但不一定在磁盘上分配, 大于该值的page是free
    flags: int = cfield(cs.Int32ub)
    free_frag_page_number: int = cfield(cs.Int32ub)
    list_base_free: MListBaseNode = cfield(MListBaseNode)
    list_base_free_frag: MListBaseNode = cfield(MListBaseNode)
    list_base_full_frag: MListBaseNode = cfield(MListBaseNode)
    next_seg_id: int = cfield(cs.Int64ub)
    list_base_full_inode: MListBaseNode = cfield(MListBaseNode)
    list_base_free_inode: MListBaseNode = cfield(MListBaseNode)


class MFspPage(CC):
    fil: MFil = cfield(MFil)
    fsp_header: MFspHeader = cfield(MFspHeader)
    xdess: t.List[MXdesEntry] = cfield(carray(256, MXdesEntry))
    info_max: t.List[int] = cfield(cs.Bytes(115))
    has_sdi_page: int = cfield(cs.Int32ub)
    sdi_page_no: int = cfield(cs.Int32ub)

    def iter_page(self, f, iter_func=None):
        for pn in range(self.fsp_header.highest_page_number):
            f.seek(pn * const.PAGE_SIZE)
            if iter_func is not None:
                iter_func(f)
