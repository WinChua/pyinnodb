from .list import MListBaseNode, MListNode
from .fil import MFil, MFilTrailer
from ..mconstruct import *

import typing as t


class MInodeEntry(CC):
    fseg_id: int = cfield(cs.Int64ub)
    not_full_list_used_page: int = cfield(cs.Int32ub)
    list_base_free: MListBaseNode = cfield(MListBaseNode)
    list_base_not_full: MListBaseNode = cfield(MListBaseNode)
    list_base_full: MListBaseNode = cfield(MListBaseNode)
    magic_number: int = cfield(cs.Int32ub)
    fragment_array: t.List[int] = cfield(carray(32, cs.Int32ub))


class MInodePage(CC):
    fil_header: MFil = cfield(MFil)
    list_node_inode_page: MListNode = cfield(MListNode)
    inodes: t.List[MInodeEntry] = cfield(carray(85, MInodeEntry))
    empty_space: t.List[int] = cfield(carray(6, cs.Int8ub))
    fil_tailer: MFilTrailer = cfield(MFilTrailer)
