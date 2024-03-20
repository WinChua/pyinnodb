import typing as t
from .list import MListBaseNode, MListNode
from .fil import MFil, MFilTrailer
from .xdes import MXdesEntry
from ..mconstruct import *

import logging
logger = logging.getLogger(__name__)


class MInodeEntry(CC):
    fseg_id: int = cfield(cs.Int64ub)
    not_full_list_used_page: int = cfield(cs.Int32ub)
    list_base_free: MListBaseNode = cfield(MListBaseNode)
    list_base_not_full: MListBaseNode = cfield(MListBaseNode)
    list_base_full: MListBaseNode = cfield(MListBaseNode)
    magic_number: int = cfield(cs.Int32ub)
    fragment_array: t.List[int] = cfield(carray(32, cs.Int32sb))

    def is_empty(self):
        return self.fseg_id == 0 or self.magic_number != 97937874

    def _get_list_base_usage_page(self, f, base_node: MListBaseNode):
        full_extents = {}
        next_pointer = base_node.first
        for i in range(base_node.length):
            loc = next_pointer.seek_loc()
            f.seek(loc-8)
            extent_desc = MXdesEntry.parse_stream(f)
            xdes_idx = next_pointer.xdes_idx()
            page_usage = extent_desc.desc_page_usage(xdes_idx)
            next_pointer = extent_desc.xdes_list.next
            full_extents[xdes_idx] = page_usage
        return full_extents

    def page_used(self, f):
        page_in_frag = [
            page_no for page_no in self.fragment_array if page_no != -1]
        not_full_extents = self._get_list_base_usage_page(
            f, self.list_base_not_full)
        full_extents = self._get_list_base_usage_page(f, self.list_base_full)
        free_extents = self._get_list_base_usage_page(f, self.list_base_free)
        return {
            "full": full_extents,
            "not_full": not_full_extents,
            "free": free_extents,
            "frag": page_in_frag}


class MInodePage(CC):  # fseg_create
    fil_header: MFil = cfield(MFil)
    list_node_inode_page: MListNode = cfield(MListNode)
    inodes: t.List[MInodeEntry] = cfield(
        carray(85, MInodeEntry))  # search for FSEG_ARR_OFFSET
    empty_space: t.List[int] = cfield(carray(6, cs.Int8ub))
    fil_tailer: MFilTrailer = cfield(MFilTrailer)

    def iter_inode(self, func=None):
        iter_value = []
        for i, inode in enumerate(self.inodes):
            if inode.is_empty():
                continue
            if func is not None:
                iter_value.append(func(inode))
        return iter_value
