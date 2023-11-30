from elftools import construct
from .list import ListBaseNode, ListNode
from .fil import Fil, FilTrailer

from .meta import *


class InodeEntry(OStruct):
    fseg_id = UBInt64
    not_full_list_used_page = UBInt32
    list_base_free = ListBaseNode
    list_base_not_full = ListBaseNode
    list_base_full = ListBaseNode
    magic_number = UBInt32  # 97937874
    fragment_array = Array(32, UBInt32(""))


class InodePage(OStruct):
    fil_header = Fil
    list_node_inode_page = ListNode
    inodes = Array(85, InodeEntry)
    empty_space = Array(6, UBInt8(""))
    # empty_space = Field(lambda name: construct.Field(name, 6))
    fil_tailer = FilTrailer
