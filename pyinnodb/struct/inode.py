from elftools import construct
from .metaclass import Struct, Field, ArrayEntry
from .list import ListBaseNode, ListNode
from .fil import Fil, FilTrailer


class InodeEntry(Struct):
    fseg_id = Field(construct.UBInt64)
    not_full_list_used_page = Field(construct.UBInt32)
    list_base_free = Field(ListBaseNode)
    list_base_not_full = Field(ListBaseNode)
    list_base_full = Field(ListBaseNode)
    magic_number = Field(construct.UBInt32) # 97937874
    fragment_array = Field(
            lambda name:
                construct.Array(32, construct.UBInt32(name))
    )

class InodePage(Struct):
    fil_header = Field(Fil)
    list_node_inode_page = Field(ListNode)
    inodes = Field(lambda name: construct.Array(85, ArrayEntry(InodeEntry)))
    empty_space = Field(lambda name: construct.Field(name, 6))
    fil_tailer = Field(FilTrailer)
