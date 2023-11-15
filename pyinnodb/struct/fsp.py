from elftools import construct
from .metaclass import Struct, Field
from .list import ListBaseNode, ListNode

class FspHeader(Struct):
    space_id = Field(construct.UBInt32)
    unused   = Field(construct.UBInt32)
    highest_page_number = Field(construct.UBInt32)
    highest_page_number_init = Field(construct.UBInt32)
    flags = Field(construct.UBInt32)
    free_frag_page_number = Field(construct.UBInt32)
    list_base_free = Field(ListBaseNode)
    list_base_free_frag = Field(ListBaseNode)
    list_base_full_frag = Field(ListBaseNode)
    next_seg_id = Field(construct.UBInt64)
    list_base_full_inode = Field(ListBaseNode)
    list_base_free_inode = Field(ListBaseNode)

# FSP_HEADER = construct.Struct("FSP_HEADER",
#         construct.UBInt32("space_id"),
#         construct.UBInt32("unused"),
#         construct.UBInt32("highest_page_number"), # FSP_SIZE
#         construct.UBInt32("highest_page_number_init"), # FSP_FREE_LIMIT
#         construct.UBInt32("flags"),
#         construct.UBInt32("free_frag_page_number"),
#         construct.Struct("list_base_node_free", *LIST_BASE_NODE), ## 所有完全没有使用的extent, 可以完全用作同一个用途
#         construct.Struct("list_base_node_free_frag", *LIST_BASE_NODE), ## 所有, 已经被部分使用的extent, 这些extent的page可能有不同用途
#                                                                       ## eg: Page(0)分配用于记录extent信息, 所属的extent剩余的其他page,可以
#                                                                       ## 分配用于其他用途
#         construct.Struct("list_base_node_full_frag", *LIST_BASE_NODE), ## free_frag中已经满了的page会移动到这里来
#         construct.UBInt64("next_unused_segment_id"),
#         construct.Struct("list_base_node_full_inodes", *LIST_BASE_NODE),
#         construct.Struct("list_base_node_free_inodes", *LIST_BASE_NODE)
# )
