from elftools import construct
from .meta import *
from .list import ListBaseNode, ListNode


class FspHeader(OStruct):
    space_id = UBInt32
    unused = UBInt32
    highest_page_number = UBInt32  # size, 已经在磁盘上分配的页数
    highest_page_number_init = UBInt32  # 已经初始化, 但不一定在磁盘上分配, 大于该值的page是free
    flags = UBInt32
    free_frag_page_number = UBInt32
    list_base_free = ListBaseNode
    list_base_free_frag = ListBaseNode
    list_base_full_frag = ListBaseNode
    next_seg_id = UBInt64
    list_base_full_inode = ListBaseNode
    list_base_free_inode = ListBaseNode
