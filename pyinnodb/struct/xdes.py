from elftools import construct
from .metaclass import Struct, Field
from .list import ListNode
from .meta import *


class XdesEntry(OStruct):
    fseg_id = UBInt64  # 8
    xdes_list = ListNode  # 12
    state = UBInt32  # 4 用来标记一个extent的分配状态, 由于一个extent管理64个page, 这些page根据分配用途有几种状态:
                     # 1: 所有page用于同一用途, 此时extent会属于某一个segment, 挂在inode的某个entry下
                     # 2: 不同page分配不同用于, 又根据extent的使用状态分:
                     #   free_frag: extent存在部分pagefree, 该extent会挂在fsp_header.list_free_frag
                     #   full_frag: extent已经完全被使用, 挂在 fsp_header.list_full_frag
                     # 3: free, extent下所有的page没有被使用
    page_state = BitArray(64, 2)  # 16 2bits per page, 1 => free, 2 => clean
