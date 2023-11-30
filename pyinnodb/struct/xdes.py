from elftools import construct
from .metaclass import Struct, Field
from .list import ListNode
from .meta import *


class XdesEntry(OStruct):
    fseg_id = UBInt64  # 8
    xdes_list = ListNode  # 12
    state = UBInt32  # 4
    page_state = BitArray(64, 2)  # 16
