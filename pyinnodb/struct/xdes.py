from elftools import construct
from .metaclass import Struct, Field
from .list import ListNode

def BitArray(size, length):
    return lambda name: construct.Bitwise(construct.Array(size, construct.Bits(name, length)))

class XdesEntry(Struct):
    fseg_id = Field(construct.UBInt64)
    xdes_list = Field(ListNode)
    state = Field(construct.UBInt32)
    page_state = Field(BitArray(64, 2))
