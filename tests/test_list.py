from context import *
from pyinnodb.struct import ListNode, ListBaseNode, Pointer

test_list_case = [
        (b'\x00\x00\x00\x01\x00\x02\x00\x00\x00\x03\x00\x04', 
            ListNode(prev=Pointer(page_number=1,page_offset=2),
                next=Pointer(page_number=3,page_offset=4)))

]

def test_list_node():
    totest(ListNode, test_list_case)
