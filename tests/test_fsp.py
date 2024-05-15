from context import *
from pyinnodb.struct import FspHeader, ListBaseNode, Pointer, InodeEntry

test_list_base_node = ListBaseNode(
    length = 6,
    first = Pointer(
        page_number = 7,
        page_offset = 8,
    ),
    last = Pointer(
        page_number = 9,
        page_offset = 10,
    ),
)

def test_fsp():
    fsp = FspHeader(
            space_id = 1,
            unused = 0,
            highest_page_number = 2,
            highest_page_number_init = 3,
            flags = 4,
            free_frag_page_number = 5,
            list_base_free = test_list_base_node,
            list_base_free_frag = test_list_base_node,
            list_base_full_frag = test_list_base_node,
            next_seg_id = 11,
            list_base_full_inode = test_list_base_node,
            list_base_free_inode = test_list_base_node,
    )
    assert len(fsp.build()) == 150 - 38


def test_inode():
    inode = InodeEntry(
            fseg_id = 1,
            not_full_list_used_page = 2,
            list_base_free = test_list_base_node,
            list_base_not_full = test_list_base_node,
            list_base_full = test_list_base_node,
            magic_number = 3,
            fragment_array = 32 * [4]
    )
    assert len(inode.build()) == 192
