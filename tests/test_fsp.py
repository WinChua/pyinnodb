from context import *

from pyinnodb.struct.fsp import MFspHeader
from pyinnodb.struct.list import MListBaseNode, MPointer
from pyinnodb.struct.inode import MInodeEntry

mtest_list_base_node = MListBaseNode(
    length=6,
    first=MPointer(
        page_number=7,
        page_offset=8,
    ),
    last=MPointer(
        page_number=9,
        page_offset=10,
    ),
)


def test_mfsp():
    fsp = MFspHeader(
        space_id=1,
        unused=0,
        highest_page_number=2,
        highest_page_number_init=3,
        flags=4,
        free_frag_page_number=5,
        list_base_free=mtest_list_base_node,
        list_base_free_frag=mtest_list_base_node,
        list_base_full_frag=mtest_list_base_node,
        next_seg_id=11,
        list_base_full_inode=mtest_list_base_node,
        list_base_free_inode=mtest_list_base_node,
    )
    assert fsp.sizeof() == len(fsp.build())
    assert fsp.parse(fsp.build()) == fsp


def test_minode():
    inode = MInodeEntry(
        fseg_id=1,
        not_full_list_used_page=2,
        list_base_free=mtest_list_base_node,
        list_base_not_full=mtest_list_base_node,
        list_base_full=mtest_list_base_node,
        magic_number=3,
        fragment_array=32 * [4],
    )
    logger.debug("inode is %s", inode)
    assert len(inode.build()) == 192
