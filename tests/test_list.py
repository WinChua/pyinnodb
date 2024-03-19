from context import *

from pyinnodb.disk_struct.list import MListBaseNode, MListNode, MPointer


def test_mlist():
    t = MListNode(
        prev=MPointer(
            page_number=1,
            page_offset=2,
        ),
        next=MPointer(
            page_number=3,
            page_offset=4,
        )
    )
    assert len(t.build()) == 12
