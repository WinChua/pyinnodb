from context import *

from pyinnodb.struct.xdes import *
from pyinnodb.struct.list import *


def test_mxdes():
    xe = MXdesEntry(
        fseg_id=1,
        xdes_list=MListNode(
            prev=MPointer(
                page_number=1,
                page_offset=2,
            ),
            next=MPointer(
                page_number=3,
                page_offset=4,
            )
        ),
        state=5,
        page_state=[0, 1, 0, 1] * 16
    )
    assert len(xe.build()) == 40
