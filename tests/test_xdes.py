from context import *
from pyinnodb.struct import XdesEntry, ListNode, Pointer, BitArray


def test_xdes():
    xe = XdesEntry(
        fseg_id=1,
        xdes_list=ListNode(
            prev=Pointer(page_number=1, page_offset=2),
            next=Pointer(page_number=3, page_offset=4),
        ),
        state=5,
        page_state=[0, 1, 0, 1] * 16,
    )
    logger.info(str(xe))
    logger.info(xe.build())
    logger.info(len(xe.build()))
    logger.info(xe._parse_order)
    assert len(xe.build()) == 40


def test_bitarray():
    ba = BitArray(4, 2)
    for i in range(8):
        data = ba.build([0, 0, 0, i])
        logger.info("%d, %s, %d", i, data, len(data))
