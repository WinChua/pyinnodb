from context import *

from pyinnodb.struct.ibuf import *


def test_mibuf_page():
    with open(test_filename, "rb") as f:
        f.seek(1 * const.PAGE_SIZE)
        ibuf_page = MIBufPage.parse_stream(f)
        logger.info("ibuf is %s", ibuf_page.change_buffer_bitmap[:3])
