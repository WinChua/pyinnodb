from context import *
from pyinnodb.struct import *


def test_ibuf_page():
    with open(test_filename, "rb") as f:
        f.seek(1 * 16 * 1024)
        ibuf_page = IBufPage.parse_stream(f)
        logger.info("ibuf is %s", ibuf_page)
