from context import *
from pyinnodb.struct import *
from pyinnodb.const import get_page_type_name
from pyinnodb import const

from pyinnodb.struct.index import *
from pyinnodb.struct.fil import *


def test_list_mpage():
    with open(test_filename, "rb") as f:
        mfil = MFil.parse_stream(f)
        logger.info(fil,)
        logger.info(mfil)
        mfsp_header = MFspHeader.parse_stream(f)
        logger.info(mfsp_header)
        for i in range(mfsp_header.highest_page_number):
            f.seek(i * const.PAGE_SIZE)
            mfili = MFil.parse_stream(f)
            logger.info("offset: %d, page_type: %s", mfili.offset,
                        const.get_page_type_name(mfili.page_type))


def test_fsp_mheader():
    with open(test_filename, "rb") as f:
        mfil = MFil.parse_stream(f)
        mfsp_header = MFspHeader.parse_stream(f)
        logger.info("fsp_header is %s", mfsp_header)
        xdess: t.List[MXdesEntry] = []
        for i in range(256):
            xdess.append(MXdesEntry.parse_stream(f))
            logger.info("idx: %d, segid: %d", i, xdess[-1].fseg_id)
            break


def test_msdi_page():
    with open(test_filename, "rb") as f:
        f.seek(3 * const.PAGE_SIZE)
        sdi_page = MSDIPage.parse_stream(f)
        logger.info("sdi_page is %s", sdi_page)
        logger.info("sdi_page ddl is %s", sdi_page.ddl)


test_index_page_data = b"2\x17X&\x00\x00\x00\x04\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x01*,\xa2E\xbf\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03\x00\x02\x00\xcf\x80\x05\x00\x00\x00\x00\x00\xb9\x00\x05\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x9c\x00\x00\x00\x03\x00\x00\x00\x02\x02r\x00\x00\x00\x03\x00\x00\x00\x02\x01\xb2\x01\x00\x02\x00\x1cinfimum\x00\x04\x00\x0b\x00\x00supremum"


def test_minode_page():
    with open(test_filename2, "rb") as f:
        f.seek(2 * const.PAGE_SIZE)
        inode_page = MInodePage.parse_stream(f)
        logger.info(inode_page)
