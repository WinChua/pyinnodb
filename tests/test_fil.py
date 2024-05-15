from collections import Counter
import crcmod
from context import *
from pyinnodb.disk_struct.fil import MFil, MFilTrailer
from pyinnodb.disk_struct.fsp import MFspHeader, MFspPage
from pyinnodb.disk_struct.inode import MInodePage

test_fil_case = [
    (
        b"\x86$\xad\x90\x00\x00\x00\x00\x00\x018\xe4\x00\x00\x00\x01\x00\x00\x00\x00\x01*\x07\xa2\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03",
        MFil(
            checksum=2250550672,
            offset=0,
            pre_page=80100,
            next_page=1,
            lsn=19531682,
            page_type=8,
            flush_lsn=0,
            spaceid=3,
        ),
    ),
    (
        b":8\rX\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01)\xfaN\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03",
        MFil(
            checksum=976751960,
            offset=1,
            pre_page=0,
            next_page=0,
            lsn=19528270,
            page_type=5,
            flush_lsn=0,
            spaceid=3,
        ),
    ),
    (
        b"\t\xbd\xae\xf0\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01*\x07\xa2\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03",
        MFil(
            checksum=163426032,
            offset=2,
            pre_page=0,
            next_page=0,
            lsn=19531682,
            page_type=3,
            flush_lsn=0,
            spaceid=3,
        ),
    ),
    (
        b"Ezp\xec\x00\x00\x00\x03\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x01*\x1f\xcbE\xbd\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03",
        MFil(
            checksum=1165652204,
            offset=3,
            pre_page=4294967295,
            next_page=4294967295,
            lsn=19537867,
            page_type=17853,
            flush_lsn=0,
            spaceid=3,
        ),
    ),
    (
        b"2\x17X&\x00\x00\x00\x04\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x01*,\xa2E\xbf\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03",
        MFil(
            checksum=840390694,
            offset=4,
            pre_page=4294967295,
            next_page=4294967295,
            lsn=19541154,
            page_type=17855,
            flush_lsn=0,
            spaceid=3,
        ),
    ),
    (
        b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        MFil(
            checksum=0,
            offset=0,
            pre_page=0,
            next_page=0,
            lsn=0,
            page_type=0,
            flush_lsn=0,
            spaceid=0,
        ),
    ),
]


def test_mfil():
    for data in test_fil_case:
        assert MFil.parse(data[0]) == data[-1]
        data[-1].build() == data[0]
        logger.info("data[-1] is %s", data[-1])


def test_list_mpage():
    with open(test_filename, "rb") as f:
        mfil = MFil.parse_stream(f)
        logger.info(mfil)
        mfsp_header = MFspHeader.parse_stream(f)
        logger.info(mfsp_header)
        for i in range(mfsp_header.highest_page_number):
            f.seek(i * const.PAGE_SIZE)
            mfili = MFil.parse_stream(f)
            logger.info("offset: %d, page_type: %s", mfili.offset,
                        const.get_page_type_name(mfili.page_type))


def test_validate_page():
    with open(test_filename, "rb") as f:
        mfil = MFil.parse_stream(f)
        mfsp_header = MFspHeader.parse_stream(f)
        for i in range(mfsp_header.highest_page_number):
            f.seek(i*const.PAGE_SIZE)
            page_data = f.read(const.PAGE_SIZE)
            mfil = MFil.parse(page_data)
            crc32c = const.page_checksum_crc32c(page_data)
            if mfil.page_type == const.FIL_PAGE_TYPE_ALLOCATED:
                logger.info("allocated page no need to calculate checksum")
                continue
            logger.info("fil check sum:%x, caculate check sum: %x",
                        mfil.checksum, crc32c)


def test_page_checksum():
    with open(test_filename1, "rb") as f:
        page0 = f.read(const.PAGE_SIZE)
        mfil = MFil.parse(page0)
        logger.info("checksum is %x", mfil.checksum)
        logger.info("page check sum is %x", const.page_checksum_crc32c(page0))
        mfil_tailer = MFilTrailer.parse(page0[-8:])
        logger.info(mfil_tailer)
        logger.info("tailer checksum is %x", mfil_tailer.old_checksum)
        import zlib
        crc321 = zlib.crc32(mfil.build()[4:26])
        logger.info("crc321 is %x", crc321)
        logger.info("page crc32 is %x", zlib.crc32(page0[4:26]))
        crc322 = zlib.crc32(page0[38:const.PAGE_SIZE-8])
        logger.info("c2 is %x", crc322)
        logger.info("result is %x", crc321 ^ crc322)
        header = mfil.build()[4:26]
        body = page0[38:const.PAGE_SIZE - 8]
        import functools
        logger.info("checksum crc32c is %x", checksum(header) ^ checksum(body))

    target_mfil = MFil.parse(
        b'\xed\x18\xb5\xc2\x00\x00\x00\x00\x00\x018\xe4\x00\x00\x00\x01\x00\x00\x00\x00\x00\x017X\x00\x08\x00\x00\x00\x00\x01"m\x8e\x00\x00\x00\x00')
    logger.info(target_mfil)


crc32c = crcmod.Crc(poly=0x11EDC6F41, rev=True,
                    initCrc=0, xorOut=0xFFFFFFFF)


def checksum(data):
    return crc32c.new(data).crcValue


def the_checksum():

    MAX = 0xFFFFFFFF
    MASK1 = 1463735687
    MASK2 = 1653893711

    def fold_pair(n1, n2):
        return (((((((n1 ^ n2 ^ MASK2) << 8) & MAX) + n1) & MAX) ^ MASK1) + n2) & MAX

    return fold_pair


def test_desc():
    with open(test_filename, "rb") as f:
        fsp_page = MFspPage.parse_stream(f)
        f.seek(0)
        page_data_0 = f.read(const.PAGE_SIZE)
        logger.info("cal check: %s", const.page_checksum_crc32c(page_data_0))
        logger.info("fil is %s", fsp_page.fil)
        logger.info("fsp_header is %s", fsp_page.fsp_header)
        for i, xdes in enumerate(fsp_page.xdess):
            if const.XDESState(xdes.state) == const.XDESState.XDES_NOT_INITED:
                continue
            for j, ps in enumerate(xdes.page_state):
                if not const.PageState.is_page_free(ps):
                    page_number = i * 64 + j
                    f.seek(page_number * const.PAGE_SIZE)
                    fil = MFil.parse_stream(f)
                    logger.info('ps:%d, page number:%d, page in used: %s',
                                ps, page_number, fil)

    return
    logger.info("xdes is %s", fsp_page.xdess[0].build())
    logger.info("xdes. page state is %s", list(fsp_page.xdess[0].page_state))
    with open(test_filename, "rb") as f:
        f.seek(36 * const.PAGE_SIZE)
        fil = MFil.parse_stream(f)
        logger.info("page 36 is %s", fil)

    with open(test_filename, "rb") as f:
        f.seek(2 * const.PAGE_SIZE)
        inode_page = MInodePage.parse_stream(f)
        for i, inode in enumerate(inode_page.inodes):
            if inode.fseg_id == 0:
                continue
            logger.info("i: %d, inode.id is %s, magic:%s", i,
                        inode.fseg_id, inode.magic_number)
            logger.info("page in used: %s", ",".join(
                map(str, [f for f in inode.fragment_array if f >= 0])))
            logger.info("xdes length: %d,%d,%d,%d",
                        inode.list_base_free.length,
                        inode.list_base_full.length,
                        inode.list_base_not_full.length,
                        inode.not_full_list_used_page)
