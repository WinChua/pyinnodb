from context import *
from pyinnodb.struct import Fil


test_fil_case = [
    (
        b"\x86$\xad\x90\x00\x00\x00\x00\x00\x018\xe4\x00\x00\x00\x01\x00\x00\x00\x00\x01*\x07\xa2\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03",
        Fil(
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
        Fil(
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
        Fil(
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
        Fil(
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
        Fil(
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
        Fil(
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


def test_fil():
    # idx = 4
    # logger.info(str(test_fil_case[idx][1]))
    # logger.info(struct_parse(Fil(), BytesIO(test_fil_case[idx][0])))
    # return
    totest(Fil, test_fil_case)
