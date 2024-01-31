from context import *
from pyinnodb.struct import *
from pyinnodb.const import get_page_type_name
from pyinnodb import const

from pyinnodb.struct.index import *
from pyinnodb.struct.fil import *


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
        first_page = MFspPage.parse_stream(f)
        logger.info("sdi page is %d", first_page.sdi_page_no)
        f.seek(first_page.sdi_page_no * const.PAGE_SIZE)
        sdi_page = MSDIPage.parse_stream(f)
        logger.info("sdi_page ddl is %s", sdi_page.ddl)
        logger.info("sdi_page.ddl type is %s", type(sdi_page.ddl))
        logger.info("schema_ref: %s", sdi_page.ddl['dd_object']['schema_ref'])
        logger.info("key is %s", sdi_page.ddl['dd_object'].keys())
        logger.info("table name is %s", sdi_page.ddl['dd_object']['name'])
        for col in sdi_page.ddl['dd_object']['columns']:
            logger.info("column name is %s, collation id is %d, type is %s",
                        col['name'], col['collation_id'], col['type'])
            logger.info("element is %s", col["elements"])
            # DB_ROLL_PTR, DB_TRX_ID, DB_ROW_ID
            logger.info("ct is %s", col["column_type_utf8"])


test_index_page_data = b"2\x17X&\x00\x00\x00\x04\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x01*,\xa2E\xbf\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03\x00\x02\x00\xcf\x80\x05\x00\x00\x00\x00\x00\xb9\x00\x05\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x9c\x00\x00\x00\x03\x00\x00\x00\x02\x02r\x00\x00\x00\x03\x00\x00\x00\x02\x01\xb2\x01\x00\x02\x00\x1cinfimum\x00\x04\x00\x0b\x00\x00supremum"


def test_minode_page():
    with open(test_filename2, "rb") as f:
        f.seek(2 * const.PAGE_SIZE)
        inode_page = MInodePage.parse_stream(f)
        logger.info(inode_page)


def test_mindex_page():
    with open(test_filename, "rb") as f:
        f.seek(4 * const.PAGE_SIZE)
        index_page = MIndexPage.parse_stream(f)
        logger.info(index_page.system_records.infimum.order)
        logger.info(index_page.system_records.supremum.order)
        logger.info(index_page.system_records.infimum.next_record_offset)
        logger.info(len(index_page.page_directory))
        logger.info(index_page.index_header.dir_slot_number)
        logger.info(index_page.index_header.record_number)
        logger.info(index_page.index_header.page_dir_insert_number)
        logger.info("index page level is %d",
                    index_page.index_header.page_level)


def test_page_directory():
    with open(test_filename, "rb") as f:
        f.seek(4 * const.PAGE_SIZE)
        page4 = f.read(const.PAGE_SIZE)
        index_page = MIndexPage.parse(page4)
        logger.info("the page_directory is %s", index_page.page_directory)
        logger.info("sizeof is %d", index_page.fil.sizeof() + index_page.index_header.sizeof() +
                    index_page.fseg_header.sizeof())
        logger.info("infimum is %s", index_page.system_records.infimum)
        for pd in index_page.page_directory:
            f.seek(4*const.PAGE_SIZE + pd - 5)
            rh = MRecordHeader.parse_stream(f)
            logger.info("the pd is %d, rh is %s, key is %s", pd, rh, f.read(4))


def test_iter_mindex_page():
    with open(test_filename, "rb") as f:
        f.seek(4 * const.PAGE_SIZE)
        page4 = f.read(const.PAGE_SIZE)
        index_page = MIndexPage.parse(page4)
        logger.info("the record number this page contains: %d",
                    index_page.index_header.record_number)
        logger.info(index_page.index_header)
        next_offset = index_page.system_records.infimum.next_record_offset
        f.seek(4 * const.PAGE_SIZE +
               index_page.fil.sizeof() +
               index_page.index_header.sizeof() +
               index_page.fseg_header.sizeof() +
               index_page.system_records.infimum.sizeof() -
               8  # the size of the last field of infimum => marker
               )
        while next_offset != 0:
            f.seek(next_offset-MRecordHeader.sizeof(), 1)
            rh = MRecordHeader.parse_stream(f)
            logger.info("next_offset is %d, rh is %s",
                        next_offset, rh)
            next_offset = rh.next_record_offset


def test_binary_search():
    target_key = const.encode_mysql_int(92, 4)
    logger.info("search for value %d", const.parse_mysql_int(target_key))
    logger.info("200 is %s", const.encode_mysql_int(92, 4))

    with open(test_filename, "rb") as f:
        f.seek(4 * const.PAGE_SIZE)
        page4 = f.read(const.PAGE_SIZE)
        index_page = MIndexPage.parse(page4)
        f.seek(4 * const.PAGE_SIZE)
        r, found = index_page.binary_search_with_page_directory(target_key, f)
        logger.info((r, found))
