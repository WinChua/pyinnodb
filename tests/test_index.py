from context import *

from pyinnodb.struct.index import *


def test_index_page_iter():
    with open(test_filename, "rb") as f:
        f.seek(4 * const.PAGE_SIZE)
        index_page = MIndexPage.parse_stream(f)
        index_page.iterate_record_header(f)


def test_index_page_header():
    with open(test_filename, "rb") as f:
        f.seek(4 * const.PAGE_SIZE)
        index_page = MIndexPage.parse_stream(f)
        logger.info("record_format: %s", index_page.index_header.record_format)
        logger.info("record num is %d",
                    index_page.index_header.heap_records_number)
        logger.info("header is %s", index_page.index_header)


def test_page_level():
    with open(test_filename, "rb") as f:
        f.seek(4 * const.PAGE_SIZE)
        index_page_4 = MIndexPage.parse_stream(f)
        logger.info("page_level is %s", index_page_4.index_header.page_level)
        all_fil_page = [index_page_4.fil]
        logger.info("index_page info is %s", index_page_4.fil)
        logger.info("page type is %s",
                    const.get_page_type_name(index_page_4.fil.page_type))
        next_page = index_page_4.fil.next_page
        while next_page != 4294967295:
            logger.info("next_page is %d", next_page)
            f.seek(next_page * const.PAGE_SIZE)
            fil = MFil.parse_stream(f)
            all_fil_page.append(fil)
            next_page = fil.next_page

        index_page_4.system_records.infimum.get_current_offset()

        logger.info("len(all_fil_page) is %d", len(all_fil_page))
        return
        f.seek(34 * const.PAGE_SIZE)
        index_page_5 = MIndexPage.parse_stream(f)
        logger.info("fseg header 5 is %s", index_page_5.fseg_header)
