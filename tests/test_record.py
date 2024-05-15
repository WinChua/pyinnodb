from context import *
from pyinnodb.disk_struct.record import MRecordHeader
from pyinnodb.disk_struct.index import MIndexPage


def test_record():
    with open(test_filename, "rb") as f:
        f.seek(4 * const.PAGE_SIZE)
        page4 = f.read(const.PAGE_SIZE)
        index_page = MIndexPage.parse(page4)
        next_offset = index_page.system_records.infimum.next_record_offset
        f.seek(
            4 * const.PAGE_SIZE
            + index_page.fil.sizeof()
            + index_page.index_header.sizeof()
            + index_page.fseg_header.sizeof()
            + index_page.system_records.infimum.sizeof()
            - 8  # the size of the last field of infimum => marker
        )
        while next_offset != 0:
            f.seek(next_offset - MRecordHeader.sizeof(), 1)
            cur = f.seek(0, 1) + 5
            rh = MRecordHeader.parse_stream(f)
            if rh.min_record != 0:
                logger.info(
                    "cur is %d, record header is %s",
                    cur % const.PAGE_SIZE,
                    rh.num_record_owned,
                )
                logger.info(
                    "min_rec is %d, type is %s",
                    rh.min_record,
                    const.RecordType(rh.record_type),
                )
                logger.info("order is %s", rh.order)
            next_offset = rh.next_record_offset
        logger.info("page_directory is %s", list(index_page.page_directory))
