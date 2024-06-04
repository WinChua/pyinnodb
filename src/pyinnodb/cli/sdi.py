from . import *

from pyinnodb.disk_struct.index import MIndexHeader, MSDIPage, MSystemRecord
from pyinnodb.disk_struct.index import MFsegHeader, MIndexSystemRecord, MDDL
from pyinnodb.disk_struct.fil import MFil
from pyinnodb.disk_struct.record import MRecordHeader
import zlib

@main.command()
@click.pass_context
@click.option("--pageno", type=click.INT)
def sdi(ctx, pageno):
    f = ctx.obj["fn"]
    fsp_page = ctx.obj["fsp_page"]
    if fsp_page.sdi_version == 0:
        print("there is no SDI info in this file")
        return

    cur_page_num = fsp_page.sdi_page_no


    while True:
        f.seek(const.PAGE_SIZE * cur_page_num)
        fil = MFil.parse_stream(f)
        index_header = MIndexHeader.parse_stream(f)
        if index_header.page_level == 0:
            break
        fseg_header = MFsegHeader.parse_stream(f)
        infimum = MSystemRecord.parse_stream(f)
        f.seek(-8+infimum.next_record_offset+12, 1)
        cur_page_num = int.from_bytes(f.read(4), byteorder="big")

    print(cur_page_num)

    while cur_page_num != 4294967295:
        f.seek(cur_page_num * const.PAGE_SIZE)
        sdi_page = MSDIPage.parse_stream(f)
        f.seek(cur_page_num * const.PAGE_SIZE + sdi_page.system_records.infimum.get_current_offset())

        next_offset = sdi_page.system_records.infimum.next_record_offset
        while next_offset != 0:
            f.seek(next_offset-MRecordHeader.sizeof(), 1)
            rh = MRecordHeader.parse_stream(f)
            if const.RecordType(rh.record_type) == const.RecordType.Supremum:
                break
            next_offset = rh.next_record_offset
            cur = f.tell()
            ddl = MDDL.parse_stream(f)
            print(rh, ddl)
            data = f.read(ddl.zip_len)
            print(zlib.decompress(data))
            f.seek(cur)
        cur_page_num = sdi_page.fil.next_page
