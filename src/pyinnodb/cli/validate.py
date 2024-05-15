from . import *
from pyinnodb.const import page_checksum_crc32c
from pyinnodb.disk_struct.fil import MFil
from pyinnodb.disk_struct.fsp import MFspHeader


@main.command()
@click.pass_context
def validate(ctx):
    f = ctx.obj["fn"]
    fsp_page = ctx.obj["fsp_page"]
    f.seek(0)
    first_page = f.read(const.PAGE_SIZE)
    cal_checksum = const.page_checksum_crc32c(first_page)
    first_page = BytesIO(first_page)
    first_fil = MFil.parse_stream(first_page)
    fsp_header = MFspHeader.parse_stream(first_page)
    print(first_fil.checksum, cal_checksum)
    for page_number in range(1, fsp_header.highest_page_number):
        f.seek(const.PAGE_SIZE * page_number)
        page_data = f.read(const.PAGE_SIZE)
        fil = MFil.parse(page_data)
        checksum = const.page_checksum_crc32c(page_data)
        if fil.page_type == const.FIL_PAGE_TYPE_ALLOCATED:
            print(f"page[{page_number}] is allocated, no need to calculate checksum")
        else:
            print(
                f"page[{page_number}], fil.checksum[{hex(fil.checksum)}], calculate checksum[{hex(checksum)}]"
            )
