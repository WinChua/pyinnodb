from . import *

from pyinnodb.disk_struct.undo_log import *
from pyinnodb.disk_struct.fil import MFil
from pyinnodb.disk_struct.trx import *
from pyinnodb.disk_struct.rollback import MRollbackPointer
from pyinnodb.disk_struct.index import MSDIPage
from pyinnodb.sdi.table import Column, Table


@main.command()
@click.pass_context
@click.option("--pageno", type=click.INT, default=9)
def undo_list(ctx, pageno):
    f = ctx.obj["fn"]
    fsp_page = ctx.obj["fsp_page"]
    f.seek(pageno * const.PAGE_SIZE)
    undo_page = MUndoPage.parse_stream(f)
    print(undo_page)
    print(undo_page.sizeof())


@main.command()
@click.pass_context
@click.option("--pageno", type=click.INT)
@click.option("--offset", type=click.INT)
@click.option("--insert", type=click.INT)
@click.option("--rsegid", type=click.INT)
def undo_record(ctx, pageno, offset, insert, rsegid):
    f = ctx.obj["fn"]
    fsp_page: MFspPage = ctx.obj["fsp_page"]
    f.seek(fsp_page.sdi_page_no * const.PAGE_SIZE)
    sdi_page = MSDIPage.parse_stream(f)
    dd_object = Table(**sdi_page.ddl["dd_object"])
    undo_001 = open("datadir/undo_001", "rb")
    undo_002 = open("datadir/undo_002", "rb")
    undo_map = {1: undo_001, 2: undo_002}
    rptr = MRollbackPointer(
        insert_flag=insert,
        rollback_seg_id=rsegid,
        page_number=pageno,
        page_offset=offset,
    )
    while rptr is not None:
        tmp = rptr
        undo_record_header, rptr = rptr.last_version(
            undo_map, dd_object.get_primary_key_col(), dd_object.get_disk_data_layout()
        )
        print(tmp, undo_record_header)
    # f.seek(pageno * const.PAGE_SIZE + offset)
    # rollptr = MUndoRecordInsert.parse_stream(f)
    # print(rollptr)
    # # trx_undo_update_rec_get_sys_cols
    # if updtype == 0: # update
    #     info_bits = f.read(1)
    #     trx_id = const.mach_u64_read_next_compressed(f)
    #     ptr = const.mach_u64_read_next_compressed(f).to_bytes(7)
    #     print(info_bits, trx_id, MRollbackPointer.parse(ptr))
    # else: # insert
    #     pass


@main.command()
@click.pass_context
@click.option("--pageno", type=click.INT)
def rseg_array(ctx, pageno):
    f = ctx.obj["fn"]
    f.seek(pageno * const.PAGE_SIZE)
    page = MRSEGArrayPage.parse_stream(f)
    print(page)
    for pageno in page.header.pagenos:
        f.seek(pageno * const.PAGE_SIZE)
        rseg_page = MRSEGPage.parse_stream(f)
        pages = [f for f in rseg_page.slots if f != 4294967295]
        if 150 in pages:
            print(rseg_page, pageno)
