from pyinnodb.disk_struct.index import MSDIPage
from pyinnodb.disk_struct.rollback import MRollbackPointer
from pyinnodb.disk_struct.trx import *
from pyinnodb.disk_struct.undo_log import *
from pyinnodb.sdi.table import Table

from . import *


@main.command()
@click.pass_context
@click.option("--datadir", type=click.Path(exists=True))
def undo_tablespaces(ctx, datadir):
    pass


@main.command()
@click.pass_context
@click.option("--pageno", type=click.INT, default=9)
def undo_list(ctx, pageno):
    """dump the undo page"""
    f = ctx.obj["fn"]
    fsp_page = ctx.obj["fsp_page"]
    f.seek(pageno * const.PAGE_SIZE)
    undo_page = MUndoPage.parse_stream(f)
    print(undo_page)
    print(undo_page.sizeof())


@main.command()
@click.pass_context
@click.option("--pageno", type=click.INT, help="page number the pointer stored at")
@click.option("--offset", type=click.INT, help="page offset the pointer stored at")
@click.option(
    "--insert", type=click.INT, help="insert flag: 1: pointer is an insert undo type"
)
@click.option("--rsegid", type=click.INT, help="undo tablespace id of the pointer")
def undo_record(ctx, pageno, offset, insert, rsegid):
    """show the history version of an RollbackPointer"""
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
    history = []
    while rptr is not None:
        hist, rptr = rptr.last_version(
            undo_map, dd_object.get_primary_key_col(), dd_object.get_disk_data_layout()
        )
        history.append(hist)
    for h in history:
        print(h)
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
    """show the RSEGArrayPage"""
    f = ctx.obj["fn"]
    f.seek(pageno * const.PAGE_SIZE)
    page = MRSEGArrayPage.parse_stream(f)
    print(page)
    for pageno in page.header.pagenos:
        f.seek(pageno * const.PAGE_SIZE)
        rseg_page = MRSEGPage.parse_stream(f)
        pages = [f for f in rseg_page.slots if f != const.FFFFFFFF]
        if 150 in pages:
            print(rseg_page, pageno)
