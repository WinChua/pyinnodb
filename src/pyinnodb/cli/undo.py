from . import *

from pyinnodb.disk_struct.undo_log import *
from pyinnodb.disk_struct.fil import MFil
from pyinnodb.disk_struct.trx import *

@main.command()
@click.pass_context
@click.option("--pageno", type=click.INT, default=9)
def undo_list(ctx, pageno):
    f = ctx.obj["fn"]
    fsp_page = ctx.obj["fsp_page"]
    f.seek(pageno * const.PAGE_SIZE)
    # undo_page = MUndoPage.parse_stream(f)
    # print(undo_page)
    # print(undo_page.sizeof())
    rseg_page = MTrxSysPage.parse_stream(f)
    print(rseg_page)

@main.command()
@click.pass_context
@click.option("--pageno", type=click.INT)
@click.option("--offset", type=click.INT)
def undo_record(ctx, pageno, offset):
    f = ctx.obj['fn']
    f.seek(pageno * const.PAGE_SIZE + offset)
    print(MUndoRecordInsert.parse_stream(f))
