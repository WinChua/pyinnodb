from . import *

from pyinnodb.disk_struct.undo_log import *
from pyinnodb.disk_struct.fil import MFil

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
