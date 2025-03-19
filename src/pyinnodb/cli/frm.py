from pyinnodb.disk_struct.index import MIndexPage
from pyinnodb.sdi import table

from . import *


@main.command()
@click.pass_context
@click.argument("frmfile")
def frm(ctx, frmfile):
    ibd = ctx.obj["fn"]

    t = table.Table(name="HELLO")
    t.update_with_frm(frmfile)
    root_page_no = 3

    ibd.seek(root_page_no * const.PAGE_SIZE)
    root_index_page = MIndexPage.parse_stream(ibd)
    first_leaf_page = root_index_page.get_first_leaf_page(ibd, t.get_primary_key_col())

    default_value_parser = MIndexPage.default_value_parser(t, hidden_col=False)

    ibd.seek(first_leaf_page * const.PAGE_SIZE)
    index_page = MIndexPage.parse_stream(ibd)
    index_page.iterate_record_header(ibd, value_parser=default_value_parser)
