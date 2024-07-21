from . import *
import dataclasses

from pyinnodb.frm import frm as mfrm
from pyinnodb.sdi import table
from pyinnodb.disk_struct.index import MIndexPage

@main.command()
@click.pass_context
@click.argument("frmfile")
def frm(ctx, frmfile):
    ibd = ctx.obj["fn"]
    f = open(frmfile, "rb")
    frm_header = mfrm.MFrm.parse_stream(f)

    cols = []
    for i, col in enumerate(frm_header.cols):
        cols.append(col.to_dd_column(col.name, i, frm_header.column_labels))


    #print(frm_header.keys[0][0].key_parts)
    keys, key_name, key_comment = frm_header.keys[0]

    idx = keys.to_dd_index(key_name.decode(), frm_header.cols)

    cols.append(table.get_sys_col("DB_TRX_ID", len(cols)))
    cols.append(table.get_sys_col("DB_ROLL_PTR", len(cols)))

    t = table.Table(name="HELLO")
    t.columns = cols
    t.indexes.append(idx)
    # print(idx.elements)
    # print(idx.get_effect_element())
    # print(keys.key_parts)
    # return
    root_page_no = 3

    ibd.seek(root_page_no * const.PAGE_SIZE)
    root_index_page = MIndexPage.parse_stream(ibd)
    first_leaf_page = root_index_page.get_first_leaf_page(ibd, t.get_primary_key_col())

    default_value_parser = MIndexPage.default_value_parser(t, hidden_col=False)

    ibd.seek(first_leaf_page * const.PAGE_SIZE)
    index_page = MIndexPage.parse_stream(ibd)
    index_page.iterate_record_header(ibd, value_parser=default_value_parser)
