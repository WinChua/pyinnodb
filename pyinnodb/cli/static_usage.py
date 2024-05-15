import click

from pyinnodb.struct.inode import MInodePage
from pyinnodb.struct.fsp import MFspPage
from pyinnodb import const


@click.pass_context
def static_page_usage(ctx):
    f = ctx.obj["fn"]
    f.seek(0)
    fsp_page = MFspPage.parse_stream(f)
    print(f"page allocate in disk: {fsp_page.fsp_header.highest_page_number}")
    print(
        f"page has been init: {fsp_page.fsp_header.highest_page_number_init}")
    f.seek(2 * const.PAGE_SIZE)
    inode_page = MInodePage.parse_stream(f)
    page_usage = inode_page.iter_inode(
        func=lambda inode: (inode.fseg_id, inode.page_used(f)))
    print(f"segment count: {len(page_usage)}")
    page_cnt = 0
    for fsegid, pu in page_usage:
        for k, v in pu.items():
            if isinstance(v, list):
                page_cnt += len(v)
                print(
                    f"\tseg_id:[{fsegid}], {k} page number in use: {const.show_seq_page_list(v)}")
            elif isinstance(v, dict):
                for xdes_id, vv in v.items():
                    page_cnt += len(vv)
                    print(
                        f"\tseg_id:[{fsegid}], {k} xdes_idx:{xdes_id} page number in use: {const.show_seq_page_list(vv)}")

    print(f"{page_cnt} page in use.")
