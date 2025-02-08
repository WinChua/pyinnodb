from . import *

from pyinnodb.disk_struct.inode import MInodePage, MInodeEntry
from pyinnodb.disk_struct.index import MIndexHeader
from pyinnodb.disk_struct.fsp import MFspPage
from pyinnodb.disk_struct.fil import MFil
from pyinnodb import const
from pyinnodb import color



@main.command()
@click.pass_context
@click.option("--kind", type=click.Choice(["type", "lsn", "ratio"]), default="type")
def list_page(ctx, kind):
    """show page type of every page"""
    f = ctx.obj["fn"]
    fsp_page = ctx.obj["fsp_page"]
    lsns = []
    ratios = []
    for pn in range(fsp_page.fsp_header.highest_page_number):
        f.seek(pn * const.PAGE_SIZE)
        fil = MFil.parse_stream(f)
        if kind == "type":
            page_name = const.get_page_type_name(fil.page_type)
            print(f"{pn} {page_name}")
        elif kind == "lsn":
            if fil.lsn != 0:
                lsns.append(fil.lsn)
        elif kind == "ratio":
            if const.PageType(fil.page_type) == const.PageType.INDEX:
                index_header = MIndexHeader.parse_stream(f)
                ratios.append(index_header.heap_top_pos / const.PAGE_SIZE)
            else:
                ratios.append(-1)

    if len(lsns) > 0:
        color.heatmap_matrix_width_high(lsns, 64, int((len(lsns) / 64) + 1), "Page NO.")
    if len(ratios) > 0:
        color.ratio_matrix_width_high(
            ratios, 64, int((len(ratios) / 64) + 1), "Page NO."
        )


@main.command()
@click.pass_context
def static_page_usage(ctx):
    """show the page usage of every inode"""
    f = ctx.obj["fn"]
    f.seek(0)
    fsp_page = MFspPage.parse_stream(f)
    print(f"page allocate in disk: {fsp_page.fsp_header.highest_page_number}")
    print(f"page has been init: {fsp_page.fsp_header.highest_page_number_init}")
    f.seek(2 * const.PAGE_SIZE)
    inode_page = MInodePage.parse_stream(f)

    def iter_func(inode: MInodeEntry):
        return inode.fseg_id, inode.page_used(f), inode.first_page()

    page_usage = inode_page.iter_inode(
        func=iter_func,
    )
    print(f"segment count: {len(page_usage)}")
    page_cnt = 0
    for fsegid, pu, fp in page_usage:
        pt = None
        if fp is not None:
            f.seek(fp * const.PAGE_SIZE)
            fp_fil = MFil.parse_stream(f)
            pt = fp_fil.page_type
            print(f"\tseg_id:[{fsegid}], {const.get_page_type_name(pt)}")
        for k, v in pu.items():
            if isinstance(v, list):
                page_cnt += len(v)
                print(
                    f"\tseg_id:[{fsegid}], {k} page number in use: {const.show_seq_page_list(v)}"
                )
            elif isinstance(v, dict):
                for xdes_id, vv in v.items():
                    page_cnt += len(vv)
                    print(
                        f"\tseg_id:[{fsegid}], {k} xdes_idx:{xdes_id} page number in use: {const.show_seq_page_list(vv)}{const.get_page_type_name(pt) if pt else ''}"
                    )

    print(f"{page_cnt} page in use.")
