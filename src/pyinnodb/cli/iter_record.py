from . import *
from pyinnodb.disk_struct.index import MIndexPage, MSDIPage
from pyinnodb.disk_struct.fsp import MFspPage
from pyinnodb.disk_struct.fil import MFil
from pyinnodb.disk_struct.record import MRecordHeader
from pyinnodb.disk_struct.inode import MInodePage
from pyinnodb.sdi.table import Column, Table
from pyinnodb.const.dd_column_type import DDColumnType
from pyinnodb.disk_struct.first_page import MFirstPage, MIndexEntryNode


@main.command()
@click.pass_context
@click.option("--pageno", type=click.INT, default=5)
def list_first_page(ctx, pageno):
    f = ctx.obj["fn"]
    fsp_page = ctx.obj["fsp_page"]
    f.seek(pageno * const.PAGE_SIZE)
    first_page = MFirstPage.parse_stream(f)
    import json

    print(json.dumps(dataclasses.asdict(first_page)))
    first_entry = first_page.index_list.idx(0, f)
    f.seek(first_entry.seek_loc())
    # print(MIndexEntryNode.parse_stream(f))


@main.command()
@click.pass_context
@click.option("--primary-key", type=click.STRING, default="")
@click.option("--hidden-col/--no-hidden-col", type=click.BOOL, default=False)
@click.option("--pageno", default=None, type=click.INT)
@click.option("--with-hist/--no-with-hist", type=click.BOOL, default=False)
@click.option("--datadir", type=click.Path(exists=False), default=None)
def search(ctx, primary_key, pageno, hidden_col, with_hist, datadir):
    """search the primary-key(int support only now)"""
    f = ctx.obj["fn"]
    # print("search start cost:", time.time() - ctx.obj["start_time"])
    fsp_page: MFspPage = ctx.obj["fsp_page"]
    f.seek(fsp_page.sdi_page_no * const.PAGE_SIZE)
    sdi_page = MSDIPage.parse_stream(f)
    dd_object = Table(**sdi_page.ddl(f, 0)["dd_object"])

    hidden_col = hidden_col or with_hist

    if primary_key != "":
        primary_key = eval(primary_key)
    else:
        print("must specify the --primary-key like --primary-key 10")
        return

    result = dd_object.search(f, primary_key, hidden_col)
    if result is None:
        print("not found")
        return

    print(result)

    if not with_hist:
        return

    if datadir is None:
        print("--datadir should be specified to view the history")
        return
    if not os.path.exists(datadir):
        print(f"--datadir {datadir} not exists")
        return

    rptr = result.DB_ROLL_PTR
    primary_key_col = dd_object.get_primary_key_col()
    disk_data_layout = dd_object.get_disk_data_layout()
    undo_map = const.util.get_undo_tablespacefile(f"{datadir}/mysql.ibd")
    history = []
    while rptr is not None:
        hist, rptr = rptr.last_version(
            undo_map,
            primary_key_col,
            disk_data_layout,
        )
        history.append(hist)
    for h in history:
        print(h)

    return


def primary_key_only(key_len: int):
    def value_parser(rh: MRecordHeader, f):
        print(rh, f.read(key_len))

    return value_parser


@main.command()
@click.pass_context
@click.option("--garbage/--no-garbage", default=False, help="include garbage mark data")
@click.option(
    "--hidden-col/--no-hidden-col",
    type=click.BOOL,
    default=False,
    help="show the DB_ROLL_PTR and DB_TRX_ID",
)
@click.option("--pageno", default=None, type=click.INT, help="iterate on pageno only")
# @click.option("--primary-key-len", type=click.INT, help="primary key only if not 0", default=0)
@click.option("--sdi-idx", type=click.INT, default=0, help="idx of sdi")
def iter_record(ctx, garbage, hidden_col, pageno, sdi_idx):
    """iterate on the leaf pages

    by default, iter_record will iterate from the first leaf page
    output every record as a namedtuple whose filed is all the column
    name of the ibd file.

    """
    f = ctx.obj["fn"]
    fsp_page: MFspPage = ctx.obj["fsp_page"]
    f.seek(fsp_page.sdi_page_no * const.PAGE_SIZE)
    sdi_page = MSDIPage.parse_stream(f)
    dd_object = Table(**sdi_page.ddl(f, sdi_idx)["dd_object"])

    dd_object.iter_record(f, hidden_col=hidden_col, garbage=garbage)

    return
