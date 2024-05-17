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
def search(ctx, primary_key, pageno, hidden_col, with_hist):
    ''' search the primary-key(int support only now) '''
    f = ctx.obj["fn"]
    fsp_page: MFspPage = ctx.obj["fsp_page"]
    f.seek(fsp_page.sdi_page_no * const.PAGE_SIZE)
    sdi_page = MSDIPage.parse_stream(f)
    dd_object = Table(**sdi_page.ddl["dd_object"])

    hidden_col = hidden_col or with_hist

    root_page_no = int(dd_object.indexes[0].private_data.get("root", 4))
    f.seek(root_page_no * const.PAGE_SIZE)
    root_index_page = MIndexPage.parse_stream(f)
    first_leaf_page = root_index_page.get_first_leaf_page(f, dd_object.get_primary_key_col())
    logger.debug("root index page: %d, first_leaf_page: %d", root_page_no, first_leaf_page)

    first_leaf_page = root_page_no

    if primary_key != "":
        primary_key = eval(primary_key)
        if isinstance(primary_key, tuple):
            primary_key = (dd_object.build_primary_key_bytes(eval(primary_key)))
        else:
            primary_key = (dd_object.build_primary_key_bytes((primary_key,)))

    value_parser = MIndexPage.default_value_parser(dd_object, hidden_col=hidden_col, transfter=lambda id: id)
    cnt = 0
    while first_leaf_page != 4294967295:
        cnt += 1
        f.seek(first_leaf_page * const.PAGE_SIZE)
        index_page = MIndexPage.parse_stream(f)
        logger.debug("page: %d", index_page.fil.offset)
        if primary_key != "":
            f.seek(first_leaf_page * const.PAGE_SIZE)
            page_dir_idx, match = index_page.binary_search_with_page_directory(primary_key, f)
            f.seek(first_leaf_page * const.PAGE_SIZE + index_page.page_directory[page_dir_idx] - 5)
            end_rh = MRecordHeader.parse_stream(f)
            logger.debug("page_dir_idx: %s, match: %s", page_dir_idx, match)
            logger.debug("end_rh is %s", end_rh)
            if match and const.RecordType(end_rh.record_type) == const.RecordType.Conventional: # the key
                logger.debug("match: %s", value_parser(end_rh, f))
                break
            elif match and const.RecordType(end_rh.record_type) == const.RecordType.NodePointer:
                logger.debug("match: %s", f.read(4))
                break
            else:
                f.seek(first_leaf_page * const.PAGE_SIZE + index_page.page_directory[page_dir_idx+1] - 5)
                start_rh = MRecordHeader.parse_stream(f)
                owned = end_rh.num_record_owned
                first_leaf_page = 4294967295 # no match if cur page is leaf then break loop
                for i in range(owned-1):
                    f.seek(start_rh.next_record_offset - 5, 1)
                    start_rh = MRecordHeader.parse_stream(f)
                    cur = f.tell()
                    if const.RecordType(start_rh.record_type) == const.RecordType.Conventional:
                        if f.read(len(primary_key)) == primary_key:
                            f.seek(-len(primary_key), 1)
                            v = value_parser(start_rh, f)
                            logger.debug("found rh: %s, value: %s", start_rh, v)
                            print("found: ", v)
                            rptr = v.DB_ROLL_PTR
                            if with_hist:
                                primary_key_col = dd_object.get_primary_key_col()
                                disk_data_layout = dd_object.get_disk_data_layout()
                                undo_map = {1: open("datadir/undo_001", "rb"), 2: open("datadir/undo_002", "rb")}
                                history = []
                                while rptr is not None:
                                    hist, rptr = rptr.last_version(
                                            undo_map, primary_key_col, disk_data_layout,
                                    )
                                    history.append(hist)
                                for h in history:
                                    print(h)
                    elif const.RecordType(start_rh.record_type) == const.RecordType.NodePointer:
                        record_key = f.read(len(primary_key))
                        logger.debug("rh: %s, record_key: %s", start_rh, record_key)
                        if record_key <= primary_key:
                            page_num = f.read(4)
                            first_leaf_page = int.from_bytes(page_num, "big")
                            break
                    f.seek(cur)




@main.command()
@click.pass_context
@click.option("--garbage/--no-garbage", default=False, help="include garbage mark data")
@click.option("--hidden-col/--no-hidden-col", type=click.BOOL, default=False, help="show the DB_ROLL_PTR and DB_TRX_ID")
@click.option("--pageno", default=None, type=click.INT, help="iterate on pageno only")
def iter_record(ctx, garbage, hidden_col, pageno):
    """iterate on the leaf pages

    by default, iter_record will iterate from the first leaf page
    output every record as a namedtuple whose filed is all the column
    name of the ibd file. 

    """
    f = ctx.obj["fn"]
    fsp_page: MFspPage = ctx.obj["fsp_page"]
    f.seek(fsp_page.sdi_page_no * const.PAGE_SIZE)
    sdi_page = MSDIPage.parse_stream(f)
    dd_object = Table(**sdi_page.ddl["dd_object"])
    root_page_no = int(dd_object.indexes[0].private_data.get("root", 4))
    f.seek(root_page_no * const.PAGE_SIZE)
    root_index_page = MIndexPage.parse_stream(f)
    first_leaf_page = root_index_page.get_first_leaf_page(f, dd_object.get_primary_key_col())
    # as the first page of leaf inode may be the off-page of large column, we should not use this way
    #first_leaf_page = root_index_page.fseg_header.get_first_leaf_page(f)
    logger.debug("first_leaf_page is %s", first_leaf_page)
    if pageno is not None:
        first_leaf_page = pageno
    default_value_parser = MIndexPage.default_value_parser(dd_object, hidden_col=hidden_col)
    while first_leaf_page != 4294967295:
        f.seek(first_leaf_page * const.PAGE_SIZE)
        index_page = MIndexPage.parse_stream(f)
        index_page.iterate_record_header(f, value_parser = default_value_parser, garbage=garbage)
        first_leaf_page = index_page.fil.next_page
    

