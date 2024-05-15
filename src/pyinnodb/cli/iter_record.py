from . import *
from pyinnodb.disk_struct.index import MIndexPage, MSDIPage
from pyinnodb.disk_struct.fsp import MFspPage
from pyinnodb.disk_struct.fil import MFil
from pyinnodb.disk_struct.record import MRecordHeader
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


def ip_context(with_dd_object):
    def ip(f):
        fil = MFil.parse_stream(f)
        if const.PageType(fil.page_type) != const.PageType.INDEX:
            return
        f.seek(-MFil.sizeof(), 1)
        index_page = MIndexPage.parse_stream(f)
        index_page.iterate_record_header(f, value_parser=with_dd_object)

    return ip


@main.command()
@click.pass_context
@click.option("--delete/--no-delete", default=False, help="include delete mark data")
def iter_record(
    ctx, delete
):
    f = ctx.obj["fn"]
    fsp_page: MFspPage = ctx.obj["fsp_page"]
    f.seek(fsp_page.sdi_page_no * const.PAGE_SIZE)
    sdi_page = MSDIPage.parse_stream(f)
    dd_object = Table(**sdi_page.ddl["dd_object"])
    fsp_page.iter_page(f, ip_context(with_dd_object(dd_object, delete)))


def with_dd_object(dd_object: Table, delete):
    primary_col = dd_object.get_primary_key_col()
    db_hidden_col = dd_object.get_default_DB_col()
    exclude_col = [c.name for c in primary_col]
    exclude_col.extend([c.name for c in db_hidden_col])
    secondary_col = dd_object.get_column(lambda c: c.name not in exclude_col)
    nullcol_bitmask_size, nullable_cols = dd_object.nullcol_bitmask_size
    may_var_col = dd_object.var_col

    def value_parser(rh: MRecordHeader, f):
        cur = f.tell()
        logger.debug("record header is %s, offset in page %d", rh, cur % const.PAGE_SIZE)
        #print(rh, cur % const.PAGE_SIZE)

        if const.RecordType(rh.record_type) == const.RecordType.NodePointer:
            next_page_no = const.parse_mysql_int(f.read(4))
            # next_page_no = int.from_bytes(f.read(4), "big")
            #print("it's a node pointer, next page is", next_page_no)
            return

        if not delete and rh.deleted:
            return

        ## read null
        # if rh.no_use_1 == 0:
        #     f.seek(-MRecordHeader.sizeof() - nullcol_bitmask_size, 1)
        # else:
        #     f.seek(-MRecordHeader.sizeof() - nullcol_bitmask_size - 1, 1)
        f.seek(-MRecordHeader.sizeof() - nullcol_bitmask_size - rh.no_use_1, 1)
        null_bitmask = f.read(nullcol_bitmask_size)
        instant_col_count_in_rec = 0
        if rh.no_use_1 == 1:
            instant_col_count_in_rec = int.from_bytes(f.read(1)) # TODO: figure what this byte do
        null_mask = int.from_bytes(null_bitmask, signed=False)
        null_col_data = {}
        for i, c in enumerate(nullable_cols):
            if null_mask & (1 << i):
                null_col_data[c.ordinal_position] = 1

        logger.debug("null_bitmask data is %s, instant byte: %s", null_bitmask, instant_col_count_in_rec)
        logger.debug("null_col_data is %s, %s", null_col_data, ",".join(c.name for c in nullable_cols))

        ## read var
        f.seek(-nullcol_bitmask_size - rh.no_use_1, 1)
        var_size = {}
        for c in may_var_col:
            if c.ordinal_position in null_col_data:
                continue
            if rh.no_use_1 == 0 and c.is_instant_col: # for the record no insert before instant col
                continue
            var_size[c.ordinal_position] = const.parse_var_size(f)
            logger.debug("read var size %s for col %s", var_size[c.ordinal_position], c.name)


        logger.debug("var size is %s", var_size)

        kv = {}
        f.seek(cur)
        for col in primary_col:
            kv[col.name] = col.read_data(f)

        for col in db_hidden_col:
            data = col.read_data(f)
            continue
            #print(col.name, col.is_unsigned, data)

        instant_col_deal = 0
        for col in secondary_col:
            if col.is_instant_col:
                instant_col_deal += 1
            if col.ordinal_position in null_col_data:
                kv[col.name] = None
                #print(col.name, col.type, "NULL")
                continue
            if rh.no_use_1 == 0 and col.is_instant_col:
                kv[col.name] = col.get_instant_default() # try to get the default value for instant col
                continue
            if instant_col_deal > instant_col_count_in_rec:
                kv[col.name] = col.get_instant_default() # try to get the default value for instant col
                continue
            kv[col.name] = col.read_data(f, var_size.get(col.ordinal_position, None))
            # print(
            #     col.name,
            #     col.type,
            #     col.read_data(f, var_size.get(col.ordinal_position, None)),
            # )

        print(dd_object.DataClass(**{k:v for k, v in kv.items() if k not in ["DB_ROW_ID", "DB_TRX_ID", "DB_ROLL_PTR"]}))

    return value_parser
