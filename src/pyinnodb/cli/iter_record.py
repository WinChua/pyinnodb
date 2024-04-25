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
def iter_record(
    ctx,
):
    f = ctx.obj["fn"]
    fsp_page: MFspPage = ctx.obj["fsp_page"]
    f.seek(fsp_page.sdi_page_no * const.PAGE_SIZE)
    sdi_page = MSDIPage.parse_stream(f)
    dd_object = Table(**sdi_page.ddl["dd_object"])
    fsp_page.iter_page(f, ip_context(with_dd_object(dd_object)))


def with_dd_object(dd_object: Table):
    primary_col = dd_object.get_primary_key_col()
    db_hidden_col = dd_object.get_default_DB_col()
    exclude_col = [c.name for c in primary_col]
    exclude_col.extend([c.name for c in db_hidden_col])
    secondary_col = dd_object.get_column(lambda c: c.name not in exclude_col)
    nullcol_bitmask_size, nullable_cols = dd_object.nullcol_bitmask_size
    may_var_col = dd_object.var_col

    def value_parser(rh: MRecordHeader, f):
        cur = f.tell()
        #print(rh, cur % const.PAGE_SIZE)

        if const.RecordType(rh.record_type) == const.RecordType.NodePointer:
            next_page_no = const.parse_mysql_int(f.read(4))
            # next_page_no = int.from_bytes(f.read(4), "big")
            #print("it's a node pointer, next page is", next_page_no)
            return

        ## read null
        f.seek(-MRecordHeader.sizeof() - nullcol_bitmask_size, 1)
        null_bitmask = f.read(nullcol_bitmask_size)
        null_mask = int.from_bytes(null_bitmask, signed=False)
        null_col_data = {}
        for i, c in enumerate(nullable_cols):
            if null_mask & (1 << i):
                null_col_data[c.ordinal_position] = 1

        ## read var
        f.seek(-nullcol_bitmask_size, 1)
        var_size = {}
        for c in may_var_col:
            if c.ordinal_position in null_col_data:
                continue
            var_size[c.ordinal_position] = const.parse_var_size(f)

        kv = {}
        f.seek(cur)
        for col in primary_col:
            kv[col.name] = col.read_data(f)

        for col in db_hidden_col:
            data = col.read_data(f)
            continue
            #print(col.name, col.is_unsigned, data)

        for col in secondary_col:
            if col.ordinal_position in null_col_data:
                kv[col.name] = None
                #print(col.name, col.type, "NULL")
                continue
            kv[col.name] = col.read_data(f, var_size.get(col.ordinal_position, None))
            # print(
            #     col.name,
            #     col.type,
            #     col.read_data(f, var_size.get(col.ordinal_position, None)),
            # )

        print(dd_object.DataClass(**kv))

    return value_parser
