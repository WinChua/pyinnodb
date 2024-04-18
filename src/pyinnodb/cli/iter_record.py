import click
from pyinnodb import const
from pyinnodb.disk_struct.index import MIndexPage, MSDIPage
from pyinnodb.disk_struct.record import MRecordHeader
from pyinnodb.sdi.table import Column, Table
from pyinnodb.const.dd_column_type import DDColumnType

@click.pass_context
@click.option("--pageno", type=click.INT, default=4)
def iter_record(ctx, pageno):
    f = ctx.obj["fn"]
    fsp_page = ctx.obj["fsp_page"]
    f.seek(fsp_page.sdi_page_no * const.PAGE_SIZE)
    sdi_page = MSDIPage.parse_stream(f)
    dd_object = Table(**sdi_page.ddl["dd_object"])
    print("null col count is", dd_object.null_col_count)
    page = f.seek(pageno * const.PAGE_SIZE)
    index_page = MIndexPage.parse_stream(f)
    index_page.iterate_record_header(f, value_parser=with_dd_object(dd_object))

def with_dd_object(dd_object: Table):
    primary_col = dd_object.get_primary_key_col()
    db_hidden_col = dd_object.get_default_DB_col()
    exclude_col = [c.name for c in primary_col]
    exclude_col.extend([c.name for c in db_hidden_col])
    secondary_col = dd_object.get_column(lambda c: c.name not in exclude_col)
    nullcol_bitmask_size, nullable_cols = dd_object.nullcol_bitmask_size
    may_var_col = dd_object.var_col
    def value_parser(rh: MRecordHeader, f):
        print(rh)
        cur = f.tell()

        ## read null
        f.seek(-MRecordHeader.sizeof()-nullcol_bitmask_size, 1)
        null_bitmask = f.read(nullcol_bitmask_size)
        null_mask = int.from_bytes(null_bitmask, signed=False)
        null_col_data = {}
        for i, c in enumerate(nullable_cols):
            if null_mask & (1<<i):
                null_col_data[c.ordinal_position] = 1

        ## read var
        f.seek(-nullcol_bitmask_size, 1)
        var_size = {}
        for c in may_var_col:
            if c.ordinal_position in null_col_data:
                continue
            var_size[c.ordinal_position] = const.parse_var_size(f)
        f.seek(cur)
        for col in primary_col:
            print(col.name, col.read_data(f))

        for col in db_hidden_col:
            print(col.name, col.is_unsigned, col.read_data(f))

        for col in secondary_col:
            if col.ordinal_position in null_col_data:
                print(col.name, col.type, "NULL")
                continue
            print(col.name, col.type, col.read_data(f, var_size.get(col.ordinal_position, None)))

    return value_parser

