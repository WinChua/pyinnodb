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


def ip_context(with_dd_object, garbage):
    def ip(f):
        fil = MFil.parse_stream(f)
        if const.PageType(fil.page_type) != const.PageType.INDEX:
            return
        f.seek(-MFil.sizeof(), 1)
        index_page = MIndexPage.parse_stream(f)
        logger.debug("fil is %s, index_header is %s", fil, index_page.index_header)
        index_page.iterate_record_header(f, value_parser=with_dd_object, garbage=garbage)

    return ip


@main.command()
@click.pass_context
@click.option("--garbage/--no-garbage", default=False, help="include garbage mark data")
def iter_record(
    ctx, garbage
):
    f = ctx.obj["fn"]
    fsp_page: MFspPage = ctx.obj["fsp_page"]
    f.seek(fsp_page.sdi_page_no * const.PAGE_SIZE)
    sdi_page = MSDIPage.parse_stream(f)
    dd_object = Table(**sdi_page.ddl["dd_object"])
    fsp_page.iter_page(f, ip_context(with_dd_object(dd_object), garbage))


def with_dd_object(dd_object: Table):
    primary_col = dd_object.get_primary_key_col()
    db_default_col = dd_object.get_default_DB_col()
    pre_col_name = [c.name for c in primary_col]
    pre_col_name.extend(c.name for c in db_default_col)
    def value_parser(rh: MRecordHeader, f):
        cur = f.tell()
        logger.debug("record header is %s, offset in page %d, page no is %d", rh, cur % const.PAGE_SIZE, int(cur / const.PAGE_SIZE))

        if const.RecordType(rh.record_type) == const.RecordType.NodePointer:
            next_page_no = const.parse_mysql_int(f.read(4))
            return

        
        # data scheme version
        data_schema_version = 0
        f.seek(-MRecordHeader.sizeof(), 1)
        if rh.instant == 1:
            f.seek(-1, 1)
            data_schema_version = int.from_bytes(f.read(1))


        cols_to_parse = dd_object.get_column_schema_version(data_schema_version)
        cols_to_parse.sort(key=lambda c: c.private_data.get("physical_pos", c.ordinal_position))
        cols_to_parse.sort(key=lambda c: pre_col_name.index(c.name) if c.name in pre_col_name else len(pre_col_name))
        logger.debug("data_schema_version:%d", data_schema_version)
        logger.debug("col to parse: %s", ",".join(c.name for c in cols_to_parse))

        nullable_cols = [c for c in cols_to_parse if c.is_nullable]
        nullcol_bitmask_size = int((len(nullable_cols)+7)/8)
        may_var_col = [
            c
            for c in cols_to_parse
            if DDColumnType.is_big(c.type) or DDColumnType.is_var(c.type)
        ]

        f.seek(-nullcol_bitmask_size - rh.instant, 1)
        null_bitmask = f.read(nullcol_bitmask_size)
        null_col_data = {}
        null_mask = int.from_bytes(null_bitmask, signed=False)
        for i, c in enumerate(nullable_cols):
            if null_mask & (1 << i):
                null_col_data[c.ordinal_position] = 1

        logger.debug("null_col_data is %s", null_col_data)

        ## read var
        f.seek(-nullcol_bitmask_size, 1)
        var_size = {}
        for c in may_var_col:
            if c.ordinal_position in null_col_data:
                continue
            var_size[c.ordinal_position] = const.parse_var_size(f)


        disk_data_parsed = {}
        f.seek(cur)
        for col in cols_to_parse:
            col_value = None
            logger.debug("parse col.name[%s], version_dropped[%s]", col.name, col.private_data.get("version_dropped"))
            if col.ordinal_position in null_col_data:
                logger.debug("c.name[%s], c.ordinal_position[%d] is null", col.name, c.ordinal_position)
                col_value = None
            else:
                logger.debug("c.name[%s], c.ordinal_position[%d] parse at %d", col.name, col.ordinal_position, f.tell()%const.PAGE_SIZE)
                col_value = col.read_data(f, var_size.get(col.ordinal_position, None))
            disk_data_parsed[col.name] = col_value


        for col in dd_object.columns:
            if col.name in ["DB_ROW_ID", "DB_TRX_ID", "DB_ROLL_PTR"] or col.private_data.get("version_dropped", 0) != 0:
                if col.name in disk_data_parsed:
                    disk_data_parsed.pop(col.name)
                continue
            if col.name not in disk_data_parsed:
                disk_data_parsed[col.name] = col.get_instant_default()

        print(dd_object.DataClass(**disk_data_parsed))


    return value_parser
