import json

from pyinnodb.disk_struct.index import MIndexPage, MSDIPage
from pyinnodb.sdi.table import Table

from . import *

logger = logging.getLogger(__name__)


@main.command()
@click.pass_context
@click.option("--mode", type=click.Choice(["sdi", "ddl", "dump"]), default="ddl")
@click.option("--sdi-idx", type=click.INT, default=0)
@click.option("--schema/--no-schema", default=True)
def tosql(ctx, mode, sdi_idx, schema):
    """dump the ddl/dml/sdi of the ibd table

    ddl) output the create table ddl;
    dump) output the dml of ibd file;
    sdi) output the dd_object stored in the SDIPage as json format
    """

    f = ctx.obj["fn"]
    fsp_page = ctx.obj["fsp_page"]
    logger.debug("fsp header is %s", fsp_page.fsp_header)
    logger.debug("fsp page is %s", fsp_page.fil)
    if fsp_page.sdi_version == 1:
        f.seek(fsp_page.sdi_page_no * const.PAGE_SIZE)
        sdi_page = MSDIPage.parse_stream(f)
        dd_obj = sdi_page.ddl(f, sdi_idx)["dd_object"]
        table_object = Table(**dd_obj)

        if mode == "sdi":
            print(json.dumps(dd_obj))
        elif mode == "ddl":
            print(table_object.gen_ddl(schema))
        else:
            dump_ibd(table_object, f)
        return

def dump_ibd(table_object, f, oneline=True):
    root_page_no = int(table_object.indexes[0].private_data.get("root", 4))
    f.seek(root_page_no * const.PAGE_SIZE)
    root_index_page = MIndexPage.parse_stream(f)
    first_leaf_page_no = root_index_page.get_first_leaf_page(
        f, table_object.get_primary_key_col()
    )
    if first_leaf_page_no is None:
        print("no data")
        return

    default_value_parser = MIndexPage.default_value_parser(
        table_object, table_object.transfer
    )

    values = []
    while first_leaf_page_no != const.FFFFFFFF:
        f.seek(first_leaf_page_no * const.PAGE_SIZE)
        index_page = MIndexPage.parse_stream(f)
        values.extend(
            index_page.iterate_record_header(
                f, value_parser=default_value_parser
            )
        )
        first_leaf_page_no = index_page.fil.next_page

    values = [f"({','.join(v)})" for v in values]

    table_name = f"`{table_object.schema_ref}`.`{table_object.name}`"
    if not oneline:
        print(
            f"INSERT INTO {table_name}({','.join( table_object.keys() )}) values {', '.join(values)}"
        )
    else:
        for v in values:
            print(f"INSERT INTO {table_name}({','.join(table_object.keys())}) values {v};")
        

    return
# 
# 'type': sql/dd/types/column.h::enum_column_type
# column_key : ag --cpp \ CK_NONE

# column_type_utf8 -> ag --cpp '::sql_type'

not_focus_col_name = ["DB_TRX_ID", "DB_ROLL_PTR", "DB_ROW_ID"]
