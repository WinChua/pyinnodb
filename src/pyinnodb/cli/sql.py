from . import *
from pyinnodb.disk_struct.fsp import MFspPage
from pyinnodb.disk_struct.index import MSDIPage
from pyinnodb.sdi.table import Table

import json

logger = logging.getLogger(__name__)


@main.command()
@click.pass_context
@click.option("--sdionly", type=click.BOOL, default=False)
def tosql(ctx, sdionly):
    f = ctx.obj["fn"]
    fsp_page = ctx.obj["fsp_page"]
    logger.debug("fsp page is %s", fsp_page.fil)
    if fsp_page.has_sdi_page == 1:
        f.seek(fsp_page.sdi_page_no * const.PAGE_SIZE)
        sdi_page = MSDIPage.parse_stream(f)
        if sdionly:
            print(json.dumps(sdi_page.ddl["dd_object"]))
            return
        table_object = Table(**sdi_page.ddl["dd_object"])

        table_name = f"`{table_object.schema_ref}`.`{table_object.name}`"
        columns_dec = []
        for c in table_object.columns:
            if (
                const.column_hidden_type.ColumnHiddenType(c.hidden)
                == const.column_hidden_type.ColumnHiddenType.HT_HIDDEN_SE
            ):
                continue
            columns_dec.append(c.gen_sql())
        idx_dec = []
        for i in table_object.indexes:
            if i.hidden:
                continue
            idx_dec.append(table_object.gen_sql_for_index(i))
        columns_dec.extend(idx_dec)
        columns_dec = "\n    " + ",\n    ".join(columns_dec) + "\n"
        table_collation = const.get_collation_by_id(table_object.collation_id)
        parts = table_object.gen_sql_for_partition()
        desc = f"ENGINE={table_object.engine} DEFAULT CHARSET={table_collation.CHARACTER_SET_NAME} COLLATE={table_collation.COLLATION_NAME}"
        comment = (
            "\nCOMMENT '" + table_object.comment + "'" if table_object.comment else ""
        )
        print(
            f"CREATE TABLE {table_name} ({columns_dec}) {desc} {'\n'+parts if parts else ''}{comment}"
        )
        return


# 'type': sql/dd/types/column.h::enum_column_type
# column_key : ag --cpp \ CK_NONE

# column_type_utf8 -> ag --cpp '::sql_type'

not_focus_col_name = ["DB_TRX_ID", "DB_ROLL_PTR", "DB_ROW_ID"]
