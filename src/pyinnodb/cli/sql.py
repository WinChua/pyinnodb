import click
from pyinnodb.disk_struct.fsp import MFspPage
from pyinnodb.disk_struct.index import MSDIPage
from pyinnodb import const
from pyinnodb.sdi.table import Table

import json

import logging

logger = logging.getLogger(__name__)


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

        table_name = f"{table_object.schema_ref}/{table_object.name}"
        columns_dec = []
        for c in table_object.columns:
            if c.name in not_focus_col_name:
                continue
            columns_dec.append(c.gen_sql())
        idx_dec = []
        for i in table_object.indexes:
            idx_dec.append(table_object.gen_sql_for_index(i))
        columns_dec.extend(idx_dec)
        columns_dec = "\n    " + ",\n    ".join(columns_dec) + "\n"
        table_collation = const.get_collation_by_id(table_object.collation_id)
        print(f"CREATE TABLE `{table_object.schema_ref}`.`{table_object.name}` ({columns_dec}) ENGINE={table_object.engine} DEFAULT CHARSET={table_collation.CHARACTER_SET_NAME} COLLATE={table_collation.COLLATION_NAME}")
        return


    if fsp_page.has_sdi_page == 1:
        f.seek(fsp_page.sdi_page_no * const.PAGE_SIZE)
        sdi_page = MSDIPage.parse_stream(f)
        dd_object = sdi_page.ddl["dd_object"]
        if sdionly:
            print(json.dumps(dd_object))
            return
        databases = dd_object["schema_ref"]
        table_name = dd_object["name"]
        columns = []
        for c in dd_object["columns"]:
            col = f"{c['name']} {c['column_type_utf8']}"
        print(f"table name is {databases}/{table_name}")
        get_col(
            dd_object["columns"],
            "name",
            "column_key",
            "type",
            "column_type_utf8",
            "collation_id",
            "is_unsigned",
        )
        g_cols = gen_column(dd_object["columns"], 1)
        table_create = f"CREATE TABLE `{databases}`.`{table_name}` ({'\n'+',\n'.join(g_cols) + '\n'})"
        print(table_create)


# 'type': sql/dd/types/column.h::enum_column_type
# column_key : ag --cpp \ CK_NONE


def get_col(columns, *col_name):
    print(
        "\n".join(
            map(
                lambda x: "\t".join(map(lambda c: str(x.get(c, "empty")), col_name)),
                columns,
            )
        )
    )


# column_type_utf8 -> ag --cpp '::sql_type'

not_focus_col_name = ['DB_TRX_ID', 'DB_ROLL_PTR', 'DB_ROW_ID']

def gen_column(column, mysqld_version_id):
    ddl_lines = []
    for col in column:
        if col['name'] in not_focus_col_name:
            continue
        line = (f"`{col['name'].strip()}` {col['column_type_utf8']}")  # column name
        if False and col["type"] != "int":
            line += (
                f" CHARACTER SET {col['character_set']} COLLATE {col['collation']}"
            )
        if not col["is_virtual"] and col["default_option"] == "":
            # nullabel
            line += (f"{' NOT' if not col['is_nullable'] else ''} NULL")
        else:
            # 虚拟列 VIRTUAL
            line += (
                f"{' GENERATED ALWAYS AS (' + col['generation_expression'] + ') VIRTUAL' if col['is_virtual'] else '' }"
            )
        if col["default_option"] != "":
            # line += (f" DEFAULT ({col['default_option']})"
            line += (
                (
                    f" DEFAULT ({col['default_option']})"
                    if mysqld_version_id > 80012
                    else f" DEFAULT {col['default_option']}"
                )
            )
        else:
            # default
            line += (
                f"{' DEFAULT '+repr(col['default']) if col.get('have_default', None) else ''}"
            )
        # auto_increment
        line += (f"{' AUTO_INCREMENT' if col.get('is_auto_increment', None) else ''}")
        # comment
        line += (
            f"{' COMMENT '+repr(col['comment']) if col.get('comment', '') != '' else '' }"
        )
        ddl_lines.append(line)
        # COLUMN_FORMAT
        # STORAGE
        # SECONDARY_ENGINE_ATTRIBUTE
    return ddl_lines
