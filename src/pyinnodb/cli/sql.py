import click
from pyinnodb.disk_struct.fsp import MFspPage
from pyinnodb.disk_struct.index import MSDIPage
from pyinnodb import const
import json

import logging

logger = logging.getLogger(__name__)


@click.pass_context
def tosql(ctx):
    f = ctx.obj["fn"]
    fsp_page = ctx.obj["fsp_page"]
    logger.debug("fsp page is %s", fsp_page.fil)
    if fsp_page.has_sdi_page == 1:
        print(f"ibd file contains sdi information at page {fsp_page.sdi_page_no}")
        f.seek(fsp_page.sdi_page_no * const.PAGE_SIZE)
        sdi_page = MSDIPage.parse_stream(f)
        dd_object = sdi_page.ddl["dd_object"]
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


def gen_column(column, mysqld_version_id):
    ddl_lines = []
    for col in column:
        ddl_lines.append(f"`{col['name']}` {col['column_type_utf8']}")  # column name
        if False and col["type"] != "int":
            ddl_lines.append(
                f" CHARACTER SET {col['character_set']} COLLATE {col['collation']}"
            )
        if not col["is_virtual"] and col["default_option"] == "":
            # nullabel
            ddl_lines.append(f"{' NOT' if not col['is_nullable'] else ''} NULL")
        else:
            # 虚拟列 VIRTUAL
            ddl_lines.append(
                f"{' GENERATED ALWAYS AS (' + col['generation_expression'] + ') VIRTUAL' if col['is_virtual'] else '' }"
            )
        if col["default_option"] != "":
            # ddl_lines.append(f" DEFAULT ({col['default_option']})"
            ddl_lines.append(
                (
                    f" DEFAULT ({col['default_option']})"
                    if mysqld_version_id > 80012
                    else f" DEFAULT {col['default_option']}"
                )
            )
        else:
            # default
            ddl_lines.append(
                f"{' DEFAULT '+repr(col['default']) if col['have_default'] else ''}"
            )
        # auto_increment
        ddl_lines.append(f"{' AUTO_INCREMENT' if col['is_auto_increment'] else ''}")
        # comment
        ddl_lines.append(
            f"{' COMMENT '+repr(col['comment']) if col['comment'] != '' else '' }"
        )
        # COLUMN_FORMAT
        # STORAGE
        # SECONDARY_ENGINE_ATTRIBUTE
    return "\n".join(ddl_lines)
