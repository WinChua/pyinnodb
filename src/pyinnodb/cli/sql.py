from . import *
from pyinnodb.disk_struct.fsp import MFspPage
from pyinnodb.disk_struct.index import MSDIPage, MIndexPage
from pyinnodb.sdi.table import Table
from pyinnodb.disk_struct.data import MGeo

import json

from datetime import timedelta, datetime, date

logger = logging.getLogger(__name__)


@main.command()
@click.pass_context
@click.option("--mode", type=click.Choice(["sdi", "ddl", "dump"]), default="ddl")
@click.option("--sdi-idx", type=click.INT, default=0)
def tosql(ctx, mode, sdi_idx):
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
        if mode == "sdi":
            print(json.dumps(sdi_page.ddl(f, sdi_idx)["dd_object"]))
        elif mode == "ddl":
            table_object = Table(**sdi_page.ddl(f, sdi_idx)["dd_object"])

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

            constraints = table_object.gen_check_constraints()
            columns_dec.extend(constraints)
            foreign_keys = table_object.gen_foreign_key()
            columns_dec.extend(foreign_keys)
            columns_dec = "\n    " + ",\n    ".join(columns_dec) + "\n"
            table_collation = const.get_collation_by_id(table_object.collation_id)
            parts = table_object.gen_sql_for_partition()
            desc = f"ENGINE={table_object.engine} DEFAULT CHARSET={table_collation.CHARACTER_SET_NAME} COLLATE={table_collation.COLLATION_NAME}"
            comment = (
                "\nCOMMENT '" + table_object.comment + "'"
                if table_object.comment
                else ""
            )
            print(
                f"CREATE TABLE {table_name} ({columns_dec}) {desc} {chr(10)+parts if parts else ''}{comment}"
            )
        else:
            table_object = Table(**sdi_page.ddl(f, sdi_idx)["dd_object"])
            root_page_no = int(table_object.indexes[0].private_data.get("root", 4))
            f.seek(root_page_no * const.PAGE_SIZE)
            root_index_page = MIndexPage.parse_stream(f)
            first_leaf_page_no = root_index_page.get_first_leaf_page(
                f, table_object.get_primary_key_col()
            )
            if first_leaf_page_no is None:
                print("no data")
                return
            values = []

            def transfter(nd):
                vs = []
                for field in nd:
                    if isinstance(field, dict) or isinstance(field, list):
                        vs.append(repr(json.dumps(field)))
                    elif field is None:
                        vs.append("NULL")
                    elif (
                        isinstance(field, date)
                        or isinstance(field, timedelta)
                        or isinstance(field, datetime)
                    ):
                        vs.append(f"'{str(field)}'")
                    elif isinstance(field, MGeo):
                        d = field.build().hex()  # .zfill(50)
                        vs.append("0x" + d)
                    else:
                        vs.append(repr(field))
                values.append(f"({','.join(vs)})")

            default_value_parser = MIndexPage.default_value_parser(
                table_object, transfter
            )
            while first_leaf_page_no != 4294967295:
                f.seek(first_leaf_page_no * const.PAGE_SIZE)
                index_page = MIndexPage.parse_stream(f)
                index_page.iterate_record_header(f, value_parser=default_value_parser)
                first_leaf_page_no = index_page.fil.next_page

            table_name = f"`{table_object.schema_ref}`.`{table_object.name}`"
            print(
                f"INSERT INTO {table_name}({','.join(table_object.DataClass._fields)}) values {', '.join(values)}"
            )

        return


# 'type': sql/dd/types/column.h::enum_column_type
# column_key : ag --cpp \ CK_NONE

# column_type_utf8 -> ag --cpp '::sql_type'

not_focus_col_name = ["DB_TRX_ID", "DB_ROLL_PTR", "DB_ROW_ID"]
