from pyinnodb.disk_struct.index import MIndexPage
from pyinnodb.sdi import table
import json
import dataclasses

from . import *


@main.command()
@click.pass_context
@click.argument("frmfile")
@click.option(
    "--mode", 
    type=click.Choice(["ddl", "dump", "json", "search"]), 
    default="dump",
    help="Operation mode: ddl (show CREATE TABLE), dump (export INSERT statements), json (export as JSON), search (search by primary key)"
)
@click.option("--schema/--no-schema", default=True, help="Include schema name in DDL output")
@click.option("--primary-key", type=click.STRING, default="", help="Primary key value for search mode")
@click.option("--hidden-col/--no-hidden-col", type=click.BOOL, default=False, help="Show hidden columns (DB_TRX_ID, DB_ROLL_PTR)")
@click.option("--root-page", type=click.INT, default=3, help="Root page number (default: 3 for MySQL 5.7)")
def frm(ctx, frmfile, mode, schema, primary_key, hidden_col, root_page):
    """Parse MySQL 5.7 data files using .frm and .ibd
    
    \b
    This command supports MySQL 5.7 and earlier versions that use .frm files.
    
    \b
    Examples:
        # Show table structure (DDL)
        pyinnodb --fn table.ibd frm table.frm --mode ddl
        
        # Export data as INSERT statements
        pyinnodb --fn table.ibd frm table.frm --mode dump
        
        # Export data as JSON
        pyinnodb --fn table.ibd frm table.frm --mode json
        
        # Search by primary key
        pyinnodb --fn table.ibd frm table.frm --mode search --primary-key 42
    """
    ibd = ctx.obj["fn"]
    
    # Parse FRM file to get table structure
    t = table.Table(name="table")
    t.update_with_frm(frmfile)
    
    # Set default values for MySQL 5.7
    t.schema_ref = "test"  # Default schema
    t.engine = "InnoDB"
    t.collation_id = 33  # utf8mb3_general_ci - common default for MySQL 5.7
    
    if mode == "ddl":
        # Generate and output DDL
        ddl = t.gen_ddl(schema)
        print(ddl)
        return
    
    # For other modes, we need to read data
    root_page_no = root_page
    ibd.seek(root_page_no * const.PAGE_SIZE)
    root_index_page = MIndexPage.parse_stream(ibd)
    first_leaf_page = root_index_page.get_first_leaf_page(ibd, t.get_primary_key_col())
    
    if first_leaf_page is None:
        print("No data found in the table")
        return
    
    if mode == "search":
        # Search by primary key
        if primary_key == "":
            print("Error: --primary-key is required for search mode")
            print("Example: --primary-key 42")
            return
        
        try:
            primary_key = eval(primary_key)
        except Exception as e:
            print(f"Error parsing primary key: {e}")
            return
        
        result = t.search(ibd, primary_key, hidden_col)
        if result is None:
            print("Record not found")
        else:
            print(f"Found: {result}")
        return
    
    # For dump and json modes, iterate all records
    transfer = t.wrap_transfer
    if mode == "json":
        transfer = None
    
    default_value_parser = MIndexPage.default_value_parser(t, hidden_col=hidden_col, transfer=transfer)
    
    values = []
    page_no = first_leaf_page
    while page_no != const.FFFFFFFF:
        ibd.seek(page_no * const.PAGE_SIZE)
        index_page = MIndexPage.parse_stream(ibd)
        page_values = list(index_page.iterate_record_header(
            ibd,
            value_parser=default_value_parser,
            page=page_no,
        ))
        values.extend(page_values)
        page_no = index_page.fil.next_page
    
    if len(values) == 0:
        print("No data found")
        return
    
    if mode == "json":
        # Output as JSON
        print(json.dumps([dataclasses.asdict(v) for v in values], default=str, indent=2))
    else:
        # Output as INSERT statements (dump mode)
        values_str = [f"({','.join(v)})" for v in values]
        table_name = f"`{t.schema_ref}`.`{t.name}`"
        
        for v in values_str:
            print(f"INSERT INTO {table_name}({','.join(t.keys())}) VALUES {v};")
