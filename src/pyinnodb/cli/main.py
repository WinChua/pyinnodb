import click
import logging
from pyinnodb.disk_struct.fsp import MFspPage
from pyinnodb.disk_struct.fil import MFil
from pyinnodb import const
from io import BytesIO
import sys

import dataclasses

logger = logging.getLogger(__name__)


@click.group()
@click.argument("fn", type=click.File("rb"))
@click.option(
    "--log-level", type=click.Choice(["DEBUG", "ERROR", "INFO"]), default="ERROR"
)
@click.pass_context
def main(ctx, fn, log_level):
    '''A ibd file parser for MySQL 8.0 above, help you to know innodb better.

    It offer several function bellow:
    a) validate the checksum of your ibd file;
    b) output the DDL of table;
    c) dump the data in ibd file as INSERT statments;
    d) search record by primary key;
    e) show the undo log history

    many other function to explore your ibd file

    '''
    logging.basicConfig(
        format="[%(levelname)s]-[%(filename)s:%(lineno)d] %(message)s", level=log_level
    )
    ctx.ensure_object(dict)
    ctx.obj["fn"] = fn
    try:
        fsp_page = MFspPage.parse_stream(fn)
        ctx.obj["fsp_page"] = fsp_page
        for pn in range(fsp_page.fsp_header.highest_page_number):
            fn.seek(const.PAGE_SIZE * pn)
            page_data = fn.read(const.PAGE_SIZE)
            fil = MFil.parse(page_data)
            if fil.page_type == const.FIL_PAGE_TYPE_ALLOCATED:
                continue
            checksum = const.page_checksum_crc32c(page_data)
            if checksum != fil.checksum:
                print(f"PAGE {pn}'s checksum is invalid, stored[{hex(fil.checksum)}] != calculate[{hex(checksum)}]")
                print("use validate to get a more detail output of the validation")
                sys.exit(1)
    except Exception as e:
        print(e)
        print("the file parse faile")

