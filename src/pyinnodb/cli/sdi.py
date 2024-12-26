from . import *

from pyinnodb.disk_struct.index import MIndexHeader, MSDIPage, MSystemRecord
from pyinnodb.disk_struct.index import MFsegHeader, MIndexSystemRecord, MDDL
from pyinnodb.disk_struct.fil import MFil
from pyinnodb.disk_struct.record import MRecordHeader
from pyinnodb.sdi.table import Column, Table
import zlib
import json


@main.command()
@click.pass_context
@click.option("--pageno", type=click.INT)
@click.option("--idx", type=click.INT, default=-1)
def sdi(ctx, pageno, idx):
    f = ctx.obj["fn"]
    fsp_page = ctx.obj["fsp_page"]
    if fsp_page.sdi_version == 0:
        print("there is no SDI info in this file")
        return

    f.seek(fsp_page.sdi_page_no * const.PAGE_SIZE)
    sdi_page = MSDIPage.parse_stream(f)

    all_sdi_record = list(sdi_page.iterate_sdi_record(f))

    if idx >= 0:
        print(json.dumps(all_sdi_record[idx]))
    else:
        for i, sdi in enumerate(all_sdi_record):
            if sdi["dd_object_type"] == "Table":
                dd_object = Table(**sdi["dd_object"])
                print(i, dd_object.name, dd_object.indexes[0].se_private_data)
