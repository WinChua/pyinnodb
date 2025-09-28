from pyinnodb import const
from pyinnodb.const import util
from pyinnodb.disk_struct.index import MSDIPage
from pyinnodb.sdi.table import Table

from . import *


@main.command()
@click.pass_context
def sys_tablespace(ctx):
    f = ctx.obj["fn"]

    mysqlibd = os.path.basename(f.name)
    if mysqlibd != "mysql.ibd":
        print("fn should be the mysql.ibd file")
        return

    fsp_page = ctx.obj["fsp_page"]
    if fsp_page.sdi_version == 0:
        print("there is no SDI info in this file")
        return

    f.seek(fsp_page.sdi_page_no * const.PAGE_SIZE)

    sdi_page = MSDIPage.parse_stream(f)

    for i, sdi in enumerate(sdi_page.iterate_sdi_record(f)):
        if sdi["dd_object_type"] == "Table":
            dd_object = Table(**sdi["dd_object"])
            if dd_object.name in ["tablespace_files"]:
                records = dd_object.iter_record(f)
                for r in records:
                    if r.se_private_data is not None:
                        private_data = const.line_to_dict(r.se_private_data, ";", "=")
                        space_id = int(private_data.get("id", 0))
                        if util.is_id_undo_tablespace(space_id):
                            print(r)
