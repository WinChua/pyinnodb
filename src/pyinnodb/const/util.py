from pyinnodb.disk_struct.fsp import MFspPage
from pyinnodb.disk_struct.index import MSDIPage
from pyinnodb.sdi.table import Table
from pyinnodb import const

from os import path


def is_id_undo_tablespace(id):
    return id >= 4244167280 and id <= 4294967279


def get_undo_tablespacefile(fn: str):
    result = {}
    with open(fn, "rb") as f:
        fsp_page = MFspPage.parse_stream(f)
        if fsp_page.sdi_version == 0:
            return

        f.seek(fsp_page.sdi_page_no * const.PAGE_SIZE)
        sdi_page = MSDIPage.parse_stream(f)
        for sdi in sdi_page.iterate_sdi_record(f):
            if sdi["dd_object_type"] != "Table":
                continue

            dd_object = Table(**sdi["dd_object"])
            if dd_object.name != "tablespace_files":
                continue

            for r in dd_object.iter_record(f):
                if r.se_private_data is None:
                    continue
                private_data = const.line_to_dict(r.se_private_data, ";", "=")
                space_id = int(private_data.get("id", 0))
                if is_id_undo_tablespace(space_id):
                    fname = path.dirname(fn) + "/" + path.basename(r.file_name)
                    result[0xFFFFFFF0 - space_id] = open(fname, "rb")

    return result
