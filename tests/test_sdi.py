from context import *

from pyinnodb.disk_struct.index import MSDIPage
from pyinnodb.sdi.table import Table

import json

def test_sdi_parse():
    with open(test_filename, "rb") as f:
        f.seek(3 * const.PAGE_SIZE)
        sdi_page = MSDIPage.parse_stream(f)
        table = Table(**sdi_page.ddl["dd_object"])
        logger.info("table is %s", table)
