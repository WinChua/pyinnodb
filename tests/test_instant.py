from context import *
from pyinnodb.sdi.table import Table
from pyinnodb.disk_struct.index import MSDIPage
from pyinnodb.disk_struct.fsp import MFspPage


def test_parse_mysql8_instant(mysqlfile: MysqlFile):
    f = mysqlfile.mysql8instantibd
    fsp_page = MFspPage.parse_stream(f)
    f.seek(fsp_page.sdi_page_no * const.PAGE_SIZE)
    sdi_page = MSDIPage.parse_stream(f)
    dd_object = Table(**sdi_page.ddl(f, 0)["dd_object"])

    result = dd_object.iter_record(f)

    cls = dd_object.DataClass
    r1 = cls(
        id=1,
        name="original record",
        drop2="original drop 2 column",
        add1=None,
        add2="add2 default",
        drop1=99,
    )
    r2 = cls(
        id=2,
        name="insert after alter",
        drop2="drop2 after alter",
        add1=None,
        add2="add2",
        drop1=99,
    )
    assert len(result) == 2
    assert r1 == result[0]
    assert r2 == result[1]
