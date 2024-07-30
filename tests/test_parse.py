from context import *
from pyinnodb.sdi.table import Table
from pyinnodb.disk_struct.index import MSDIPage
from pyinnodb.disk_struct.fsp import MFspPage
import datetime
from decimal import Decimal

timeformat = "%Y-%m-%d %H:%M:%S"

def test_parse_mysql8(mysqlfile: MysqlFile):
    fsp_page = MFspPage.parse_stream(mysqlfile.mysql8ibd)
    mysqlfile.mysql8ibd.seek(fsp_page.sdi_page_no * const.PAGE_SIZE)
    sdi_page = MSDIPage.parse_stream(mysqlfile.mysql8ibd)
    dd_object = Table(**sdi_page.ddl(mysqlfile.mysql8ibd, 0)["dd_object"])
    
    result = dd_object.iter_record(mysqlfile.mysql8ibd, transfter=lambda x: x)
    cls = dd_object.DataClass
    test_data = cls(id=1, BIGINT=98283201,
        BIT = 1, DATETIME=datetime.datetime.strptime('2024-01-01 09:00:01',timeformat),
        DOUBLE = 3.1415926, FLOAT = 6.189000129699707 ,
        INTEGER = 8621, DECIMAL=Decimal('910.79'),
        LONGBLOB= 'x' * 100,
        LONGTEXT = 'g' * 3,
        MEDIUMBLOB = None,
        MEDIUMINT = 999999,
        MEDIUMTEXT = None,
        NUMERIC = Decimal('11'), REAL = 1092.892,
        SMALLINT = 981, TEXT = "TEXT",
        TIME = datetime.timedelta(seconds=11040),
        TIMESTAMP = datetime.datetime.strptime("2024-07-24 09:05:28", timeformat).replace(tzinfo=datetime.timezone.utc),
        YEAR = 2024, ENUM = b"a", SET  = "a,b,c",
        TINYBLOB = "TINYBLOB",
        TINYINT = 99, TINYTEXT = "TINYTEXT",
        CHAR = "09283012", VARBINARY = "VARBINARY",
        int_def_col = 42, str_def_col='world',
    )
    assert len(result) == 2
    assert test_data == result[0]

