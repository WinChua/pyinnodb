from context import *

from pyinnodb.disk_struct.inode import MInodePage
from pyinnodb.disk_struct.index import MIndexPage
from pyinnodb.disk_struct.fil import MFil

def test_instant(containerOp: ContainerOp):
    containerOp.build_ibd(
        insert(User).values(name="WinChua", age=30)
    )
    table_data_dir = containerOp.build_data_path(f"/test/{test_table_name}.ibd")

    tar1 = containerOp.open(table_data_dir)

    containerOp.build_ibd(
        text(f"ALTER TABLE {test_table_name} DROP COLUMN name")
    )

    tar2 = containerOp.open(table_data_dir)

    logger.debug(MFil.parse_stream(tar1))
    logger.debug(MFil.parse_stream(tar2))

    tar1.seek(2 * const.PAGE_SIZE)
    tar1_inode = MInodePage.parse_stream(tar1).iter_inode()[0]
    tar2.seek(2 * const.PAGE_SIZE)
    tar2_inode = MInodePage.parse_stream(tar2).iter_inode()[0]
    logger.debug(tar1_inode)
    logger.debug(tar2_inode)

