from context import *
from pyinnodb.disk_struct.inode import *
from pyinnodb.disk_struct.fsp import *
from pyinnodb.disk_struct.xdes import *
from pyinnodb.disk_struct.fil import *


# inode is fseg
def test_inode_page():
    with open(test_filename, "rb") as f:
        f.seek(2 * const.PAGE_SIZE)
        inode_page = MInodePage.parse_stream(f)
        page_usage = inode_page.iter_inode(
            func=lambda inode: (inode.fseg_id, inode.page_used(f)))
        logger.info(MXdesEntry.sizeof())
        for fsegid, pu in page_usage:
            for k, v in pu.items():
                if isinstance(v, list):
                    logger.info("k:%s.%s, usage:%s", fsegid, k,
                                const.show_seq_page_list(v))
                elif isinstance(v, dict):
                    for kk, vv in v.items():
                        logger.info("k:%s.%s,id:%s,usage:%s", fsegid, k, kk,
                                    const.show_seq_page_list(vv))
