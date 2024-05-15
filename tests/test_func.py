from context import *
from pyinnodb.struct import *
from pyinnodb.const import get_page_type_name
from pyinnodb import const

test_filename1 = parent_dir + "/data/t1.ibd"
test_filename = "/home/winchua/github/mysql-server/make/data/test/t1.ibd"
test_filename2 = "/home/winchua/github/mysql-server/make/data/test/t_btree.ibd"

def test_list_page():
    with open(test_filename, "rb") as f:
        fil = Fil()
        fil.parse_stream(f)
        logger.info(fil)
        fsp_header = struct_parse(FspHeader(), f)
        logger.info(fsp_header)
        for i in range(fsp_header.highest_page_number):
            f.seek(i * 16 * 1024)
            page_fil = struct_parse(Fil(), f)
            logger.info("offset: %d, page_type: %s", page_fil.offset, const.get_page_type_name(page_fil.page_type))


def test_fsp_header():
    with open(test_filename, "rb") as f:
        fil = Fil()
        fil.parse_stream(f)
        fsp_header = FspHeader()
        fsp_header.parse_stream(f)
        logger.info("fsp_header is %s", fsp_header)
        xdess = []
        return
        for i in range(256):
            xdess.append(XdesEntry().parse_stream(f))
            logger.info("idx: %d, segid: %d", i, xdess[-1].fseg_id)

def test_sdi_page():

    with open(test_filename1, "rb") as f:
        f.seek(3 * 16 * 1024)
        index_page = SDIPage()
        index_page.parse_stream(f)
        # logger.info("index page size %d", len(index_page.build()))
        # logger.info("page_type %s", get_page_type_name(index_page.fil.page_type))
        # logger.info("fil size %d, index_header size %d, fseg_header size %d", len(index_page.fil.build()), len(index_page.index_header.build()), len(index_page.fseg_header.build()))
        # logger.info("infimum size %d", len(index_page.system_records.infimum.build()))
        logger.info("leaf pointer: %s", index_page.fseg_header.leaf_pointer)
        logger.info("internal_pointer %s", index_page.fseg_header.internal_pointer)
        logger.info("infimum is %s", index_page.system_records.infimum)
        logger.info("supremum is %s", index_page.system_records.supremum)
        assert index_page.system_records.infimum.marker == b"infimum\x00"
        assert index_page.system_records.infimum.record_type == 2
        assert index_page.system_records.supremum.marker == b"supremum"
        assert index_page.system_records.supremum.record_type == 3
        logger.info("index_header is %s", index_page.index_header)
        logger.info("index_page.consumer is %d", index_page._consume_num)
        logger.info("tail is %s", index_page.fil_tailer)


test_index_page_data = b'2\x17X&\x00\x00\x00\x04\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x01*,\xa2E\xbf\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03\x00\x02\x00\xcf\x80\x05\x00\x00\x00\x00\x00\xb9\x00\x05\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x9c\x00\x00\x00\x03\x00\x00\x00\x02\x02r\x00\x00\x00\x03\x00\x00\x00\x02\x01\xb2\x01\x00\x02\x00\x1cinfimum\x00\x04\x00\x0b\x00\x00supremum'

def test_index_page():

    with open(test_filename1, "rb") as f:
        f.seek(4 * 16 * 1024)
        index_page = IndexPage()
        index_page.parse_stream(f)
        # logger.info("index page size %d", len(index_page.build()))
        # logger.info("page_type %s", get_page_type_name(index_page.fil.page_type))
        # logger.info("fil size %d, index_header size %d, fseg_header size %d", len(index_page.fil.build()), len(index_page.index_header.build()), len(index_page.fseg_header.build()))
        # logger.info("infimum size %d", len(index_page.system_records.infimum.build()))
        logger.info("leaf pointer: %s", index_page.fseg_header.leaf_pointer)
        logger.info("internal_pointer %s", index_page.fseg_header.internal_pointer)
        logger.info("infimum is %s", index_page.system_records.infimum)
        logger.info("supremum is %s", index_page.system_records.supremum)

def test_inode_page():
    with open(test_filename2, "rb") as f:
        f.seek(2 * 16 * 1024)
        inode_page = InodePage()
        inode_page.parse_stream(f)
        logger.info("consume_num: fil_header: %d, list_node_inode_page: %d, inodes[0]: %d",
        inode_page.fil_header._consume_num, inode_page.list_node_inode_page._consume_num, inode_page.inodes[0]._consume_num)
        logger.debug("consume befor inodes is %d", inode_page.fil_header._consume_num + inode_page.list_node_inode_page._consume_num)
        f.seek(4 * 16 * 1024)
        index_page = IndexPage()
        index_page.parse_stream(f)
        logger.debug("leaf_pointer: %s", index_page.fseg_header.leaf_pointer)
        logger.debug("internal_pointer: %s", index_page.fseg_header.internal_pointer)
        _, idx = index_page.fseg_header.leaf_pointer.inode_idx()
        logger.debug("inode_entry is %s", inode_page.inodes[idx])
        _, idx = index_page.fseg_header.internal_pointer.inode_idx()
        logger.debug("internal entry is %s", inode_page.inodes[idx])

def test_inode_entry():
    with open(test_filename1, "rb") as f:
        f.seek(2*16*1024 + 50)
        data = f.read(192)
        inode_entry = InodeEntry()
        inode_entry.parse_stream(BytesIO(data))
        logger.debug("inode entry is %s", inode_entry)
        inodes = construct.Array(1, InodeEntry())
        a = inodes.parse_stream(BytesIO(data))
        logger.debug("inodes is %s", a[0])
        logger.debug("eq is %s", a == inode_entry)

def test_index_iter():
    with open(test_filename1, "rb") as f:
        f.seek(3 * 16 * 1024)
        index_page = SDIPage()
        index_page.parse_stream(f)
        f.seek(index_page.fseg_header.leaf_pointer.seek_loc())
        leaf_inode = InodeEntry()
        leaf_inode.parse_stream(f)
        f.seek(index_page.fseg_header.internal_pointer.seek_loc())
        internal_inode = InodeEntry()
        internal_inode.parse_stream(f)
        logger.info("leaf_inode %s", leaf_inode)
        logger.info("leaf_space_id %d", index_page.fseg_header.leaf_space_id)
        logger.info("root page is %d", internal_inode.fragment_array[0])

