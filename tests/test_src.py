from context import *
from pyinnodb.struct.deplicate import *
from pyinnodb import const

test_fil_case = [
    (
        b"\x86$\xad\x90\x00\x00\x00\x00\x00\x018\xe4\x00\x00\x00\x01\x00\x00\x00\x00\x01*\x07\xa2\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03",
        Fil(
            checksum=2250550672,
            offset=0,
            pre_page=80100,
            next_page=1,
            lsn=19531682,
            page_type=8,
            flush_lsn=0,
            spaceid=3,
        ),
    ),
    (
        b":8\rX\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01)\xfaN\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03",
        Fil(
            checksum=976751960,
            offset=1,
            pre_page=0,
            next_page=0,
            lsn=19528270,
            page_type=5,
            flush_lsn=0,
            spaceid=3,
        ),
    ),
    (
        b"\t\xbd\xae\xf0\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01*\x07\xa2\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03",
        Fil(
            checksum=163426032,
            offset=2,
            pre_page=0,
            next_page=0,
            lsn=19531682,
            page_type=3,
            flush_lsn=0,
            spaceid=3,
        ),
    ),
    (
        b"Ezp\xec\x00\x00\x00\x03\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x01*\x1f\xcbE\xbd\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03",
        Fil(
            checksum=1165652204,
            offset=3,
            pre_page=4294967295,
            next_page=4294967295,
            lsn=19537867,
            page_type=17853,
            flush_lsn=0,
            spaceid=3,
        ),
    ),
    (
        b"2\x17X&\x00\x00\x00\x04\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x01*,\xa2E\xbf\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03",
        Fil(
            checksum=840390694,
            offset=4,
            pre_page=4294967295,
            next_page=4294967295,
            lsn=19541154,
            page_type=17855,
            flush_lsn=0,
            spaceid=3,
        ),
    ),
    (
        b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        Fil(
            checksum=0,
            offset=0,
            pre_page=0,
            next_page=0,
            lsn=0,
            page_type=0,
            flush_lsn=0,
            spaceid=0,
        ),
    ),
]


def test_fil():
    totest(Fil, test_fil_case)


test_list_base_node = ListBaseNode(
    length=6,
    first=Pointer(
        page_number=7,
        page_offset=8,
    ),
    last=Pointer(
        page_number=9,
        page_offset=10,
    ),
)


def test_fsp():
    fsp = FspHeader(
        space_id=1,
        unused=0,
        highest_page_number=2,
        highest_page_number_init=3,
        flags=4,
        free_frag_page_number=5,
        list_base_free=test_list_base_node,
        list_base_free_frag=test_list_base_node,
        list_base_full_frag=test_list_base_node,
        next_seg_id=11,
        list_base_full_inode=test_list_base_node,
        list_base_free_inode=test_list_base_node,
    )
    assert len(fsp.build()) == 150 - 38
    logger.debug("fsp is %s", fsp)
    logger.debug("fsp data is %s", fsp.build())


def test_inode():
    inode = InodeEntry(
        fseg_id=1,
        not_full_list_used_page=2,
        list_base_free=test_list_base_node,
        list_base_not_full=test_list_base_node,
        list_base_full=test_list_base_node,
        magic_number=3,
        fragment_array=32 * [4],
    )
    logger.debug("inode is %s", inode)
    assert len(inode.build()) == 192


test_list_case = [
    (
        b"\x00\x00\x00\x01\x00\x02\x00\x00\x00\x03\x00\x04",
        ListNode(
            prev=Pointer(page_number=1, page_offset=2),
            next=Pointer(page_number=3, page_offset=4),
        ),
    )
]


def test_list_node():
    totest(ListNode, test_list_case)


def test_bitarray():
    ba = BitArray(4, 2)
    for i in range(8):
        data = ba.build([0, 0, 0, i])
        logger.info("%d, %s, %d", i, data, len(data))


def test_xdes():
    xe = XdesEntry(
        fseg_id=1,
        xdes_list=ListNode(
            prev=Pointer(page_number=1, page_offset=2),
            next=Pointer(page_number=3, page_offset=4),
        ),
        state=5,
        page_state=[0, 1, 0, 1] * 16,
    )
    logger.info(str(xe))
    logger.info(xe.build())
    logger.info(len(xe.build()))
    logger.info(xe._parse_order)
    assert len(xe.build()) == 40


def test_ibuf_page():
    with open(test_filename, "rb") as f:
        f.seek(1 * 16 * 1024)
        ibuf_page = IBufPage.parse_stream(f)
        logger.info("ibuf is %s", ibuf_page.change_buffer_bitmap[:3])


def test_list_page():
    with open(test_filename, "rb") as f:
        fil = Fil.parse_stream(f)
        logger.info(fil)
        fsp_header = struct_parse(FspHeader, f)
        logger.info(fsp_header)
        for i in range(fsp_header.highest_page_number):
            f.seek(i * 16 * 1024)
            page_fil = struct_parse(Fil(), f)
            logger.info("offset: %d, page_type: %s",
                        page_fil.offset,
                        const.get_page_type_name(page_fil.page_type),)


def test_fsp_header():
    with open(test_filename, "rb") as f:
        fil = Fil.parse_stream(f)
        fsp_header = FspHeader.parse_stream(f)
        logger.info("fsp_header is %s", fsp_header)
        xdess = []
        for i in range(256):
            xdess.append(XdesEntry.parse_stream(f))
            logger.info("idx: %d, segid: %d", i, xdess[-1].fseg_id)
            break


def test_sdi_page():
    with open(test_filename, "rb") as f:
        f.seek(3 * 16 * 1024)
        index_page = SDIPage._parse(f)
        logger.info("leaf pointer: %s", index_page.fseg_header.leaf_pointer)
        logger.info("internal_pointer %s",
                    index_page.fseg_header.internal_pointer)
        logger.info("fil: %d, index_header: %d, fseg_header: %d", index_page.fil._consume_num,
                    index_page.index_header._consume_num, index_page.fseg_header._consume_num)
        logger.info("system.infimum: %d, system.supremum: %d",
                    index_page.system_records.infimum._consume_num, index_page.system_records.supremum._consume_num)
        logger.info("infimum is %s", index_page.system_records.infimum)
        logger.info("supremum is %s", index_page.system_records.supremum)
        assert index_page.system_records.infimum.marker == b"infimum\x00"
        assert index_page.system_records.infimum.record_type == 2
        assert index_page.system_records.supremum.marker == b"supremum"
        assert index_page.system_records.supremum.record_type == 3
        logger.info("index_header is %s", index_page.index_header)
        logger.info("index_page.consumer is %d", index_page._consume_num)
        logger.info("tail is %s", index_page.fil_tailer)


def test_inode_page():
    with open(test_filename2, "rb") as f:
        f.seek(2 * 16 * 1024)
        inode_page = InodePage.parse_stream(f)
        logger.info(
            "consume_num: fil_header: %d, list_node_inode_page: %d, inodes[0]: %d",
            inode_page.fil_header._consume_num,
            inode_page.list_node_inode_page._consume_num,
            inode_page.inodes[0]._consume_num,
        )
        logger.debug(
            "consume befor inodes is %d",
            inode_page.fil_header._consume_num
            + inode_page.list_node_inode_page._consume_num,
        )
        f.seek(4 * 16 * 1024)
        index_page = IndexPage.parse_stream(f)
        logger.debug("leaf_pointer: %s", index_page.fseg_header.leaf_pointer)
        logger.debug("internal_pointer: %s",
                     index_page.fseg_header.internal_pointer)
        _, idx = index_page.fseg_header.leaf_pointer.inode_idx()
        logger.debug("inode_entry is %s", inode_page.inodes[idx])
        _, idx = index_page.fseg_header.internal_pointer.inode_idx()
        logger.debug("internal entry is %s", inode_page.inodes[idx])


def test_inode_entry():
    with open(test_filename1, "rb") as f:
        f.seek(2 * 16 * 1024 + 50)
        data = f.read(192)
        inode_entry = InodeEntry.parse_stream(BytesIO(data))
        logger.debug("inode entry is %s", inode_entry)


def test_index_iter():
    with open(test_filename1, "rb") as f:
        f.seek(3 * 16 * 1024)
        index_page = SDIPage.parse_stream(f)
        f.seek(index_page.fseg_header.leaf_pointer.seek_loc())
        leaf_inode = InodeEntry()
        leaf_inode.parse_stream(f)
        f.seek(index_page.fseg_header.internal_pointer.seek_loc())
        internal_inode = InodeEntry.parse_stream(f)
        logger.info("leaf_inode %s", leaf_inode)
        logger.info("leaf_space_id %d", index_page.fseg_header.leaf_space_id)
        logger.info("root page is %d", internal_inode.fragment_array[0])


def test_iter_record():
    with open(test_filename, "rb") as f:
        f.seek(4 * 16 * 1024)
        index_page = IndexPage.parse_stream(f)
        logger.info("fseg_header is %s", index_page.fseg_header)
        logger.info("infimum is %s", index_page.system_records.infimum)
        logger.info("supremum is %s", index_page.system_records.supremum)
        logger.info("page_dir is %s", index_page.page_directory)
        logger.info("infimum consume %d", index_page.fil._consume_num + index_page.index_header._consume_num
                    + index_page.fseg_header._consume_num + index_page.system_records.infimum._consume_num-8)
        f.seek(4 * 16 * 1024
               + index_page.fil._consume_num
               + index_page.index_header._consume_num
               + index_page.fseg_header._consume_num
               + index_page.system_records.infimum._consume_num-8)
        next_record_offset = index_page.system_records.infimum.next_record_offset
        cnt = 0
        while next_record_offset != 0:
            f.seek(next_record_offset - 5, 1)
            rh = RecordHeader.parse_stream(f)
            cnt += 1
            if cnt >= 10:
                break
            logger.info("rh is %s", rh)
            next_record_offset = rh.next_record_offset


def test_index_page():
    with open(test_filename, "rb") as f:
        f.seek(4 * 16 * 1024)
        index_page = IndexPage.parse_stream(f)
        # logger.info("index page size %d", len(index_page.build()))
        # logger.info("page_type %s", get_page_type_name(index_page.fil.page_type))
        # logger.info("fil size %d, index_header size %d, fseg_header size %d", len(index_page.fil.build()), len(index_page.index_header.build()), len(index_page.fseg_header.build()))
        # logger.info("infimum size %d", len(index_page.system_records.infimum.build()))
        logger.info("leaf pointer: %s", index_page.fseg_header.leaf_pointer)
        logger.info("internal_pointer %s",
                    index_page.fseg_header.internal_pointer)
        logger.info("infimum is %s", index_page.system_records.infimum)
        logger.info("supremum is %s", index_page.system_records.supremum)
