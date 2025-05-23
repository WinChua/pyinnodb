import typing
from ..mconstruct import *
from .list import MListBaseNode, MListNode
from .fil import MFil

from pyinnodb import const

blob_hdr_size = 8


class MBlobHdr(CC):  # only used in MySQL 5
    part_len: int = cfield(cs.Int32ub)
    next_page_no: int = cfield(cs.Int32ub)  # FIL_NULL if none 0xffffffff

    def get_data(self, stream):
        data = stream.read(self.part_len)
        if self.next_page_no == 0xFFFFFFFF:
            return data
        else:
            stream.seek(self.next_page_no * const.PAGE_SIZE + 38)
            blob_header = MBlobHdr.parse_stream(stream)
            return data + blob_header.get_data(stream)


def btr_blob_get_next_page_no():
    return 0


class MIndexEntryNode(CC):  ## index_entry_t
    node: MListNode = cfield(MListNode)
    # prev: MPointer = cfield(MPointer)
    # next: MPointer = cfield(MPointer)
    versions: MListBaseNode = cfield(MListBaseNode)
    trx_id: int = cfield(IntFromBytes(6))
    trx_id_modifier: int = cfield(IntFromBytes(6))
    trx_undo_no: int = cfield(cs.Int32ub)
    trx_undo_no_modifier: int = cfield(cs.Int32ub)
    page_no: int = cfield(cs.Int32ub)
    data_len: int = cfield(cs.Int32ul)
    lob_version: int = cfield(cs.Int32ub)


class MFirstPage(CC):  ## first_page_t
    fil: MFil = cfield(MFil)
    version: int = cfield(cs.Int8ub)
    flag: int = cfield(cs.Int8ub)
    lob_version: int = cfield(cs.Int32ub)
    last_trx_id: int = cfield(IntFromBytes(6))
    last_undo_no: int = cfield(cs.Int32ub)
    data_len: int = cfield(cs.Int32ub)
    trx_id: int = cfield(IntFromBytes(6))
    index_list: MListBaseNode = cfield(
        MListBaseNode
    )  # index_list indicate the used entries in index_entry
    index_free_node: MListBaseNode = cfield(
        MListBaseNode
    )  # index_entry list locate after index_free_node, which indicate the free entry in index_entry
    index_entry: typing.List[MIndexEntryNode] = cfield(carray(10, MIndexEntryNode))

    def _post_parsed(self, stream, context, path):
        self.first_page_data = stream.read(self.data_len)

    def get_data(self, stream):
        ie = self.index_entry[0]
        data = self.first_page_data
        for i in range(self.index_list.length):
            logger.debug("ie is %s", ie)
            stream.seek(ie.page_no * const.PAGE_SIZE)
            dp = MDataPage.parse_stream(stream)
            data += stream.read(dp.data_len)
            if ie.node.next.page_number == const.FFFFFFFF:
                break
            stream.seek(ie.node.next.seek_loc())
            ie = MIndexEntryNode.parse_stream(stream)

        return data

        stream.seek(self.index_entry[0].page_no * const.PAGE_SIZE + self.sizeof())
        first_page_data = stream.read(self.data_len)
        for i in range(1, self.index_list.length):
            stream.seek(self.index_entry[i].page_no * const.PAGE_SIZE)
            dp = MDataPage.parse_stream(stream)
            first_page_data += stream.read(dp.data_len)
        return first_page_data

    # def post_parsed(self, stream, context, path):
    #     free_node: typing.List[MIndexEntryNode] = []
    #     for idx in range(self.index_free_node.length):
    #         free_node.append(MIndexEntryNode.parse_stream(stream))

    #     self.index_free_node_list = free_node


class MDataPage(CC):
    fil: MFil = cfield(MFil)
    version: int = cfield(cs.Int8ub)
    data_len: int = cfield(cs.Int32ub)
    trx_id: int = cfield(IntFromBytes(6))
