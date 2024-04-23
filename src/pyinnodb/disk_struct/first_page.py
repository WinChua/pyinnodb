import typing
from ..mconstruct import *
from .list import MListBaseNode, MPointer


class MIndexEntryNode(CC): ## index_entry_t
    prev: MPointer = cfield(MPointer)
    next: MPointer = cfield(MPointer)
    versions: MListBaseNode = cfield(MListBaseNode)
    trx_id: int = cfield(IntFromBytes(6))
    trx_id_modifier: int = cfield(IntFromBytes(6))
    trx_undo_no: int = cfield(cs.Int32ub)
    trx_undo_no_modifier: int = cfield(cs.Int32ub)
    page_no: int = cfield(cs.Int32ub)
    data_len: int = cfield(cs.Int32ub)
    lob_version: int = cfield(cs.Int32ub)


class MFirstPage(CC): ## first_page_t
    version: int = cfield(cs.Int8ub)
    flag: int = cfield(cs.Int8ub)
    lob_version: int = cfield(cs.Int32ub)
    last_trx_id: int = cfield(IntFromBytes(6))
    last_undo_no: int = cfield(cs.Int32ub)
    data_len: int = cfield(cs.Int32ub)
    trx_id: int = cfield(IntFromBytes(6))
    index_list: MListBaseNode = cfield(MListBaseNode)
    index_free_node: MListBaseNode = cfield(MListBaseNode)
    index_entry : typing.List[MIndexEntryNode] = cfield(carray(10, MIndexEntryNode))

