from ..mconstruct import *
from .list import MListNode, MListBaseNode
from .fil import MFil

# trx0undo.h

class MUndoHeader(CC):
    undo_page_type: int = cfield(cs.Int16ub)
    last_log_record_offset: int = cfield(cs.Int16ub)
    free_space_offset: int = cfield(cs.Int16ub)
    undo_page_list_node: MListNode = cfield(MListNode)


class MUndoSegmentHeader(CC):
    state: int = cfield(cs.Int16ub)
    last_log_offset: int = cfield(cs.Int16ub)
    fseg_entry: str = cfield(cs.Bytes(10))
    undo_page_list_node_base: MListBaseNode = cfield(MListBaseNode)

class MUndoLogHeader(CC):
    transaction_id : int = cfield(cs.Int64ub)
    transaction_number: int = cfield(cs.Int64ub)
    delete_mask_flag: int = cfield(cs.Int16ub)
    log_start_offset: int = cfield(cs.Int16ub)
    xid_flag: int = cfield(cs.Int8ub)
    ddl_transaction_flag: int = cfield(cs.Int8ub)
    table_id : int = cfield(cs.Int64ub) # if ddl_transaction
    next_undo_log_offset: int = cfield(cs.Int16ub)
    prev_undo_log_offset: int = cfield(cs.Int16ub)
    hist_list_node: MListNode = cfield(MListNode)
    # if xid flag
    xid_format: int = cfield(cs.Int32ub)
    trid_length: int = cfield(cs.Int32ub)
    bqual_length: int = cfield(cs.Int32ub)
    xid_data: str = cfield(cs.Bytes(128))
    
class MUndoLogForInsert(CC):
    next_record_offset: int = cfield(cs.Int16ub)
    flag: int = cfield(cs.Int8ub)

class MUndoPage(CC):
    fil: MFil = cfield(MFil)
    page_header: MUndoHeader = cfield(MUndoHeader)
    seg_header: MUndoSegmentHeader = cfield(MUndoSegmentHeader)
    test_log_header: MUndoLogHeader = cfield(MUndoLogHeader)
    test_undo_log: MUndoLogForInsert = cfield(MUndoLogForInsert)


