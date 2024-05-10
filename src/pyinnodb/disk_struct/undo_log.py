from ..mconstruct import *
from .list import MListNode, MListBaseNode, MPointer
from .fil import MFil
import typing
from .. import const

# trx0undo.h

class MUndoHeader(CC):
    undo_page_type: int = cfield(cs.Int16ub)
    last_log_record_offset: int = cfield(cs.Int16ub)
    free_space_offset: int = cfield(cs.Int16ub)
    undo_page_list_node: MListNode = cfield(MListNode)

class MUndoSegmentHeader(CC):
    state: int = cfield(cs.Int16ub)
    last_log_offset: int = cfield(cs.Int16ub)
    spaceid: int = cfield(cs.Int32ub)
    pointer: MPointer = cfield(MPointer)
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
    #test_undo_log: MUndoLogForInsert = cfield(MUndoLogForInsert)

class MRSEGHeader(CC):
    max_size: int = cfield(cs.Int32ub)
    history_size: int = cfield(cs.Int32ub)
    history_list_node_base: MListBaseNode = cfield(MListBaseNode)
    spaceid: int = cfield(cs.Int32ub)
    pointer: MPointer = cfield(MPointer)

class MRSEGArrayHeader(CC):
    marker: bytes = cfield(cs.Bytes(4)) #'RSEH'
    array_size: int = cfield(cs.Int32ub)
    spaceid: int = cfield(cs.Int32ub)
    pointer: MPointer = cfield(MPointer)
    pagenos: typing.List[int] = cfield(carray(128, cs.Int32ub))
    # reserve_space: bytes = cfield(cs.Bytes(200)) ## at the end

class MRSEGArrayPage(CC):
    fil: MFil = cfield(MFil)
    header: MRSEGArrayHeader = cfield(MRSEGArrayHeader)

class MRSEGPage(CC):
    fil: MFil = cfield(MFil)
    header: MRSEGHeader = cfield(MRSEGHeader)
    slots: typing.List[int] = cfield(carray(1024, cs.Int32ub))

TRX_UNDO_MODIFY_BLOB = 64
TRX_UNDO_UPD_EXIST_REC = 12
TRX_UNDO_UPD_DEL_REC = 13
TRX_UNDO_DEL_MARK_REC = 14

class MUndoRecordInsert(CC):
    # prev_record_offset: int = cfield(cs.Int16ub)
    next_record_offset: int = cfield(cs.Int16ub)
    flag: int = cfield(cs.Int8ub)
    undo_number: int = cfield(cs.Bytes(0))
    table_id: int = cfield(cs.Bytes(0))

    def _post_parsed(self, stream, context, path):
        # trx_undo_rec_get_undo_no
        if self.flag & TRX_UNDO_MODIFY_BLOB > 0:
            assert stream.read(1) == b'\x00'

        self.undo_number = const.read_compressed_mysql_int(stream)
        self.table_id = const.read_compressed_mysql_int(stream)
