from ..mconstruct import *
from .. import const
from .undo_log import MUndoRecordInsert


class MRollbackPointer(CC):
    insert_flag: int = cfield(cs.BitsInteger(1))
    rollback_seg_id: int = cfield(cs.BitsInteger(7))
    page_number: int = cfield(cs.Int32ub)
    page_offset: int = cfield(cs.Int16ub)

    def last_version(self, undo_map):
        f = undo_map[self.rollback_seg_id]
        f.seek(self.page_number * const.PAGE_SIZE + self.page_offset)
        undo_record_header = MUndoRecordInsert.parse_stream(f)
        if self.insert_flag == 0:
            info_bits = f.read(1)
            trx_id = const.mach_u64_read_next_compressed(f)
            ptr = const.mach_u64_read_next_compressed(f)
            ptr = MRollbackPointer.parse(ptr.to_bytes(7))
            return undo_record_header, ptr
        else:
            return undo_record_header, None
        
