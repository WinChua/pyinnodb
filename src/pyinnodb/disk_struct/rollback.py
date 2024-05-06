from ..mconstruct import *


class MRollbackPointer(CC):
    insert_flag: int = cfield(cs.BitsInteger(1))
    rollback_seg_id: int = cfield(cs.BitsInteger(7))
    page_number: int = cfield(cs.Int32ub)
    page_offset: int = cfield(cs.Int16ub)
