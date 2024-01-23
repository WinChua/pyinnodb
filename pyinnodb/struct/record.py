from ..mconstruct import *


class MRecordHeader(CC):
    info_flags: int = cfield(cs.BitsInteger(4))
    num_record_owned: int = cfield(cs.BitsInteger(4))
    # 本page的插入序号, infimum:0, supremum: 1, 用户数据从2开始递增
    order: int = cfield(cs.BitsInteger(13))
    record_type: int = cfield(cs.BitsInteger(3))
    next_record_offset: int = cfield(cs.Int16sb)
