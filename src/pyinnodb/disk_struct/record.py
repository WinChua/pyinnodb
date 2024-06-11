from ..mconstruct import *


class MRecordHeader(CC):
    """
    before the record header, there are
    variable field length(1-2 bytes for every var. field) and
    nullable field bitmap(1 bit per nullable field)
    after the record header, there are datas,
    according to the record_type, data may be pointer to next page,
    infimum, supremum or the data itself
    """

    # info_flags: int = cfield(cs.BitsInteger(4))
    instant: int = cfield(cs.BitsInteger(1))
    instant_version: int = cfield(cs.BitsInteger(1))
    deleted: int = cfield(cs.BitsInteger(1))
    min_record: int = cfield(cs.BitsInteger(1))
    num_record_owned: int = cfield(cs.BitsInteger(4))  # if != 0, it's a slot
    # 本page的插入序号, infimum:0, supremum: 1, 用户数据从2开始递增
    order: int = cfield(cs.BitsInteger(13))
    record_type: int = cfield(cs.BitsInteger(3))
    next_record_offset: int = cfield(cs.Int16sb)
