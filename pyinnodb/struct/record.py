from elftools import construct
from .meta import *


class RecordHeader(OStruct):
    info_flags = OBits(4)
    num_record_owned = OBits(4)
    order = OBits(13) ## 本page的插入序号, infimum:0, supremum: 1, 用户数据从2开始递增
    record_type = OBits(3)
    next_record_offset = SBInt16

class RecordLeaf(OStruct):
    pass
    # Cluster Key field(k)
    # Transaction ID(6)
    # Roll Pointer(7)
    # Non-key fields(j)

class RecordNonLeaf(OStruct):
    pass
    # Cluster key min. on child page(k)
    # child page num (4)

class SecondaryIndex(OStruct):
    pass
    # Secondary Key (j)
    # Clusterkey (k)

# class ORecordHeader(Struct):  ## minimum of 5 bytes, there're other dynamic field
#     info_flags = Field(Bits(4))
#     record_owned_num = Field(Bits(4))
#     order = Field(Bits(13))  # 0: infimum, 1: supremum, 2: user records
#     record_type = Field(
#         Bits(3)
#     )  # 0: conventional; 1: node pointer; 2: infinum; 3: supremum
#     next_record_offset = Field(construct.UBInt16)
