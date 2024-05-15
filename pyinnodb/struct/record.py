from elftools import construct
from .metaclass import Struct, Field, Bits


class RecordHeader(Struct):  ## minimum of 5 bytes, there're other dynamic field
    info_flags = Field(Bits(4))
    record_owned_num = Field(Bits(4))
    order = Field(Bits(13))  # 0: infimum, 1: supremum, 2: user records
    record_type = Field(
        Bits(3)
    )  # 0: conventional; 1: node pointer; 2: infinum; 3: supremum
    next_record_offset = Field(construct.UBInt16)
