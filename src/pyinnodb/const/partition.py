from enum import Enum

class PartitionType(Enum):
     PT_NONE = 0                                                                                                                                                               
     PT_HASH = 1
     PT_KEY_51 = 2
     PT_KEY_55 = 3
     PT_LINEAR_HASH = 4
     PT_LINEAR_KEY_51 = 5
     PT_LINEAR_KEY_55 = 6
     PT_RANGE = 7
     PT_LIST = 8
     PT_RANGE_COLUMNS = 9
     PT_LIST_COLUMNS = 10
     PT_AUTO = 11
     PT_AUTO_LINEAR = 12

class SubpartitionType(Enum):
    ST_NONE = 0
    ST_HASH = 1
    ST_KEY_51 = 2
    ST_KEY_55 = 3
    ST_LINEAR_HASH = 4
    ST_LINEAR_KEY_51 = 5
    ST_LINEAR_KEY_55 = 6
