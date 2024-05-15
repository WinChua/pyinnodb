from .meta import *
from .fil import Fil

import logging

class IBufEntry2(OStruct):
    free_space_0 = OBits(2)
    buffer_flag_0 = OBits(1)
    change_buffer_flag_0 = OBits(1)
    free_space_1 = OBits(2)
    buffer_flag_1 = OBits(1)
    change_buffer_flag_1 = OBits(1)


class IBufPage(OStruct):
    fil = Fil
    change_buffer_bitmap = Array(4096, IBufEntry2)
