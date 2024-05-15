import os
import sys
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, parent_dir) 

import logging
logger = logging.getLogger(__name__)

from elftools.common.utils import struct_parse

from io import BytesIO

def totest(targetClass, test_case):
    # idx = 4
    # logger.info(str(test_fil_case[idx][1]))
    # logger.info(struct_parse(Fil(), BytesIO(test_fil_case[idx][0])))
    # return
    for data in test_case:
        stream = BytesIO(data[0])
        testF = data[1]
        assert struct_parse(targetClass(), stream) == testF
        assert testF.build(testF) == data[0]
        assert testF.build() == data[0]

