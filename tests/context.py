from io import BytesIO
from elftools.common.utils import struct_parse
import logging
import os
import sys

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, parent_dir)


logger = logging.getLogger(__name__)


def totest(targetClass, test_case):
    for data in test_case:
        stream = BytesIO(data[0])
        testF = data[1]
        assert struct_parse(targetClass(), stream) == testF
        assert testF.build(testF) == data[0]
        assert testF.build() == data[0]


test_filename = os.path.join(parent_dir, "data", "t2.ibd")
test_filename1 = os.path.join(parent_dir, "data", "t1.ibd")
test_filename2 = test_filename
