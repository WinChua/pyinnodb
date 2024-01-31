import os
import sys
import logging
from io import BytesIO

from pyinnodb import const

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, parent_dir)

logger = logging.getLogger(__name__)


test_filename = os.path.join(parent_dir, "data", "t2.ibd")
test_filename1 = os.path.join(parent_dir, "data", "t1.ibd")
test_filename2 = test_filename

locate_filename = "/opt/homebrew/var/mysql/test/t1.ibd"
test_filename = locate_filename
