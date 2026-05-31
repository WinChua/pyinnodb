"""Test fixtures and utilities for pyinnodb.

Defines:
- Test data discovery helpers
- Fixtures that provide open file handles to the bundled test IBD files
- Reusable builders for generating binary page data used by unit tests
"""

from __future__ import annotations

import io
import json
import os
import struct
from pathlib import Path
from typing import BinaryIO, NamedTuple, Sequence

def get_project_root():
    return Path(__file__).parent.parent

import pytest

from pyinnodb import const
from pyinnodb.disk_struct.fil import MFil, MFilTrailer
from pyinnodb.disk_struct.fsp import MFspPage
from pyinnodb.disk_struct.index import MIndexPage, MSDIPage
from pyinnodb.disk_struct.record import MRecordHeader
from pyinnodb.sdi.table import Table


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TESTS_DIR = Path(__file__).parent
MYSQL5_DIR = TESTS_DIR / "mysql5"
MYSQL8_DIR = TESTS_DIR / "mysql8"
DATADIR_MYSQL8_TEST = get_project_root() / "datadir/8.0.17/test"

MYSQL5_IBD = MYSQL5_DIR / "all_type.ibd"
MYSQL5_FRM = MYSQL5_DIR / "all_type.frm"
MYSQL8_IBD = DATADIR_MYSQL8_TEST / "geometry_test.ibd" if DATADIR_MYSQL8_TEST.exists() else MYSQL8_DIR / "all_type.ibd"
MYSQL8_INSTANT_IBD = DATADIR_MYSQL8_TEST / "test_for_instant.ibd" if DATADIR_MYSQL8_TEST.exists() else MYSQL8_DIR / "test_for_instant.ibd"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def read_page(f: BinaryIO, page_no: int) -> bytes:
    """Read a single 16 KiB page from *f* at *page_no*."""
    f.seek(page_no * const.PAGE_SIZE)
    return f.read(const.PAGE_SIZE)


def page_checksum_crc32c(page_data: bytes) -> int:
    """Compute the CRC32-C checksum for a full page (same algorithm InnoDB uses)."""
    return const.page_checksum_crc32c(page_data)


# ---------------------------------------------------------------------------
# Named test result containers
# ---------------------------------------------------------------------------

class TableMeta(NamedTuple):
    """Lightweight container passed to tests that need a parsed ``Table``."""
    table: Table
    ibd_path: Path


class TableAndFile(NamedTuple):
    table: Table
    f: BinaryIO


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def mysql8_ibd_path() -> Path:
    """Return the path to the MySQL 8.0 test IBD file (session-scoped)."""
    if not MYSQL8_IBD.exists():
        pytest.skip(f"test data file not found: {MYSQL8_IBD}")
    return MYSQL8_IBD


@pytest.fixture(scope="session")
def mysql8_instant_ibd_path() -> Path:
    """Return the path to the MySQL 8.0 Instant-Add-Column test IBD file."""
    if not MYSQL8_INSTANT_IBD.exists():
        pytest.skip(f"test data file not found: {MYSQL8_INSTANT_IBD}")
    return MYSQL8_INSTANT_IBD


@pytest.fixture(scope="session")
def mysql5_ibd_path() -> Path:
    if not MYSQL5_IBD.exists():
        pytest.skip(f"test data file not found: {MYSQL5_IBD}")
    return MYSQL5_IBD


@pytest.fixture(scope="session")
def mysql5_frm_path() -> Path:
    if not MYSQL5_FRM.exists():
        pytest.skip(f"test data file not found: {MYSQL5_FRM}")
    return MYSQL5_FRM


@pytest.fixture
def mysql8_ibd_file(mysql8_ibd_path: Path) -> BinaryIO:
    """Open the MySQL 8 test IBD in binary mode (function-scoped so each test gets a fresh handle)."""
    return open(mysql8_ibd_path, "rb")


@pytest.fixture
def mysql8_instant_ibd_file(mysql8_instant_ibd_path: Path) -> BinaryIO:
    return open(mysql8_instant_ibd_path, "rb")


@pytest.fixture
def parsed_mysql8(mysql8_ibd_file: BinaryIO) -> TableAndFile:
    """Parse SDI from the MySQL 8 test IBD and return (Table, file_handle)."""
    f = mysql8_ibd_file
    fsp_page = MFspPage.parse_stream(f)
    sdi_page_no = fsp_page.get_sdi_page_no_with_guess(f)
    assert sdi_page_no is not None, "SDI page not found in test IBD"

    f.seek(sdi_page_no * const.PAGE_SIZE)
    sdi_page = MSDIPage.parse_stream(f)
    dd_object = Table(**sdi_page.ddl(f, 0)["dd_object"])
    return TableAndFile(table=dd_object, f=f)


@pytest.fixture
def parsed_mysql8_instant(mysql8_instant_ibd_file: BinaryIO) -> TableAndFile:
    f = mysql8_instant_ibd_file
    fsp_page = MFspPage.parse_stream(f)
    sdi_page_no = fsp_page.sdi_page_no
    f.seek(sdi_page_no * const.PAGE_SIZE)
    sdi_page = MSDIPage.parse_stream(f)
    dd_object = Table(**sdi_page.ddl(f, 0)["dd_object"])
    return TableAndFile(table=dd_object, f=f)
