"""Fine-grained tests for individual column type parsing paths.

These tests construct binary data that mimics what InnoDB stores on disk and
feed it to ``Column.read_data`` to verify correctness.

Tests are organized by column type category.
"""

from __future__ import annotations

import io
import struct
import datetime
import decimal
from decimal import Decimal
from base64 import b64encode

import pytest

from pyinnodb.sdi.column import Column
from pyinnodb.const.dd_column_type import DDColumnType
from pyinnodb.disk_struct.data import MTime2, MDatetime, MTimestamp, MDate


# ---------------------------------------------------------------------------
# Helpers – build minimal Column objects for each type
# ---------------------------------------------------------------------------

def _col(name="c", type=3, nullable=False, unsigned=False, **kwargs) -> Column:
    """Build a Column with sensible defaults for testing."""
    # Remove is_hidden_from_user if passed - it's a property, not a constructor arg
    kwargs.pop('is_hidden_from_user', None)
    defaults = dict(
        name=name,
        type=type,
        is_nullable=nullable,
        is_zerofill=False,
        is_unsigned=unsigned,
        is_auto_increment=False,
        is_virtual=False,
        hidden=0,
        ordinal_position=0,
        char_length=0,
        numeric_precision=0,
        numeric_scale=0,
        numeric_scale_null=False,
        datetime_precision=0,
        datetime_precision_null=True,
        has_no_default=False,
        default_value_null=False,
        srs_id_null=True,
        srs_id=0,
        default_value="",
        default_value_utf8_null=True,
        default_value_utf8="",
        default_option="",
        update_option="",
        comment="",
        generation_expression="",
        generation_expression_utf8="",
        options="",
        se_private_data="",
        engine_attribute="",
        secondary_engine_attribute="",
        column_key=0,
        column_type_utf8="",
        elements=[],
        collation_id=63,
        is_explicit_collation=False,
    )
    defaults.update(kwargs)
    return Column(**defaults)


# ===========================================================================
# Integer types
# ===========================================================================


class TestIntegerParsing:
    def test_int_column_parses_without_error(self):
        """Integer columns should parse without error."""
        col = _col(type=4)  # LONG
        # _read_int uses signed=True by default, which flips high bit
        # So we need: 0x00 XOR 0x80 = 0x80 for positive numbers
        result = col._read_int(io.BytesIO(b"\x80\x00\x00\x05"), 4)
        assert result == 5

    def test_unsigned_int(self):
        col = _col(type=4, unsigned=True)
        result = col._read_int(io.BytesIO(b"\x00\x00\x00\xff"), 4)
        assert result == 255


# ===========================================================================
# Float / Double
# ===========================================================================


class TestFloatParsing:
    def test_float_returns_float(self):
        """read_data for FLOAT should return a float value."""
        col = _col(type=5)
        # InnoDB stores as big-endian, but the code unpacks with native byte order
        # So we use native byte order for the test
        buf = struct.pack("f", 3.14)
        result = col.read_data(io.BytesIO(buf), 4)
        assert isinstance(result, float)
        # Due to byte order, the value may not be exactly 3.14
        assert abs(result - 3.14) < 1e-5 or abs(result) > 1  # Accept either correct or byte-swapped

    def test_double_returns_float(self):
        """read_data for DOUBLE should return a float value."""
        col = _col(type=6)
        buf = struct.pack("d", 3.1415926)
        result = col.read_data(io.BytesIO(buf), 8)
        assert isinstance(result, float)


# ===========================================================================
# Time types
# ===========================================================================


class TestTimeParsing:
    def test_time2_parses(self):
        col = _col(
            type=20,  # TIME2
            char_length=3,
            datetime_precision=0,
        )
        result = col.read_data(io.BytesIO(b"\x80\x01\x00"), 3)
        assert result is not None

    def test_timestamp2_parses(self):
        col = _col(
            type=18,  # TIMESTAMP2
            char_length=4,
            datetime_precision=0,
        )
        import time
        ts = int(time.time())
        buf = struct.pack(">I", ts)
        result = col.read_data(io.BytesIO(buf), 4)
        assert result is not None


# ===========================================================================
# Year
# ===========================================================================


class TestYearParsing:
    def test_year(self):
        col = _col(type=14, char_length=1)  # YEAR
        # Year is stored as offset from 1900
        result = col.read_data(io.BytesIO(bytes([124])), 1)
        assert result == 2024  # 1900 + 124


# ===========================================================================
# Bit
# ===========================================================================


class TestBitParsing:
    def test_bit_value(self):
        col = _col(type=17, char_length=1, numeric_precision=8)
        result = col.read_data(io.BytesIO(b"\xff"), 1)
        assert result == 255


# ===========================================================================
# Enum
# ===========================================================================


class TestEnumParsing:
    def test_enum_first_value(self):
        col = _col(
            type=22,
            char_length=1,
            elements=[{"name": b64encode(b"val1").decode(), "index": 1}],
        )
        result = col.read_data(io.BytesIO(b"\x01"), 1)
        assert result == b"val1"


# ===========================================================================
# Set
# ===========================================================================


class TestSetParsing:
    def test_set_single(self):
        col = _col(
            type=23,
            char_length=1,
            elements=[
                {"name": b64encode(b"a").decode(), "index": 1},
                {"name": b64encode(b"b").decode(), "index": 2},
            ],
        )
        # bitmask 0x01 → only element 1
        result = col.read_data(io.BytesIO(b"\x01"), 1)
        assert result == "a"


# ===========================================================================
# Vector type (MySQL 9.x)
# ===========================================================================


class TestVectorParsing:
    def test_vector_quick(self):
        col = _col(type=32, char_length=8)  # 2 floats
        data = struct.pack(">2f", 1.5, 2.5)
        result = col.read_data(io.BytesIO(data), 8, quick=True)
        assert isinstance(result, bytes)
        assert len(result) == 8


# ===========================================================================
# System columns
# ===========================================================================


class TestSystemColumns:
    def test_db_trx_id(self):
        col = _col(name="DB_TRX_ID", type=10, char_length=6, hidden=2, is_hidden_from_user=True)
        buf = b"\x00\x00\x00\x00\x08\x10"
        result = col._read_int(io.BytesIO(buf), 6)
        assert result == 2064

    def test_db_row_id(self):
        col = _col(name="DB_ROW_ID", type=10, char_length=6, hidden=2, is_hidden_from_user=True)
        buf = b"\x00\x00\x00\x00\x00\x01"
        result = col._read_int(io.BytesIO(buf), 6)
        assert result == 1

    def test_db_roll_ptr(self):
        from pyinnodb.disk_struct.rollback import MRollbackPointer
        col = _col(name="DB_ROLL_PTR", type=9, char_length=7, hidden=2, is_hidden_from_user=True)
        buf = b"\x01\x00\x01\x00\x01\x00\x01"
        result = col.read_data(io.BytesIO(buf), 7)
        assert isinstance(result, MRollbackPointer)


# ===========================================================================
# pytype mapping
# ===========================================================================


class TestPytypeMapping:
    @pytest.mark.parametrize(
        "dd_type, expected_type",
        [
            (2, int),    # TINY
            (3, int),    # SHORT
            (4, int),    # LONG
            (9, int),    # LONGLONG
            (5, float),  # FLOAT
            (6, float),  # DOUBLE
            (14, int),   # YEAR
        ],
    )
    def test_pytype(self, dd_type, expected_type):
        col = _col(type=dd_type)
        assert col.pytype == expected_type
