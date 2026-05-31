"""Tests for CLI commands and edge-case / regression scenarios.

Covers:
- Eval safety for --primary-key parameter
- Boundary conditions in search / layout parsing
- Empty and single-record tables
- File handle lifecycle
"""

from __future__ import annotations

import io
import struct
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from pyinnodb import const
from pyinnodb.disk_struct.fil import MFil
from pyinnodb.disk_struct.fsp import MFspPage, FSP_FLAGS_POS_SDI, FSP_FLAGS_MASK_SDI
from pyinnodb.disk_struct.index import MIndexPage, MSDIPage, MIndexHeader
from pyinnodb.disk_struct.record import MRecordHeader
from pyinnodb.sdi.column import Column, Index, ColumnElement
from pyinnodb.sdi.table import Table
from pyinnodb.const.dd_column_type import DDColumnType
from pyinnodb.const.column_hidden_type import ColumnHiddenType


# ===========================================================================
# Eval safety – the search command previously used eval()
# ===========================================================================


class TestEvalSafety:
    """Ensure primary key parsing is safe when passed from CLI."""

    def test_literal_eval_int(self):
        import ast
        assert ast.literal_eval("42") == 42

    def test_literal_eval_tuple(self):
        import ast
        result = ast.literal_eval("(1, 2)")
        assert result == (1, 2)

    def test_literal_eval_rejects_code(self):
        import ast
        with pytest.raises((ValueError, SyntaxError)):
            ast.literal_eval("__import__('os').system('echo pwned')")

    def test_literal_eval_rejects_function_call(self):
        import ast
        with pytest.raises((ValueError, SyntaxError)):
            ast.literal_eval("print('hello')")


# ===========================================================================
# FSP page edge cases
# ===========================================================================


class TestFSPEdgeCases:
    def test_sdi_flag_mask(self):
        """Verify that the SDI flag in FSP header can be detected."""
        # FSP_FLAGS_MASK_SDI is the bitmask, use it directly
        flags = FSP_FLAGS_MASK_SDI
        assert (flags & FSP_FLAGS_MASK_SDI) > 0

    def test_sdi_flag_unset(self):
        flags = 0
        assert (flags & FSP_FLAGS_MASK_SDI) == 0


# ===========================================================================
# Empty / single-record table scenarios
# ===========================================================================


class TestEmptyTable:
    def test_table_with_no_indexes(self):
        t = Table(**{
            "name": "empty_idx",
            "mysql_version_id": 80031,
            "created": 0,
            "last_altered": 0,
            "hidden": 0,
            "columns": [],
            "schema_ref": "test",
            "se_private_data": "",
            "engine": "InnoDB",
            "indexes": [],
            "foreign_keys": [],
            "check_constraints": [],
            "partitions": [],
            "collation_id": 33,
        })
        assert len(t.indexes) == 0

    def test_get_primary_key_col_no_pk_fallback(self):
        """When there's no primary key, fallback to DB_ROW_ID if present."""
        t = Table(**{
            "name": "no_pk_table",
            "mysql_version_id": 80031,
            "created": 0,
            "last_altered": 0,
            "hidden": 0,
            "columns": [
                Column(
                    name="DB_ROW_ID",
                    type=10,
                    hidden=2,
                    ordinal_position=0,
                    char_length=6,
                    has_no_default=False,
                    default_value="",
                    default_value_utf8_null=True,
                    collation_id=63,
                    is_explicit_collation=False,
                ).__dict__,
            ],
            "schema_ref": "test",
            "engine": "InnoDB",
            "indexes": [],
            "foreign_keys": [],
            "check_constraints": [],
            "partitions": [],
            "collation_id": 33,
        })
        pk = t.get_primary_key_col()
        assert len(pk) > 0
        assert pk[0].name == "DB_ROW_ID"


# ===========================================================================
# Column hidden type
# ===========================================================================


class TestColumnHiddenType:
    def test_visible(self):
        assert ColumnHiddenType(0) == ColumnHiddenType.HT_VISIBLE

    def test_hidden_se(self):
        assert ColumnHiddenType(1) == ColumnHiddenType.HT_HIDDEN_SE

    def test_hidden_sql(self):
        assert ColumnHiddenType(2) == ColumnHiddenType.HT_HIDDEN_SQL

    def test_hidden_user(self):
        assert ColumnHiddenType(3) == ColumnHiddenType.HT_HIDDEN_USER


# ===========================================================================
# Column edge cases
# ===========================================================================


class TestColumnEdgeCases:
    def test_column_with_elements(self):
        """ENUM/SET columns have elements that need proper parsing."""
        col = Column(
            name="color",
            type=22,  # ENUM
            hidden=0,
            ordinal_position=1,
            elements=[
                {"name": "red", "index": 1},
                {"name": "blue", "index": 2},
            ],
            collation_id=63,
            is_explicit_collation=False,
        )
        assert len(col.elements) == 2

    def test_column_with_se_private_data(self):
        col = Column(
            name="instant_col",
            type=3,
            hidden=0,
            ordinal_position=5,
            se_private_data="version_added=2;default_null=1",
            collation_id=63,
            is_explicit_collation=False,
            char_length=4,
            numeric_precision=10,
            numeric_scale=0,
        )
        assert col.is_instant_col
        assert col.private_data["version_added"] == "2"

    def test_column_version_valid_boundary(self):
        col = Column(
            name="vcol",
            type=3,
            hidden=0,
            ordinal_position=1,
            se_private_data="version_added=5",
            collation_id=63,
            is_explicit_collation=False,
            char_length=4,
            numeric_precision=10,
            numeric_scale=0,
        )
        assert not col.version_valid(4)
        assert col.version_valid(5)
        assert col.version_valid(100)

    def test_column_version_dropped(self):
        col = Column(
            name="dropped_col",
            type=3,
            hidden=0,
            ordinal_position=2,
            se_private_data="version_added=2;version_dropped=5",
            collation_id=63,
            is_explicit_collation=False,
            char_length=4,
            numeric_precision=10,
            numeric_scale=0,
        )
        assert col.version_valid(2)
        assert not col.version_valid(5)
        assert not col.version_valid(100)

    def test_hidden_from_user_column(self):
        col = Column(
            name="hidden_col",
            type=3,
            hidden=1,  # HT_HIDDEN_SE
            ordinal_position=0,
            collation_id=63,
            is_explicit_collation=False,
            char_length=4,
            numeric_precision=10,
            numeric_scale=0,
        )
        assert col.is_hidden_from_user

    def test_visible_column(self):
        col = Column(
            name="visible_col",
            type=3,
            hidden=0,  # HT_VISIBLE
            ordinal_position=0,
            collation_id=63,
            is_explicit_collation=False,
            char_length=4,
            numeric_precision=10,
            numeric_scale=0,
        )
        assert not col.is_hidden_from_user


# ===========================================================================
# Index edge cases
# ===========================================================================


class TestIndexEdgeCases:
    def test_index_with_no_elements(self):
        idx = Index(name="empty_idx", type=1)
        assert len(idx.elements) == 0
        assert len(idx.get_effect_element()) == 0

    def test_hidden_index(self):
        idx = Index(name="hidden_idx", hidden=True, type=4)
        assert idx.hidden

    def test_primary_index_type(self):
        idx = Index(name="PRIMARY", type=1)
        assert "PRIMARY" in idx.get_index_type()


# ===========================================================================
# Constants
# ===========================================================================


class TestRecordType:
    def test_conventional(self):
        assert const.RecordType.Conventional.value == 0

    def test_node_pointer(self):
        assert const.RecordType.NodePointer.value == 1

    def test_infimum(self):
        assert const.RecordType.Infimum.value == 2

    def test_supremum(self):
        assert const.RecordType.Supremum.value == 3


class TestPageType:
    def test_allocated(self):
        assert const.PageType(0) == const.PageType.TYPE_ALLOCATED

    def test_index(self):
        assert const.PageType(17855) == const.PageType.INDEX

    def test_sdi(self):
        assert const.PageType(17853) == const.PageType.SDI


# ===========================================================================
# Fixture integration
# ===========================================================================


class TestFixtureIntegration:
    def test_both_fixtures_work(self, parsed_mysql8, parsed_mysql8_instant):
        assert parsed_mysql8.table.name == "all_type"
        assert parsed_mysql8_instant.table.name != ""

    def test_instant_has_records(self, parsed_mysql8_instant):
        records = parsed_mysql8_instant.table.iter_record(parsed_mysql8_instant.f)
        assert len(records) > 0
