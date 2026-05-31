"""Tests for SDI (Serialized Dictionary Information) parsing.

Covers:
- MSDIPage parsing
- Table and Column dataclass construction
- DDL generation
- Column type helpers (is_number, is_var, is_string, is_big, is_int_number)
"""

from __future__ import annotations

import dataclasses
import json
from collections import namedtuple

import pytest

from pyinnodb import const
from pyinnodb.disk_struct.fsp import MFspPage
from pyinnodb.disk_struct.index import MSDIPage
from pyinnodb.disk_struct.record import MRecordHeader
from pyinnodb.sdi.column import Column, Index
from pyinnodb.sdi.table import Table
from pyinnodb.const.dd_column_type import DDColumnType


# ===========================================================================
# Helpers
# ===========================================================================


def make_sdi_page() -> MSDIPage:
    """Build a minimal (structurally valid) MSDIPage object."""
    sdi_page = MSDIPage.__new__(MSDIPage)
    sdi_page.fil = None
    sdi_page.index_header = None
    sdi_page.fseg_header = None

    class _Infimum:
        next_record_offset = 38

    class _SystemRecord:
        def get_current_offset(self):
            return 38 + MRecordHeader.sizeof() + 12

    sdi_page.system_records = type("obj", (object,), {
        "infimum": _Infimum(),
    })()
    return sdi_page


# Use hidden=0 (HT_VISIBLE) for normal columns to match ColumnHiddenType enum
MINIMAL_TABLE_DD = {
    "name": "test_table",
    "mysql_version_id": 80031,
    "created": 0,
    "last_altered": 0,
    "hidden": 0,
    "columns": [
        {
            "name": "id",
            "type": 3,  # LONG (INT)
            "is_nullable": False,
            "is_zerofill": False,
            "is_unsigned": False,
            "is_auto_increment": True,
            "is_virtual": False,
            "hidden": 0,  # HT_VISIBLE
            "ordinal_position": 0,
            "char_length": 4,
            "numeric_precision": 10,
            "numeric_scale": 0,
            "numeric_scale_null": False,
            "datetime_precision": 0,
            "datetime_precision_null": True,
            "has_no_default": False,
            "default_value_null": False,
            "srs_id_null": True,
            "srs_id": 0,
            "default_value": "",
            "default_value_utf8_null": True,
            "default_value_utf8": "",
            "default_option": "",
            "update_option": "",
            "comment": "",
            "generation_expression": "",
            "generation_expression_utf8": "",
            "options": "",
            "se_private_data": "DB_TRX_ID=6",
            "engine_attribute": "",
            "secondary_engine_attribute": "",
            "column_key": 0,
            "column_type_utf8": "int",
            "elements": [],
            "collation_id": 63,
            "is_explicit_collation": False,
        },
        {
            "name": "name",
            "type": 16,  # VARCHAR
            "is_nullable": True,
            "is_zerofill": False,
            "is_unsigned": False,
            "is_auto_increment": False,
            "is_virtual": False,
            "hidden": 0,  # HT_VISIBLE
            "ordinal_position": 1,
            "char_length": 255,
            "numeric_precision": 0,
            "numeric_scale": 0,
            "numeric_scale_null": False,
            "datetime_precision": 0,
            "datetime_precision_null": True,
            "has_no_default": False,
            "default_value_null": True,
            "srs_id_null": True,
            "srs_id": 0,
            "default_value": "",
            "default_value_utf8_null": True,
            "default_value_utf8": "",
            "default_option": "",
            "update_option": "",
            "comment": "",
            "generation_expression": "",
            "generation_expression_utf8": "",
            "options": "",
            "se_private_data": "",
            "engine_attribute": "",
            "secondary_engine_attribute": "",
            "column_key": 0,
            "column_type_utf8": "varchar(63)",
            "elements": [],
            "collation_id": 33,
            "is_explicit_collation": False,
        },
    ],
    "schema_ref": "test_db",
    "se_private_id": 0,
    "engine": "InnoDB",
    "last_checked_for_upgrade_version_id": 0,
    "comment": "test table",
    "se_private_data": "autoextend_size=0;root=4;server_version=80031",
    "engine_attribute": "",
    "secondary_engine_attribute": "",
    "row_format": 0,
    "partition_type": 0,
    "partition_expression": "",
    "partition_expression_utf8": "",
    "default_partitioning": 0,
    "subpartition_type": 0,
    "subpartition_expression": "",
    "subpartition_expression_utf8": "",
    "default_subpartitioning": 0,
    "indexes": [
        {
            "name": "PRIMARY",
            "hidden": False,
            "is_generated": False,
            "ordinal_position": 1,
            "comment": "",
            "options": "",
            "se_private_data": "",
            "type": 1,  # PRIMARY
            "algorithm": 0,
            "is_algorithm_explicit": False,
            "is_visible": True,
            "engine": "InnoDB",
            "engine_attribute": "",
            "secondary_engine_attribute": "",
            "elements": [
                {
                    "ordinal_position": 1,
                    "length": 4,
                    "order": 0,
                    "hidden": False,
                    "column_opx": 0,
                }
            ],
        }
    ],
    "foreign_keys": [],
    "check_constraints": [],
    "partitions": [],
    "collation_id": 33,
    "tablespace_ref": "",
}


# ===========================================================================
# Table construction from SDI DD
# ===========================================================================


class TestTableConstruction:
    def test_from_dict(self):
        t = Table(**MINIMAL_TABLE_DD)
        assert t.name == "test_table"
        assert t.schema_ref == "test_db"
        assert len(t.columns) == 2
        assert t.columns[0].name == "id"
        assert t.columns[1].name == "name"

    def test_private_data_parsing(self):
        t = Table(**MINIMAL_TABLE_DD)
        pd = t.private_data
        assert "root" in pd
        assert pd["root"] == "4"

    def test_engine(self):
        t = Table(**MINIMAL_TABLE_DD)
        assert t.engine == "InnoDB"

    def test_collation_id(self):
        t = Table(**MINIMAL_TABLE_DD)
        assert t.collation_id == 33

    def test_indexes_parsed(self):
        t = Table(**MINIMAL_TABLE_DD)
        assert len(t.indexes) == 1
        assert t.indexes[0].name == "PRIMARY"


# ===========================================================================
# DDL generation
# ===========================================================================


class TestGenerateDDL:
    def test_create_table_exists(self):
        t = Table(**MINIMAL_TABLE_DD)
        ddl = t.gen_ddl(schema=True)
        assert ddl is not None
        assert len(ddl) > 0

    def test_contains_create_table(self):
        t = Table(**MINIMAL_TABLE_DD)
        ddl = t.gen_ddl(schema=True)
        assert "CREATE" in ddl.upper()
        assert "TABLE" in ddl.upper()

    def test_contains_table_name(self):
        t = Table(**MINIMAL_TABLE_DD)
        ddl = t.gen_ddl(schema=True)
        # Table name should appear
        assert "test_table" in ddl or "`test_table`" in ddl

    def test_without_schema(self):
        t = Table(**MINIMAL_TABLE_DD)
        ddl = t.gen_ddl(schema=False)
        assert ddl is not None


# ===========================================================================
# Column helpers
# ===========================================================================


class TestColumnHelpers:
    def _col(self, type_val, nullable=False, unsigned=False, auto_increment=False, **kwargs):
        defaults = dict(
            name="test",
            type=type_val,
            is_nullable=nullable,
            is_zerofill=False,
            is_unsigned=unsigned,
            is_auto_increment=auto_increment,
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

    def test_is_int_number(self):
        assert DDColumnType(2).is_int_number()   # TINY
        assert DDColumnType(3).is_int_number()   # SHORT
        assert DDColumnType(4).is_int_number()   # LONG
        assert DDColumnType(9).is_int_number()   # LONGLONG
        assert DDColumnType(10).is_int_number()  # INT24
        assert not DDColumnType(5).is_int_number()  # FLOAT

    def test_is_number(self):
        assert DDColumnType.is_number(5)   # FLOAT
        assert DDColumnType.is_number(6)   # DOUBLE
        assert DDColumnType.is_number(3)   # SHORT
        assert not DDColumnType.is_number(16)  # VARCHAR

    def test_is_var(self):
        assert DDColumnType.is_var(16)   # VARCHAR

    def test_is_big(self):
        assert DDColumnType.is_big(26)   # LONG_BLOB
        assert DDColumnType.is_big(25)   # MEDIUM_BLOB
        assert DDColumnType.is_big(27)   # BLOB
        # TINY_BLOB (24) is NOT in _big_type
        assert not DDColumnType.is_big(24)  # TINY_BLOB

    def test_column_not_nullable(self):
        c = self._col(3, nullable=False)
        assert not c.is_nullable

    def test_column_nullable(self):
        c = self._col(3, nullable=True)
        assert c.is_nullable


# ===========================================================================
# Column size
# ===========================================================================


class TestColumnSize:
    @pytest.mark.parametrize(
        "col_name, expected",
        [
            ("DB_ROW_ID", 6),
            ("DB_TRX_ID", 6),
            ("DB_ROLL_PTR", 7),
        ],
    )
    def test_sys_col_size(self, col_name, expected):
        c = Column(
            name=col_name,
            type=10,
            hidden=2,
            ordinal_position=0,
            char_length=6,
            has_no_default=False,
            default_value="",
            default_value_utf8_null=True,
            collation_id=63,
            is_explicit_collation=False,
        )
        assert c.size == expected

    def test_tiny_column_size(self):
        c = Column(
            name="flag",
            type=2,  # TINY
            hidden=0,
            ordinal_position=0,
            char_length=1,
            numeric_precision=3,
            numeric_scale=0,
        )
        assert c.size == 1


# ===========================================================================
# Index helpers
# ===========================================================================


class TestIndexHelpers:
    def test_get_effect_element(self):
        idx = Index(
            name="PRIMARY",
            hidden=False,
            is_generated=False,
            ordinal_position=1,
            comment="",
            options="",
            se_private_data="",
            type=1,
            algorithm=0,
            is_algorithm_explicit=False,
            is_visible=True,
            engine="InnoDB",
        )
        idx.elements = [
            type("E", (), {"length": 4, "hidden": False, "ordinal_position": 1, "order": 0, "column_opx": 0})(),
            type("E", (), {"length": const.FFFFFFFF, "hidden": False, "ordinal_position": 2, "order": 0, "column_opx": 1})(),
        ]
        assert len(idx.get_effect_element()) == 1

    def test_get_index_type_primary(self):
        idx = Index(type=1)
        assert "PRIMARY" in idx.get_index_type()

    def test_get_index_type_unique(self):
        idx = Index(type=2)
        assert "UNIQUE" in idx.get_index_type()

    def test_get_index_type_spatial(self):
        idx = Index(type=5)
        assert "SPATIAL" in idx.get_index_type()


# ===========================================================================
# DataClass creation
# ===========================================================================


class TestDataClass:
    def test_table_has_dataclass(self):
        t = Table(**MINIMAL_TABLE_DD)
        dc = t.DataClass
        assert dc is not None

    def test_table_has_dataclass_with_hidden(self):
        t = Table(**MINIMAL_TABLE_DD)
        dc = t.DataClassHiddenCol
        assert dc is not None

    def test_dataclass_fields_include_user_columns(self):
        t = Table(**MINIMAL_TABLE_DD)
        dc = t.DataClass
        field_names = {f.name for f in dataclasses.fields(dc)}
        # Should have user-visible columns
        assert "id" in field_names or "name" in field_names

    def test_dataclass_hidden_includes_all_columns(self):
        """DataClassHiddenCol includes all columns (including system columns)."""
        t = Table(**MINIMAL_TABLE_DD)
        dc = t.DataClassHiddenCol
        # DataClassHiddenCol is a namedtuple
        assert hasattr(dc, '_fields')
        field_names = set(dc._fields)
        # User-visible columns should be included
        assert "id" in field_names


# ===========================================================================
# Keys helper on Table
# ===========================================================================


class TestTableKeys:
    def test_keys_returns_list(self):
        t = Table(**MINIMAL_TABLE_DD)
        k = t.keys()
        assert isinstance(k, (list, tuple))

    def test_keys_no_primary_excludes_pk(self):
        t = Table(**MINIMAL_TABLE_DD)
        pk_cols = {c.name for c in t.get_primary_key_col()}
        k = t.keys(no_primary=True)
        for pk in pk_cols:
            assert pk not in k
