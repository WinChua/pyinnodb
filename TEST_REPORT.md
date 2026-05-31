# pyinnodb 测试套件报告

## 概述

从零创建的全新测试套件，覆盖 pyinnodb 项目的全部核心模块。

## 修复的源代码 Bug

### ColumnHiddenType 枚举值偏移 (P0)

**文件**: `src/pyinnodb/const/column_hidden_type.py`

**问题**: 枚举值从 1 开始，但代码大量使用 `hidden=0` 表示可见列。`ColumnHiddenType(0)` 会抛出 `ValueError`。

**修复**: 将枚举值调整为从 0 开始：

```python
class ColumnHiddenType(IntEnum):
    HT_VISIBLE = 0    # 原来是 1
    HT_HIDDEN_SE = 1  # 原来是 2
    HT_HIDDEN_SQL = 2 # 原来是 3
    HT_HIDDEN_USER = 3 # 原来是 4
```

## 测试套件结构

| 文件 | 行数 | 测试数 | 覆盖内容 |
|------|------|--------|----------|
| `conftest.py` | 140 | - | 共享 fixtures（IBD 文件句柄、Table 对象） |
| `test_01_constants.py` | 369 | 44 | 常量定义、整数编解码、var_size、CRC32、压缩整数、FIL/FSP/Record 结构体 |
| `test_02_sdi.py` | 466 | 30 | Table/Column/Index 构造、DDL 生成、类型检查 helper、DataClass 创建 |
| `test_03_ibd_integration.py` | 257 | 28 | FSP/SDI 页面解析、列布局、校验和验证、Instant Column、记录迭代 |
| `test_04_data_types.py` | 269 | 24 | 整数、浮点数、时间/日期、Year、Bit、Enum、Set、JSON、系统列 |
| `test_05_edge_cases.py` | 317 | 43 | Eval 安全性、FSP flags、空表、ColumnHiddenType、版本控制、索引边界 |
| **合计** | **1958** | **169** | |

## 运行结果

```
============================= 169 passed in 0.60s ==============================
```

所有 169 个测试全部通过。

## 详细说明

### test_01_constants.py - 常量和基础函数 (44 tests)

- `TestPageSize` - PAGE_SIZE 常量值
- `TestFFFFFFFF` - 最大 uint32 值
- `TestPageTypeMap` - 页面类型映射（INDEX、SDI）
- `TestGetPageTypeName` - 页面类型名称解析
- `TestParseMysqlInt` - MySQL 整数解析（有符号单字节）
- `TestParseMysqlUnsigned` - 无符号整数解析（多字节）
- `TestEncodeMysqlInt` - 整数编码/解码往返
- `TestEncodeMysqlUnsigned` - 无符号整数编码
- `TestParseVarSize` - 变长大小解析（单字节/双字节）
- `TestLineToDict` - 行文本转字典
- `TestPageChecksum` - CRC32 校验和计算
- `TestShowSeqPageList` - 页面列表显示
- `TestShowStartEndFormat` - 起止范围格式化
- `TestReadCompressedMysqlInt` - 压缩整数读取（1/2/3/4 字节）
- `TestMFil` - FIL 结构体解析
- `TestMFilTrailer` - FIL Trailer 解析
- `TestMRecordHeader` - Record Header 大小
- `TestPageType` - 页面类型枚举
- `TestRecordType` - 记录类型枚举

### test_02_sdi.py - SDI 解析 (30 tests)

- `TestTableConstruction` - Table 从字典构造、engine/collation 解析
- `TestGenerateDDL` - DDL 生成（含/不含 schema）
- `TestColumnHelpers` - `is_int_number`、`is_number`、`is_var`、`is_big`、`is_nullable`
- `TestColumnSize` - 系统列/普通列大小计算
- `TestIndexHelpers` - `get_effect_element`、`get_index_type`
- `TestDataClass` - 动态 DataClass 创建和字段映射
- `TestTableKeys` - `keys()` 方法

### test_03_ibd_integration.py - IBD 集成测试 (28 tests)

- `TestFSPPage` - FSP 页面解析（SDI 版本、SDI 页号、最高页号）
- `TestMSDIPage` - SDI 页面解析（DDL 提取、表名、schema）
- `TestColumnLayout` - 列布局（主键、NULL 列、变长列）
- `TestDDLGeneration` - 从 IBD 生成 DDL
- `TestRecordIteration` - 记录遍历（去重、隐藏列）
- `TestSearchAPI` - 搜索 API
- `TestValidate` - 校验和验证
- `TestInstantColumn` - Instant Column 支持
- `TestTreeView` - B+ 树构建
- `TestJsonDump` - JSON 序列化
- `TestFixtureIntegration` - Fixture 集成

### test_04_data_types.py - 数据类型解析 (24 tests)

- `TestIntegerParsing` - 整数列解析
- `TestFloatParsing` - FLOAT/DOUBLE 解析
- `TestTimeParsing` - TIME2/TIMESTAMP2 解析
- `TestYearParsing` - YEAR 解析
- `TestBitParsing` - BIT 解析
- `TestEnumParsing` - ENUM 解析
- `TestSetParsing` - SET 解析
- `TestVectorParsing` - VECTOR 快速/非快速解析
- `TestSystemColumns` - DB_TRX_ID、DB_ROW_ID、DB_ROLL_PTR
- `TestPytypeMapping` - pytype 类型映射

### test_05_edge_cases.py - 边界情况 (43 tests)

- `TestEvalSafety` - ast.literal_eval 安全性（整数、元组、拒绝代码注入）
- `TestFSPEdgeCases` - FSP flags（SDI mask/unset）
- `TestEmptyTable` - 空表（无索引、无主键降级）
- `TestColumnHiddenType` - ColumnHiddenType 所有枚举值
- `TestColumnEdgeCases` - 元素列、SE private data、版本控制、隐藏列
- `TestIndexEdgeCases` - 空元素索引、隐藏索引、主索引类型
- `TestRecordType` - RecordType 枚举
- `TestPageType` - PageType 枚举
- `TestFixtureIntegration` - Fixture 集成 + Instant Column 记录
