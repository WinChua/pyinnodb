# Docker 测试报告

## 测试环境

使用项目内置的 Docker 测试功能进行测试：
- **工具**: `poe dp` (devtools/deploy_mysqld.py)
- **MySQL 版本**: 5.7
- **测试表**: test_frm_all_types (包含所有数据类型)
- **测试数据**: 2 条记录

## 测试执行

### 1. 创建测试表

```bash
uv run poe dp exec --version 5.7 --sql "CREATE TABLE test_frm_all_types (...)"
```

创建了包含以下列类型的表：
- 整数类型: TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT
- 浮点类型: FLOAT, DOUBLE, DECIMAL
- 字符串类型: CHAR, VARCHAR, BINARY, VARBINARY
- 文本/BLOB类型: TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT, TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB
- 特殊类型: ENUM, SET, BIT
- 日期时间: DATE, TIME, DATETIME, TIMESTAMP, YEAR

### 2. 插入测试数据

**记录 1**: 完整数据，所有列都有值
**记录 2**: 稀疏数据，大量 NULL 值

### 3. 测试结果

#### ✅ DDL 模式 - 通过

```bash
uv run python -m pyinnodb.cli --fn datadir/5.7/test/test_frm_all_types.ibd \
  frm datadir/5.7/test/test_frm_all_types.frm --mode ddl
```

**结果**: 
- ✅ 成功生成 CREATE TABLE 语句
- ✅ 所有列类型正确识别
- ✅ ENUM 和 SET 值正确显示
- ⚠️ DATE 类型显示为 `newdate` (MySQL 内部类型，应映射为 `date`)
- ⚠️ BINARY 类型显示为 `char` (应显示为 `binary`)

#### ❌ DUMP 模式 - 失败

```bash
uv run python -m pyinnodb.cli --fn datadir/5.7/test/test_frm_all_types.ibd \
  frm datadir/5.7/test/test_frm_all_types.frm --mode dump
```

**问题**:
- ✅ 第一条记录（完整数据）: 解析正确
- ❌ 第二条记录（有 NULL 值）: 数据完全错乱

**实际数据 (MySQL)**:
```
id=2, col_tinyint=10, col_smallint=100, col_int=1000, col_bigint=10000, 
col_varchar='Second Row', col_text='More text', col_enum='one', col_set='b,d'
```

**解析结果**:
```
id=2, col_tinyint=-128, col_smallint=-32767, col_int=-1493102454, col_bigint=28288235224989696,
col_varchar="\x00\x00\x00\x00'\x10Seco", col_text='nd RowMor', col_enum=101, col_set=''
```

#### ❌ JSON 模式 - 失败

同样的问题，第二条记录数据错乱。

#### ✅ SEARCH 模式 - 部分通过

- ✅ 搜索 id=1: 完全正确
- ❌ 搜索 id=2: 数据错乱

## 问题分析

### 根本原因

通过对比测试结果和 MySQL 实际数据，发现问题出现在：

1. **NULL 值处理错误**: 
   - 当记录包含 NULL 值时，数据解析完全错乱
   - 可能是 NULL bitmap 的读取或应用逻辑有问题

2. **列顺序问题**:
   - 数据读取的顺序可能与 FRM 定义的列顺序不匹配
   - 需要检查 `get_disk_data_layout()` 的实现

3. **类型映射问题**:
   - `newdate` 应该映射为 `date`
   - `BINARY` 类型识别不正确

### 受影响的代码

**可能问题所在**:

1. **src/pyinnodb/frm/frm.py**: 
   - `to_dd_column()` 方法的类型映射
   - 没有正确处理 `DATE` 和 `BINARY` 类型

2. **src/pyinnodb/sdi/table.py**:
   - `get_disk_data_layout()` 可能返回了错误的列顺序
   - NULL 值处理逻辑

3. **src/pyinnodb/disk_struct/record.py**:
   - 记录解析时的 NULL bitmap 处理

## 与固定测试文件的对比

### tests/mysql5/all_type.{ibd,frm}

之前使用固定的测试文件时，测试都通过了：
- ✅ DDL 生成正确
- ✅ DUMP 导出正确
- ✅ JSON 导出正确
- ✅ SEARCH 查找正确

**为什么固定文件测试通过？**

可能原因：
1. 固定测试文件的数据可能都是完整的（没有 NULL 值）
2. 或者固定测试文件恰好符合当前实现的假设

## 建议修复方案

### 优先级 P0 - 必须修复

1. **修复 NULL 值处理**
   - 仔细检查 NULL bitmap 的读取和应用逻辑
   - 确保在解析记录时正确跳过 NULL 列

2. **修复列顺序问题**
   - 检查 FRM 解析时的列顺序
   - 验证与实际存储顺序的对应关系

### 优先级 P1 - 应该修复

1. **修复类型映射**
   ```python
   elif dtype == const.dd_column_type.DDColumnType.NEWDATE:
       c.column_type_utf8 = "date"  # 不是 newdate
   
   elif dtype == const.dd_column_type.DDColumnType.STRING and is_binary:
       c.column_type_utf8 = f"binary({self.length})"
   ```

2. **添加更多边缘情况测试**
   - 全 NULL 记录
   - 部分 NULL 记录
   - 不同的列组合

### 优先级 P2 - Nice to have

1. **添加单元测试**
   - 为 NULL 值处理添加专门的测试
   - 为不同数据类型组合添加测试

2. **改进错误提示**
   - 当数据解析失败时给出更明确的错误信息

## 测试重现步骤

```bash
# 1. 创建测试表
uv run poe dp exec --version 5.7 --sql "CREATE TABLE test_frm_all_types (...)"

# 2. 插入测试数据（包含 NULL 值）
uv run poe dp exec --version 5.7 --sql "INSERT INTO test_frm_all_types (...) VALUES (...)"

# 3. 测试 frm 命令
uv run python -m pyinnodb.cli --fn datadir/5.7/test/test_frm_all_types.ibd \
  frm datadir/5.7/test/test_frm_all_types.frm --mode dump

# 4. 对比 MySQL 实际数据
uv run poe dp exec --version 5.7 --sql "SELECT * FROM test_frm_all_types"
```

## 结论

Docker 测试发现了一个**严重的 bug**：

- **问题**: 当记录包含 NULL 值时，数据解析完全错误
- **影响范围**: 所有包含 NULL 值的 MySQL 5.7 表都无法正确解析
- **严重程度**: **Critical** (P0)
- **建议**: 在合并 PR 之前**必须修复**此问题

当前的实现虽然在固定测试文件上工作正常，但在实际使用场景中会有严重问题。

## 下一步

1. 立即调查 NULL 值处理逻辑
2. 修复核心问题
3. 添加包含 NULL 值的测试用例
4. 重新验证所有模式

---

**测试执行人**: AI Assistant  
**测试日期**: 2026-01-04  
**状态**: ❌ 发现严重 Bug，需要修复
