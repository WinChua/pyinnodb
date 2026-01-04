# Docker 测试总结

## 📋 快速概览

| 测试项 | 状态 | 说明 |
|--------|------|------|
| DDL 模式 | ✅ 通过 | 表结构识别正确 |
| DUMP 模式 - 完整数据 | ✅ 通过 | 无 NULL 值的记录正确 |
| DUMP 模式 - 稀疏数据 | ❌ **失败** | 包含 NULL 值的记录完全错乱 |
| JSON 模式 - 稀疏数据 | ❌ **失败** | 同上 |
| SEARCH 模式 - 完整数据 | ✅ 通过 | 查找无 NULL 值记录正确 |
| SEARCH 模式 - 稀疏数据 | ❌ **失败** | 查找有 NULL 值记录错误 |

## 🔴 严重问题

### 问题描述
**当记录包含 NULL 值时，数据解析完全错误**

### 实际案例

**MySQL 中的真实数据** (id=2):
```sql
col_tinyint=10
col_smallint=100
col_int=1000
col_bigint=10000
col_varchar='Second Row'
col_text='More text'
col_enum='one'
col_set='b,d'
```

**PyInnoDB 解析结果**:
```python
col_tinyint=-128       # 应该是 10
col_smallint=-32767    # 应该是 100
col_int=-1493102454    # 应该是 1000
col_bigint=28288235... # 应该是 10000
col_varchar="\x00..."  # 应该是 'Second Row'
col_text='nd RowMor'   # 应该是 'More text'
col_enum=101           # 应该是 'one'
col_set=''             # 应该是 'b,d'
```

**数据完全错乱！**

## 🎯 根本原因

1. **NULL bitmap 处理错误**
   - 没有正确识别哪些列是 NULL
   - 没有正确跳过 NULL 列的读取

2. **为什么之前没发现**
   - 固定测试文件 `tests/mysql5/all_type.{ibd,frm}` 的数据可能都是完整的
   - 没有包含 NULL 值的测试用例

## 📊 测试环境

- **MySQL 版本**: 5.7
- **测试工具**: Docker (通过 `uv run poe dp` 管理)
- **测试表**: test_frm_all_types (29 列，覆盖所有数据类型)
- **测试数据**: 2 条记录
  - 记录 1: 所有列都有值
  - 记录 2: 大量 NULL 值

## 🔧 重现步骤

```bash
# 1. 查看运行中的 MySQL 容器
uv run poe dp list

# 2. 在 MySQL 5.7 中创建测试表（包含 NULL 值的数据）
uv run poe dp exec --version 5.7 --sql "INSERT INTO test_frm_all_types (...) VALUES (...)"

# 3. 使用 frm 命令解析
uv run python -m pyinnodb.cli --fn datadir/5.7/test/test_frm_all_types.ibd \
  frm datadir/5.7/test/test_frm_all_types.frm --mode dump

# 4. 对比 MySQL 实际数据
uv run poe dp exec --version 5.7 --sql "SELECT * FROM test_frm_all_types WHERE id=2"
```

## 📁 相关文件

- **详细报告**: `DOCKER_TEST_REPORT.md` (5.7KB)
- **测试数据位置**: `datadir/5.7/test/test_frm_all_types.{ibd,frm}`
- **问题代码**: 
  - `src/pyinnodb/frm/frm.py` - FRM 解析
  - `src/pyinnodb/sdi/table.py` - 数据读取逻辑
  - `src/pyinnodb/disk_struct/record.py` - 记录解析

## 💡 建议

### ⚠️ 合并前必须修复

这不是一个小 bug，而是**核心功能的严重缺陷**：

1. **影响范围**: 所有包含 NULL 值的 MySQL 5.7 表
2. **严重程度**: Critical (P0)
3. **实际影响**: 数据恢复场景下会导致数据错误/损坏

### 📝 修复建议

1. **立即修复 NULL 值处理逻辑**
   - 检查 NULL bitmap 的读取
   - 检查列的实际存储顺序

2. **添加测试用例**
   - 包含 NULL 值的记录
   - 各种 NULL 值组合

3. **重新验证**
   - 使用 Docker 测试实际场景
   - 测试多个 MySQL 版本

## 🔗 查看详细报告

```bash
cat DOCKER_TEST_REPORT.md
```

---

**发现者**: AI Assistant  
**测试日期**: 2026-01-04  
**严重程度**: 🔴 Critical (P0)  
**建议**: ❌ 不建议合并，需要先修复
