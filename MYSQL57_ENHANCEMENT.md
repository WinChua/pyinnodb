# MySQL 5.7 Support Enhancement

## 概述
本次更新大幅增强了 PyInnoDB 对 MySQL 5.7 及更早版本的支持，使其功能与 MySQL 8.0+ 的支持水平保持一致。

## 更新内容

### 1. 完善的 FRM 文件解析 (`src/pyinnodb/frm/frm.py`)

#### 增强的列类型识别
扩展了 `MFrmColumn.to_dd_column()` 方法，现在能够为所有 MySQL 数据类型生成完整的 `column_type_utf8` 字段：

**支持的数据类型：**
- **整数类型**: TINYINT, SMALLINT, INT, MEDIUMINT, BIGINT
- **浮点类型**: FLOAT, DOUBLE, DECIMAL/NUMERIC
- **日期时间**: DATE, TIME, DATETIME, TIMESTAMP, YEAR
- **字符串类型**: CHAR, VARCHAR
- **二进制类型**: BINARY, VARBINARY (通过 VARCHAR 标识)
- **文本/BLOB**: TINYTEXT/TINYBLOB, TEXT/BLOB, MEDIUMTEXT/MEDIUMBLOB, LONGTEXT/LONGBLOB
- **特殊类型**: BIT, ENUM, SET

#### 类型判断逻辑
通过 `FieldFlag.BLOB` 标志位正确区分 TEXT 和 BLOB 类型，例如：
- `TINY_BLOB` 类型 + BLOB 标志 = `tinyblob`
- `TINY_BLOB` 类型 - BLOB 标志 = `tinytext`

### 2. 重构的 FRM 命令 (`src/pyinnodb/cli/frm.py`)

完全重写了 `frm` 命令，添加了多种操作模式：

#### 支持的模式

##### **DDL 模式** (`--mode ddl`)
生成 CREATE TABLE 语句，展示完整的表结构定义

```bash
uv run python -m pyinnodb.cli --fn tests/mysql5/all_type.ibd frm tests/mysql5/all_type.frm --mode ddl
```

**输出示例：**
```sql
CREATE TABLE `test`.`table` (
  `id` int NOT NULL,
  `BIGINT` bigint DEFAULT NULL,
  `ENUM` enum('hello','world','a') DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci
```

##### **DUMP 模式** (`--mode dump`)
将数据导出为 INSERT 语句，可直接导入其他 MySQL 实例

```bash
uv run python -m pyinnodb.cli --fn tests/mysql5/all_type.ibd frm tests/mysql5/all_type.frm --mode dump
```

**输出示例：**
```sql
INSERT INTO `test`.`table`(id,BIGINT,BIT,...) VALUES (1,98283201,1,...);
INSERT INTO `test`.`table`(id,BIGINT,BIT,...) VALUES (2,98283201,1,...);
```

##### **JSON 模式** (`--mode json`)
以 JSON 格式导出数据，便于程序化处理

```bash
uv run python -m pyinnodb.cli --fn tests/mysql5/all_type.ibd frm tests/mysql5/all_type.frm --mode json
```

**输出示例：**
```json
[
  {
    "id": 1,
    "BIGINT": 98283201,
    "BIT": 1,
    "DATETIME": "2024-01-01 09:00:01",
    ...
  }
]
```

##### **SEARCH 模式** (`--mode search`)
通过主键快速查找特定记录

```bash
uv run python -m pyinnodb.cli --fn tests/mysql5/all_type.ibd frm tests/mysql5/all_type.frm --mode search --primary-key 1
```

**输出示例：**
```
Found: table(id=1, BIGINT=98283201, BIT=1, ...)
```

#### 新增选项

| 选项 | 类型 | 默认值 | 说明 |
|-----|-----|-------|------|
| `--mode` | choice | dump | 操作模式：ddl/dump/json/search |
| `--schema/--no-schema` | bool | True | DDL 中是否包含 schema 名称 |
| `--primary-key` | string | "" | search 模式下的主键值 |
| `--hidden-col/--no-hidden-col` | bool | False | 是否显示隐藏列（DB_TRX_ID, DB_ROLL_PTR） |
| `--root-page` | int | 3 | 根页编号（MySQL 5.7 默认为 3） |

### 3. 改进的默认值设置

为从 FRM 解析的表对象设置合理的默认值：
- `schema_ref`: "test" (默认数据库名)
- `engine`: "InnoDB" (存储引擎)
- `collation_id`: 33 (utf8mb3_general_ci，MySQL 5.7 常见默认值)

## 功能对比

| 功能 | MySQL 8.0+ (SDI) | MySQL 5.7 (FRM) | 状态 |
|-----|-----------------|----------------|------|
| 解析表结构 | ✅ | ✅ | 完成 |
| 生成 DDL | ✅ | ✅ | 完成 |
| 导出 INSERT 语句 | ✅ | ✅ | 完成 |
| 导出 JSON | ✅ | ✅ | 完成 |
| 主键搜索 | ✅ | ✅ | 完成 |
| 显示隐藏列 | ✅ | ✅ | 完成 |
| 数据校验 | ✅ | ⚠️ | 需要独立实现 |
| Undo Log 历史 | ✅ | ⚠️ | 需要独立实现 |

## 已知限制

1. **元数据限制**: FRM 文件不包含列注释、AUTO_INCREMENT 等信息，导出的 DDL 缺少这些细节
2. **默认值**: FRM 中的默认值解析较为复杂，当前实现可能不完整
3. **TEXT/BLOB 区分**: 基于 BLOB 标志位判断，但某些边缘情况可能存在误判
4. **字符集信息**: 从 FRM 解析的字符集信息有限，使用了固定的默认值

## 测试验证

所有功能已通过测试数据验证：

```bash
# DDL 生成测试
uv run python -m pyinnodb.cli --fn tests/mysql5/all_type.ibd frm tests/mysql5/all_type.frm --mode ddl

# 数据导出测试
uv run python -m pyinnodb.cli --fn tests/mysql5/all_type.ibd frm tests/mysql5/all_type.frm --mode dump

# JSON 导出测试
uv run python -m pyinnodb.cli --fn tests/mysql5/all_type.ibd frm tests/mysql5/all_type.frm --mode json

# 主键搜索测试
uv run python -m pyinnodb.cli --fn tests/mysql5/all_type.ibd frm tests/mysql5/all_type.frm --mode search --primary-key 1
```

## 使用建议

1. **数据恢复**: 当 MySQL 5.7 实例无法启动时，使用 `dump` 模式导出数据
2. **数据迁移**: 结合 `ddl` 和 `dump` 模式完整迁移表结构和数据
3. **数据分析**: 使用 `json` 模式导出数据供其他工具分析
4. **快速查询**: 使用 `search` 模式快速定位特定记录

## 后续优化方向

1. **完善 FRM 解析**: 
   - 解析列的默认值
   - 提取字符集和排序规则信息
   - 支持更多索引类型（FULLTEXT, SPATIAL）

2. **增加验证功能**:
   - 为 MySQL 5.7 添加类似 MySQL 8.0 的 checksum 验证

3. **Undo Log 支持**:
   - 实现 MySQL 5.7 的历史版本追踪功能

4. **性能优化**:
   - 大表数据导出的流式处理
   - 并行解析多个页面

## 贡献者
- 增强实现: AI Assistant (Claude)
- 原始框架: WinChua

## 更新日期
2026-01-04
