# MySQL 5.7 使用指南

PyInnoDB 现在完整支持 MySQL 5.7 及更早版本的数据文件解析。本指南将帮助你充分利用这些功能。

## 前提条件

- Python 3.9+
- MySQL 5.7 的 `.ibd` (数据文件) 和 `.frm` (表结构文件)

## 快速开始

### 1. 安装

```bash
# 使用 uv (推荐)
uv pip install pyinnodb

# 或下载预编译版本
wget https://github.com/WinChua/pyinnodb/releases/latest/download/pyinnodb.sh
chmod +x pyinnodb.sh
```

### 2. 准备文件

确保你有以下文件：
- `table_name.ibd` - InnoDB 数据文件
- `table_name.frm` - 表结构定义文件

这些文件通常位于 MySQL 数据目录：`/var/lib/mysql/database_name/`

## 使用示例

### 查看表结构 (DDL)

生成 CREATE TABLE 语句：

```bash
# 基本用法
pyinnodb --fn table.ibd frm table.frm --mode ddl

# 不包含 schema 名称
pyinnodb --fn table.ibd frm table.frm --mode ddl --no-schema

# 示例输出
CREATE TABLE `test`.`users` (
  `id` int NOT NULL,
  `name` varchar(50) DEFAULT NULL,
  `email` varchar(100) DEFAULT NULL,
  `created_at` timestamp DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci
```

### 导出数据 (INSERT 语句)

将表数据导出为可执行的 SQL 语句：

```bash
# 导出所有数据
pyinnodb --fn table.ibd frm table.frm --mode dump

# 重定向到文件
pyinnodb --fn table.ibd frm table.frm --mode dump > data.sql

# 示例输出
INSERT INTO `test`.`users`(id,name,email,created_at) VALUES (1,'Alice','alice@example.com','2024-01-01 10:00:00');
INSERT INTO `test`.`users`(id,name,email,created_at) VALUES (2,'Bob','bob@example.com','2024-01-02 11:30:00');
```

### 导出为 JSON

以 JSON 格式导出数据，便于程序处理：

```bash
# 导出为 JSON
pyinnodb --fn table.ibd frm table.frm --mode json

# 保存到文件
pyinnodb --fn table.ibd frm table.frm --mode json > data.json

# 示例输出
[
  {
    "id": 1,
    "name": "Alice",
    "email": "alice@example.com",
    "created_at": "2024-01-01 10:00:00"
  },
  {
    "id": 2,
    "name": "Bob",
    "email": "bob@example.com",
    "created_at": "2024-01-02 11:30:00"
  }
]
```

### 搜索特定记录

通过主键快速查找记录：

```bash
# 搜索主键为 1 的记录
pyinnodb --fn table.ibd frm table.frm --mode search --primary-key 1

# 示例输出
Found: users(id=1, name='Alice', email='alice@example.com', created_at=datetime.datetime(2024, 1, 1, 10, 0, 0))

# 搜索复合主键
pyinnodb --fn table.ibd frm table.frm --mode search --primary-key "(1, 'key2')"
```

### 查看隐藏列

显示 InnoDB 的内部列（事务ID、回滚指针）：

```bash
# 在导出时显示隐藏列
pyinnodb --fn table.ibd frm table.frm --mode dump --hidden-col

# 在搜索时显示隐藏列
pyinnodb --fn table.ibd frm table.frm --mode search --primary-key 1 --hidden-col

# 输出包含 DB_TRX_ID 和 DB_ROLL_PTR
Found: users(id=1, name='Alice', ..., DB_TRX_ID=12345, DB_ROLL_PTR=...)
```

## 常见场景

### 场景 1: 数据库无法启动，需要恢复数据

```bash
# 1. 找到数据文件位置
cd /var/lib/mysql/mydb/

# 2. 导出表结构
pyinnodb --fn users.ibd frm users.frm --mode ddl > users_schema.sql

# 3. 导出数据
pyinnodb --fn users.ibd frm users.frm --mode dump > users_data.sql

# 4. 在新的 MySQL 实例中恢复
mysql -u root -p newdb < users_schema.sql
mysql -u root -p newdb < users_data.sql
```

### 场景 2: 从 MySQL 5.7 迁移到 MySQL 8.0

```bash
# 对每个表执行以下操作
for table in *.frm; do
    base=$(basename $table .frm)
    echo "Processing $base..."
    
    # 导出表结构
    pyinnodb --fn ${base}.ibd frm ${base}.frm --mode ddl > ${base}_schema.sql
    
    # 导出数据
    pyinnodb --fn ${base}.ibd frm ${base}.frm --mode dump > ${base}_data.sql
done

# 在 MySQL 8.0 中导入
for schema in *_schema.sql; do
    mysql -u root -p newdb < $schema
done

for data in *_data.sql; do
    mysql -u root -p newdb < $data
done
```

### 场景 3: 数据分析和审计

```bash
# 导出为 JSON 供 Python/Node.js 分析
pyinnodb --fn orders.ibd frm orders.frm --mode json > orders.json

# 使用 jq 进行查询
cat orders.json | jq '.[] | select(.amount > 1000)'

# 统计数据
cat orders.json | jq 'length'  # 记录总数
```

### 场景 4: 查找特定数据

```bash
# 查找用户 ID 为 12345 的所有信息
pyinnodb --fn users.ibd frm users.frm --mode search --primary-key 12345

# 查找并显示事务信息
pyinnodb --fn users.ibd frm users.frm --mode search --primary-key 12345 --hidden-col

# 批量查找
for id in 100 200 300 400 500; do
    echo "=== User ID: $id ==="
    pyinnodb --fn users.ibd frm users.frm --mode search --primary-key $id
done
```

## 高级选项

### 指定根页编号

如果表使用了非标准的根页位置：

```bash
pyinnodb --fn table.ibd frm table.frm --mode dump --root-page 5
```

### 组合使用

```bash
# 先验证数据存在，再导出
if pyinnodb --fn table.ibd frm table.frm --mode search --primary-key 1 > /dev/null 2>&1; then
    echo "Data found, exporting..."
    pyinnodb --fn table.ibd frm table.frm --mode dump > data.sql
else
    echo "No data found"
fi
```

## 故障排查

### 问题 1: "No data found"

**原因**: 根页位置不正确

**解决方案**:
```bash
# 尝试不同的根页编号
pyinnodb --fn table.ibd frm table.frm --mode dump --root-page 4
pyinnodb --fn table.ibd frm table.frm --mode dump --root-page 5
```

### 问题 2: 类型显示不正确

**原因**: FRM 文件可能损坏或版本不兼容

**解决方案**:
- 检查 FRM 文件是否完整
- 确认 MySQL 版本（5.5, 5.6, 5.7）
- 尝试从备份获取 FRM 文件

### 问题 3: 中文乱码

**原因**: 字符集设置问题

**解决方案**:
```bash
# 导出后转换编码
pyinnodb --fn table.ibd frm table.frm --mode dump | iconv -f utf8 -t utf8 > data.sql

# 或在 MySQL 导入时指定字符集
mysql --default-character-set=utf8mb4 -u root -p < data.sql
```

## 性能提示

1. **大表处理**: 对于大表，建议使用 `dump` 模式并重定向到文件，避免终端输出延迟
   ```bash
   pyinnodb --fn large_table.ibd frm large_table.frm --mode dump > large_table.sql
   ```

2. **批量处理**: 使用脚本批量处理多个表
   ```bash
   #!/bin/bash
   for frm in *.frm; do
       base=$(basename $frm .frm)
       pyinnodb --fn ${base}.ibd frm $frm --mode dump > ${base}.sql &
   done
   wait
   ```

3. **JSON 大文件**: 使用流式 JSON 解析器处理大型 JSON 输出
   ```bash
   pyinnodb --fn table.ibd frm table.frm --mode json | jq -c '.[]' | while read record; do
       # 逐条处理记录
       echo "$record"
   done
   ```

## 限制和注意事项

1. **元数据限制**: FRM 文件不包含列注释、外键约束等信息
2. **AUTO_INCREMENT**: 当前值不会在 DDL 中体现
3. **触发器和存储过程**: 需要从其他途径恢复
4. **权限信息**: 需要单独备份和恢复
5. **主键限制**: search 模式目前主要支持整数主键

## 获取帮助

```bash
# 查看命令帮助
pyinnodb --fn table.ibd frm --help

# 查看所有可用选项
pyinnodb --help
```

## 相关资源

- [PyInnoDB GitHub](https://github.com/WinChua/pyinnodb)
- [MySQL 5.7 官方文档](https://dev.mysql.com/doc/refman/5.7/en/)
- [InnoDB 文件格式](https://dev.mysql.com/doc/internals/en/innodb-file-format.html)

## 更新日志

- 2026-01-04: 新增完整的 MySQL 5.7 支持
- 支持 DDL/DUMP/JSON/SEARCH 四种模式
- 完善所有 MySQL 数据类型的解析
