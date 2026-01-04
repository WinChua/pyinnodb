# Code Review Checklist

## 分支信息
- **分支名称**: `feature/enhance-mysql57-support`
- **提交哈希**: `c39dd4f199e2c0746e0917101d27168f3a4f1e81`
- **基础分支**: `main`
- **修改文件数**: 4
- **新增行数**: +671
- **删除行数**: -21

## 快速查看命令

```bash
# 切换到 review 分支
git checkout feature/enhance-mysql57-support

# 查看提交信息
git show c39dd4f --stat

# 查看具体代码差异
git diff main...feature/enhance-mysql57-support

# 查看单个文件的差异
git diff main src/pyinnodb/cli/frm.py
git diff main src/pyinnodb/frm/frm.py

# 查看文件内容
cat src/pyinnodb/cli/frm.py
cat src/pyinnodb/frm/frm.py
```

## 文件审查清单

### 1. src/pyinnodb/frm/frm.py
**关键修改**: 增强 MFrmColumn.to_dd_column() 方法

#### Review Points
- [ ] **导入检查**: 添加了 `b64decode` 导入 (line 8)
- [ ] **类型覆盖**: 检查所有 MySQL 数据类型是否正确映射
- [ ] **TEXT vs BLOB**: 通过 FieldFlag.BLOB 判断逻辑是否正确 (lines 128-149)
- [ ] **ENUM/SET 处理**: 元素解析和类型字符串生成 (lines 138-156)
- [ ] **字符集处理**: b64decode().decode('utf-8') 是否需要异常处理 (line 152)
- [ ] **边缘情况**: 空 elements 列表的默认值处理 (line 156)

**重点关注**:
```python
# Line 152: 可能的编码异常
elem_names = [b64decode(e.name).decode('utf-8') for e in c.elements]

# Line 156: 空 ENUM/SET 的默认值
c.column_type_utf8 = "enum('')" if dtype == ... else "set('')"
```

**查看命令**:
```bash
git diff main src/pyinnodb/frm/frm.py | less
```

### 2. src/pyinnodb/cli/frm.py
**关键修改**: 完全重写 frm 命令，新增多种模式

#### Review Points
- [ ] **函数签名**: 新增参数的类型和默认值 (lines 14-22)
- [ ] **模式实现**: 四种模式 (ddl/dump/json/search) 的逻辑
- [ ] **错误处理**: 各种异常情况的处理
- [ ] **向后兼容**: 默认模式是否为 dump (line 16)
- [ ] **eval 使用**: primary_key 使用 eval() 是否安全 (line 63)
- [ ] **内存管理**: 大表数据全部加载到内存 (lines 84-92)
- [ ] **文档字符串**: 命令帮助信息是否清晰 (lines 27-43)

**重点关注**:
```python
# Line 63: eval() 安全性
primary_key = eval(primary_key)  # 建议改为 ast.literal_eval()

# Lines 84-92: 内存管理
values = []
while page_no != const.FFFFFFFF:
    page_values = list(...)
    values.extend(page_values)  # 大表可能 OOM
```

**查看命令**:
```bash
git diff main src/pyinnodb/cli/frm.py | less
```

### 3. MYSQL57_ENHANCEMENT.md
**文档类型**: 技术实现文档

#### Review Points
- [ ] **准确性**: 技术描述是否准确
- [ ] **完整性**: 是否涵盖所有改动
- [ ] **功能对比**: 对比表格是否正确
- [ ] **已知限制**: 限制说明是否充分

**查看命令**:
```bash
cat MYSQL57_ENHANCEMENT.md | less
```

### 4. docs/MYSQL57_USAGE_GUIDE.md
**文档类型**: 用户使用指南

#### Review Points
- [ ] **示例准确性**: 所有示例是否可以正常运行
- [ ] **场景覆盖**: 常见场景是否都有涉及
- [ ] **故障排查**: 问题解决方案是否有效
- [ ] **命令正确性**: 命令语法是否正确

**查看命令**:
```bash
cat docs/MYSQL57_USAGE_GUIDE.md | less
```

## 功能测试清单

### 基础功能测试
```bash
cd /data/github/pyinnodb

# 1. DDL 模式
uv run python -m pyinnodb.cli --fn tests/mysql5/all_type.ibd frm tests/mysql5/all_type.frm --mode ddl

# 2. DUMP 模式 (默认)
uv run python -m pyinnodb.cli --fn tests/mysql5/all_type.ibd frm tests/mysql5/all_type.frm

# 3. JSON 模式
uv run python -m pyinnodb.cli --fn tests/mysql5/all_type.ibd frm tests/mysql5/all_type.frm --mode json

# 4. SEARCH 模式
uv run python -m pyinnodb.cli --fn tests/mysql5/all_type.ibd frm tests/mysql5/all_type.frm --mode search --primary-key 1
```

### 边缘情况测试
- [ ] 空表处理
- [ ] 不存在的主键
- [ ] 特殊字符在数据中
- [ ] 显示隐藏列
- [ ] 不包含 schema 的 DDL

## 代码质量检查

### 静态分析
```bash
# 如果有 ruff 或其他 linter
uv run ruff check src/pyinnodb/cli/frm.py
uv run ruff check src/pyinnodb/frm/frm.py
```

### 类型检查
```bash
# 如果配置了 mypy
uv run mypy src/pyinnodb/cli/frm.py
uv run mypy src/pyinnodb/frm/frm.py
```

## 安全检查

- [ ] **SQL 注入**: 生成的 SQL 是否有注入风险
- [ ] **路径遍历**: 文件路径是否经过验证
- [ ] **代码注入**: eval() 使用是否安全
- [ ] **敏感信息**: 是否会泄露敏感数据

## 性能考虑

- [ ] **内存使用**: 大表是否会 OOM
- [ ] **CPU 占用**: 是否有性能瓶颈
- [ ] **IO 优化**: 文件读取是否高效

## 建议的改进优先级

### P0 (必须修复)
无

### P1 (应该修复)
1. 将 eval() 替换为 ast.literal_eval()
2. 添加字符集解码异常处理

### P2 (可以后续优化)
1. 添加单元测试
2. 优化大表内存使用
3. 拆分 to_dd_column 为更小的函数
4. 从 FRM 解析字符集信息

## Review 完成标准

- [ ] 所有文件都已仔细审查
- [ ] 所有功能测试都已通过
- [ ] 所有 P0 问题已修复
- [ ] 文档已审查并确认准确
- [ ] 向后兼容性已验证
- [ ] 安全问题已评估

## 最终决策

- [ ] **Approve**: 可以合并
- [ ] **Request Changes**: 需要修改
- [ ] **Comment**: 仅评论，不阻止合并

---

**Reviewer**: _______________  
**Date**: _______________  
**Decision**: _______________
