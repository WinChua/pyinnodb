# pyinnodb 发现的 Bug 总结

## Bug 1: eval() 导致远程代码执行 (RCE) - 严重

**文件**: `src/pyinnodb/cli/iter_record.py` 第 45 行

```python
if primary_key != "":
    primary_key = eval(primary_key)
```

**现象**: 使用 `eval()` 直接处理用户在 `--primary-key` 命令行参数中输入的内容。

**影响**: 攻击者可以构造恶意输入执行任意 Python 代码，例如：

```bash
pyinnodb --fn test.ibd search --primary-key "__import__('os').system('whoami')"
```

这会直接在服务器上执行系统命令。

**修复建议**:

```python
import ast

if primary_key != "":
    try:
        primary_key = ast.literal_eval(primary_key)
    except (ValueError, SyntaxError):
        print(f"Invalid primary key value: {primary_key}")
        return
```

---

## Bug 2: ColumnHiddenType 枚举值偏移导致 ValueError - 严重

**文件**: `src/pyinnodb/const/column_hidden_type.py`

**现象**: 原始代码枚举值从 1 开始（HT_VISIBLE=1, HT_HIDDEN_SE=2...），但 MySQL 中 `hidden=0` 表示可见列是默认值。大量 Column 实例化时传入 `hidden=0`，触发 `ValueError: 0 is not a valid ColumnHiddenType`。

**影响**: 所有包含可见列（绝大多数列）的表都无法正常解析。

**状态**: 已在测试过程中修复，枚举值改为从 0 开始：

```python
class ColumnHiddenType(Enum):
    HT_VISIBLE = 0
    HT_HIDDEN_SE = 1
    HT_HIDDEN_SQL = 2
    HT_HIDDEN_USER = 3
```

---

## Bug 3: search() 方法中 page_num 可能未初始化就被使用 - 严重

**文件**: `src/pyinnodb/sdi/table.py` 第 595-598 行

```python
for i in range(owned + 1):
    # ...
    elif (
        const.RecordType(start_rh.record_type)
        == const.RecordType.NodePointer
    ):
        record_key = f.read(len(primary_key))
        if record_key > primary_key:
            if i == 1:
                page_num = f.read(4)
            first_leaf_page = int.from_bytes(page_num, "big")  # ← BUG
            break
```

**现象**: 当 `record_key > primary_key` 且 `i != 1` 时，`page_num` 只在 `i == 1` 时被赋值，但 `first_leaf_page = int.from_bytes(page_num, "big")` 在任何情况下都会执行。如果 `i != 1` 且此前循环没有进入 `record_key == primary_key` 分支（那个分支里有 `page_num = f.read(4)`），`page_num` 就是未定义的。

**影响**: 搜索复合主键或特定 B+ 树结构的表时可能抛出 `NameError` 或读到错误的页号导致搜索失败。

**修复建议**:

```python
if record_key > primary_key:
    if i == 1:
        page_num = f.read(4)
    else:
        f.read(4)  # 跳过不需要的页号
    if i == 1:
        first_leaf_page = int.from_bytes(page_num, "big")
    break
# 更清晰的写法：在循环外初始化 page_num
```

---

## Bug 4: dump_ibd() 未检查 indexes 为空列表 - 中等

**文件**: `src/pyinnodb/cli/sql.py` 第 48 行

```python
def dump_ibd(table_object, f, oneline=True, in_json=False):
    root_page_no = int(table_object.indexes[0].private_data.get("root", 4))
```

**现象**: 直接访问 `table_object.indexes[0]`，未检查列表是否为空。

**影响**: 如果表的 SDI 数据中 indexes 为空列表（极端情况或损坏的 ibd 文件），直接抛出 `IndexError: list index out of range`，没有友好的错误提示。

**修复建议**:

```python
def dump_ibd(table_object, f, oneline=True, in_json=False):
    if not table_object.indexes:
        print("no indexes found in table")
        return
    root_page_no = int(table_object.indexes[0].private_data.get("root", 4))
```

---

## Bug 5: iter_record() 和 tree_view() 同样未检查 indexes 为空 - 中等

**文件**: `src/pyinnodb/sdi/table.py` 第 596 行和第 625 行

```python
def iter_record(self, f, hidden_col=False, garbage=False, transfer=None):
    root_page_no = int(self.indexes[0].private_data.get("root", 4))
    # ...

def tree_view(self, f):
    root_page_no = int(self.indexes[0].private_data.get("root", 4))
```

**现象**: 同 Bug 4，多处代码都直接访问 `self.indexes[0]` 而没有边界检查。第一个 leaf page 的获取也是基于 `root_index_page.get_first_leaf_page()` 的返回值做 None 检查，但 root 页号的获取本身就没有保护。

**影响**: 空索引表在调用 `iter_record()` 或 `tree_view()` 时抛出 `IndexError`。

---

## Bug 6: Python 版本检查写法不严谨 - 低

**文件**: `src/pyinnodb/sdi/table.py` 第 8 行, `src/pyinnodb/sdi/column.py` 类似位置

```python
if sys.version_info.minor >= 9:
    from functools import cache
else:
    cache = lambda x: x
```

**现象**: 仅检查了 `minor` 版本号。Python 3.x 已保证 `minor` 存在，但正确写法应同时检查 `major` 版本。此外 else 分支直接将 `cache` 设为恒等函数（不做任何缓存），而不是使用 `lru_cache(maxsize=None)` 作为 fallback，这意味着在 Python 3.8 下带 `@cache` 装饰的方法每次调用都会重新计算。

**影响**: 不会导致报错，但在 Python 3.8 下 `@cache` 装饰器完全失效，所有被装饰的 property（如 `DataClass`、`private_data`、`null_col_count` 等）每次访问都会重新构建，严重影响性能。

**修复建议**:

```python
if sys.version_info >= (3, 9):
    from functools import cache
else:
    from functools import lru_cache
    cache = lru_cache(maxsize=None)
```

---

## Bug 7: parse_var_size() 重复 seek 无保护 - 低

**文件**: `src/pyinnodb/const/tool.py` 第 10-18 行

```python
def parse_var_size(stream):
    stream.seek(-1, 1)
    size = construct.Int8ub.parse_stream(stream)
    stream.seek(-1, 1)
    if size > 0x7F:
        stream.seek(-1, 1)
        parts = construct.Int8ub.parse_stream(stream)
        stream.seek(-1, 1)
        return (size - 0x80) * 256 + parts
    return size
```

**现象**: 每次循环都先 `seek(-1, 1)` 回退 1 字节再解析，重复 4 次 seek。如果 `stream` 不支持 seek（如某些网络流或管道），会抛出 `io.UnsupportedOperation`。此外没有任何对异常输入的保护。

**影响**: 在特殊使用场景下可能抛出不明确的异常。

---

## Bug 8: search() 中 binary_search_with_page_directory 可能越界 - 低

**文件**: `src/pyinnodb/sdi/table.py` 第 581 行

```python
page_dir_idx, match = index_page.binary_search_with_page_directory(
    primary_key, f
)
# ...
f.seek(
    first_leaf_page * const.PAGE_SIZE
    + index_page.page_directory[page_dir_idx + 1]  # ← 可能越界
    - 5
)
```

**现象**: `page_dir_idx + 1` 可能超出 `page_directory` 列表的边界。如果二分查找返回的 `page_dir_idx` 是最后一个有效索引（即 `len(page_directory) - 1`），`page_dir_idx + 1` 就会越界。

**影响**: 在特定 B+ 树页面结构下抛出 `IndexError`，搜索功能崩溃。

---

## Bug 9: FLOAT/DOUBLE 字节序错误导致解析值错误 - 严重

**文件**: `src/pyinnodb/sdi/column.py` 第 423-431 行

```python
elif dtype == DDColumnType.FLOAT:
    byte_data = stream.read(dsize)
    if dsize == 4:
        return struct.unpack("f", byte_data)[0]   # ← 本机字节序（小端）
    if dsize == 8:
        return struct.unpack("d", byte_data)[0]   # ← 本机字节序（小端）
elif dtype == DDColumnType.DOUBLE:
    byte_data = stream.read(dsize)
    return struct.unpack("d", byte_data)[0]       # ← 本机字节序（小端）
```

同样的问题也出现在 VECTOR 类型的解析中（第 421 行）：

```python
vec.append(struct.unpack("f", byte_data)[0])
```

**现象**: InnoDB 在磁盘上存储 FLOAT 和 DOUBLE 时使用的是 **大端字节序（Big-Endian）** 的 IEEE 754 格式（参考 MySQL 源码 `storage/innobase/rem/rem0rec.cc` 中 `mach_write_float` / `mach_read_float` 的实现）。但代码中使用 `struct.unpack("f", ...)` 和 `struct.unpack("d", ...)`，没有指定字节序前缀，默认使用本机字节序（在 x86/x64 架构上是小端）。

**实际效果验证**：
- 磁盘上 `3.14` 的 big-endian 十六进制为 `0x4048f5c3`
- 按本机小端解析后得到的值是 **-490.56**（完全错误）
- 磁盘上 `3.1415926` 的 big-endian 十六进制为 `0x400921fb4d12d84a`
- 按本机小端解析后得到的值是 **3.6e+52**（完全错误）

**影响**:
- 所有 FLOAT、DOUBLE、REAL 列的数据解析结果都是错误的
- VECTOR 类型的非快速模式（`quick=False`）解析也是错误的
- 导出数据（dump/json/search）时浮点列的值全部错误
- 只有在小端架构（如 x86）上运行才会暴露问题，但绝大多数服务器都是 x86 架构

**修复建议**:

```python
elif dtype == DDColumnType.FLOAT:
    byte_data = stream.read(dsize)
    if dsize == 4:
        return struct.unpack(">f", byte_data)[0]   # 大端
    if dsize == 8:
        return struct.unpack(">d", byte_data)[0]   # 大端
elif dtype == DDColumnType.DOUBLE:
    byte_data = stream.read(dsize)
    return struct.unpack(">d", byte_data)[0]       # 大端
```

同理 VECTOR 类型：

```python
vec.append(struct.unpack(">f", byte_data)[0])
```

---

## Bug 10: 多处 se_private_data 返回空字符串导致 line_to_dict 解析失败 - 低

**文件**:
- `src/pyinnodb/sdi/table.py` `private_data` property
- `src/pyinnodb/sdi/column.py` `private_data` property

```python
@property
@cache
def private_data(self):
    data = const.line_to_dict(self.se_private_data, ";", "=")
    return data
```

而 `line_to_dict` 要求分隔符参数：

```python
def line_to_dict(data, linesep, keysep):
    return {
        k.strip(): v.strip()
        for k, v in [
            line.strip().split(keysep) for line in data.split(linesep) if line.strip()
        ]
    }
```

**现象**: 当 `se_private_data` 为空字符串 `""` 时，`"".split(";")` 返回 `['']`，随后 `''.split('=')` 返回 `['']`，最后 `[''].strip()` 返回 `''`，导致 `k.strip(): v.strip()` 变成 `{'' : ''}`。这个空键值对会在后续通过 `.get("root", 4)` 等方式取值时不会触发异常，但会在 `int()` 转换时潜在地出问题。不过主要问题是这是脏数据积累。

---

## Bug 总结

| 编号 | 严重程度 | 文件 | 问题 | 影响 |
|------|---------|------|------|------|
| 1 | **严重** | `cli/iter_record.py` | `eval()` 处理用户输入 | 远程代码执行 (RCE) |
| 2 | **严重** | `const/column_hidden_type.py` | 枚举值从 1 开始应为 0 | 所有表的可见列解析失败 |
| 3 | **严重** | `sdi/table.py` | `page_num` 可能未初始化 | 搜索功能报错或结果错误 |
| 4 | **中等** | `cli/sql.py` | 空 `indexes` 列表未检查 | 导出功能 IndexError |
| 5 | **中等** | `sdi/table.py` | 多处空 `indexes` 列表未检查 | iter/tree_view IndexError |
| 6 | **低** | `sdi/table.py`, `sdi/column.py` | Python 版本检查不严谨 | Python 3.8 下缓存失效 |
| 7 | **低** | `const/tool.py` | seek 无保护 | 特殊流可能报错 |
| 8 | **低** | `sdi/table.py` | `page_dir_idx + 1` 可能越界 | 搜索功能 IndexError |
| 9 | **严重** | `sdi/column.py` | FLOAT/DOUBLE 字节序错误 | 所有浮点列数据解析错误 |
| 10 | **低** | `sdi/table.py`, `sdi/column.py` | 空 se_private_data 产生脏数据 | 潜在解析异常 |
