# 使用 Docker 与 uv 测试 pyinnodb 项目完整指南

本文档详细介绍如何利用项目自带的 `devtools/deploy_mysqld.py` 脚本，结合 `uv` 环境管理，部署 MySQL Docker 容器，生成测试数据，并运行 `pyinnodb` 的完整测试套件。该流程利用了 Docker 的数据持久化能力和 `uv` 的环境隔离特性，实现了测试环境的高效搭建与数据管理。

## 前提条件

*   **Docker 环境**: 确保 Docker Desktop 或 Docker Engine 已安装并正常运行。
*   **Python 与 `uv`**: 已安装 Python 3.9+ 和 `uv`。
*   **项目依赖**: 已通过 `uv sync` 安装所有项目依赖，包括 `testcontainers` 和 `docker`。

## 操作流程

### 1. 确保环境就绪

首先，确保您的 `uv` 环境是最新的，并且所有依赖都已正确安装。

```bash
# 同步项目依赖，确保环境是最新的
uv sync
```

### 2. 部署 MySQL Docker 容器

使用 `deploy_mysqld.py` 脚本启动一个特定版本的 MySQL 容器。`uv` 会自动使用正确的 Python 解释器和虚拟环境来执行脚本。

```bash
# 部署 MySQL 8.0.17 容器
uv run devtools/deploy_mysqld.py deploy --version 8.0.17
```

执行后，可以通过以下命令查看所有已部署的容器实例：

```bash
uv run devtools/deploy_mysqld.py list
```

### 3. 准备并执行 SQL 脚本

为避免权限问题，我们需要修改 SQL 脚本，使其使用 `test` 数据库（`deploy_mysqld.py` 默认创建的数据库）。

1.  **修改脚本**:
    将 `tests/mysql5/bug_fix_test.sql` 中的 `CREATE DATABASE test_frm_bugs; USE test_frm_bugs;` 替换为 `USE test;`。
    如果需要，可以将修改后的脚本保存为新文件，例如 `tests/mysql5/bug_fix_test_modified.sql`。

2.  **执行脚本**:
    使用 `deploy_mysqld.py` 的 `exec` 命令，在已部署的容器中执行 SQL 脚本，创建测试表并插入数据。

    ```bash
    uv run devtools/deploy_mysqld.py exec --version 8.0.17 --file tests/mysql5/bug_fix_test_modified.sql
    ```

### 4. 验证数据生成

`deploy_mysqld.py` 脚本通过 `mContainer.with_volume_mapping()` 将宿主机的 `datadir/8.0.17/` 目录挂载到容器的 `/var/lib/mysql`。因此，MySQL 在容器内生成的所有 `.ibd` 文件会**直接同步到宿主机的 `datadir/8.0.17/test/` 目录**。

我们可以验证文件是否已生成：

```bash
ls -la datadir/8.0.17/test/
```

您应该能看到类似 `charset_test.ibd`, `comprehensive_test.ibd` 等文件。

### 5. 配置测试套件使用新数据

为了让 `pyinnodb` 的测试套件使用我们刚刚生成的 `.ibd` 文件，需要修改测试配置文件 `tests/conftest.py`。

1.  **添加项目根路径查找函数**:
    在 `tests/conftest.py` 文件顶部添加 `get_project_root` 函数：

    ```python
    def get_project_root():
        return Path(__file__).parent.parent
    ```

2.  **修改文件路径常量**:
    在 `tests/conftest.py` 中，修改 `MYSQL8_IBD` 的定义，使其优先使用 Docker 生成的数据文件：

    ```python
    DATADIR_MYSQL8_TEST = get_project_root() / "datadir/8.0.17/test"
    MYSQL8_IBD = DATADIR_MYSQL8_TEST / "geometry_test.ibd" if DATADIR_MYSQL8_TEST.exists() else MYSQL8_DIR / "all_type.ibd"
    ```

3.  **更新测试断言 (如果需要)**:
    如果您生成的表结构与原始测试文件中定义的不同（例如，表名是 `geometry_test` 而不是 `all_type`），您需要更新相应的测试断言。例如，在 `tests/test_03_ibd_integration.py` 中：

    ```python
    # 修改前
    # assert parsed_mysql8.table.name == "all_type"
    # 修改后
    assert parsed_mysql8.table.name == "geometry_test"
    ```

### 6. 运行测试套件

一切准备就绪后，可以运行完整的测试套件来验证 `pyinnodb` 对 Docker 生成的 InnoDB 数据文件的解析能力。

```bash
uv run pytest tests/
```

您应该会看到所有或大部分测试通过。

### 7. 清理环境 (可选)

测试完成后，可以使用 `clean` 命令停止并移除 MySQL 容器及其数据目录。

```bash
uv run devtools/deploy_mysqld.py clean --version 8.0.17
```

## 关键优势与最佳实践

1.  **环境隔离**: `uv` 确保了依赖环境的干净和一致，避免了不同项目间的依赖冲突。
2.  **自动化与效率**: 整个流程高度自动化，`deploy_mysqld.py` 脚本简化了环境部署和数据管理的复杂性。
3.  **数据持久化**: 通过 Docker 卷挂载，测试数据在宿主机上持久化，无需手动 `docker cp`。
4.  **真实世界数据**: 在真实运行的 MySQL 实例中生成测试数据，比静态文件更能反映真实场景。
5.  **可复现性**: 该流程是完全可复现的，确保了在不同环境下测试的一致性。
6.  **避免版本冲突**: 明确指定 MySQL 版本进行测试，可以有效避免因 MySQL 版本差异导致的兼容性问题。

## 故障排查

*   **权限问题**: 如果执行 SQL 脚本时遇到 `Access denied` 错误，请确保脚本使用的是 `test` 数据库。
*   **路径问题**: 如果测试失败，请检查 `tests/conftest.py` 中的文件路径是否正确指向了 `datadir/8.0.17/test/`。
*   **数据不匹配**: 如果测试因表结构不匹配而失败，请检查 SQL 脚本生成的表，并相应地更新测试断言。

---

通过这份指南，您可以轻松地为 `pyinnodb` 项目搭建一个强大、灵活且可复现的 Docker 测试环境，并始终利用 `uv` 来保证环境的标准化和一致性。