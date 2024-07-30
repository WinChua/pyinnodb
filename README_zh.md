<div align="center">
<img align="center" src="https://s3.bmp.ovh/imgs/2024/07/20/13701166ef090a1e.png" alt="logo" width="200px">
<p>A parser for innodb file format</p>
</div>

----

pyinnodb是一个mysql数据文件的解析工具, 同时支持mysql5.7,8.0以上的磁盘文件解析, 主要特性包括: 

* mysql8.0以上:
  * 从ibd文件中导出ddl语句;
  * 从ibd文件中导出sql语句;
  * 查看ibd文件中的sdi数据;
  * 在ibd文件中搜索指定主键的数据;

* mysql5.7:
  * 结合表结构文件(.frm)从数据文件(.ibd)中导出数据;
  * 结合表结构文件(.frm)从数据文件(.ibd)中搜索数据;

# 如何使用

## 获取最新版本的pyinnodb
```bash
wget https://github.com/WinChua/pyinnodb/releases/latest/download/pyinnodb.sh
```
## 运行环境要求
python 3.8 以上

## 基本使用

### mysql 8.0 及以上
8.0之后, mysql新增了SDI页,将表结构信息和数据都存储在一个文件中,解析的时候只
需要处理但一个.ibd文件即可.

#### 1. 验证.ibd文件
```bash
$ ./pyinnodb.sh datadir/test/all_type.ibd validate

page[1], fil.checksum[0x20fa5081], calculate checksum[0x20fa5081], eq[True]
page[2], fil.checksum[0x18395c50], calculate checksum[0x18395c50], eq[True]
page[3], fil.checksum[0x1493810c], calculate checksum[0x1493810c], eq[True]
```
如输出所示, validate会逐页读取校验字段值,计算页内容的crc32值, 输出比较结果
通过 ` | grep False` 可以确定是否存在某个页数据校验值不等,则表名.ibd存在损坏

#### 2. 输出表结构DDL语句
```bash
$ ./pyinnodb.sh datadir/test/all_type.ibd tosql --mode ddl
```

#### 3. 查看sdi
8.0之后, mysql新增了一种page用于存储表结构数据,将表结构存储在.ibd文件中,一般
称为SDI,通过以下命令查看表结构的sdi数据

```bash
$ ./pyinnodb.sh datadir/test/all_type.ibd tosql --mode sdi
```
SDI页中每一条记录都是一个JSON串, 可以通过 ` | jnv ` 实时查看json数据

#### 4. 导出ibd文件中的数据
```bash
$ ./pyinnodb.sh datadir/test/all_type.ibd tosql --mode dump
```
命令会将ibd文件中每一条记录导出成SQL语句, 通过 ` > data.sql`

#### 5. 搜索指定主键的记录
```bash
$ ./pyinnodb.sh datadir/test/all_type.ibd search --primary-key 1
```
<details>
<summary>展开输出以及解释</summary>
<pre><code>found:  all_type(id=2, BIGINT=98283201, BIT=1, DATETIME=datetime.datetime(2024, 1, 1, 9, 0, 1), DOUBLE=3.1415926, FLOAT=6.189000129699707, INTEGER=8621, LONGBLOB='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', LONGTEXT='ggg', MEDIUMBLOB=None, MEDIUMINT=999999, MEDIUMTEXT=None, NUMERIC=Decimal('11'), REAL=1092.892, SMALLINT=981, TEXT='TEXT', TIME=datetime.timedelta(seconds=11040), TIMESTAMP=datetime.datetime(2024, 7, 24, 9, 5, 28), TINYBLOB='TINYBLOB', TINYINT=99, TINYTEXT='TINYTEXT', YEAR=2024, ENUM=b'a', SET='a,b,c', DECIMAL=Decimal('910.79'), CHAR=None, VARBINARY='VARBINARY', int_def_col=42, str_def_col='world')
</code></pre>

search命令通过--primary-key选项指定主键的值, 将会在ibd文件中查找主键等于该值的记录
</details>

此外,search命令还包括--hidden-col, 指定后将会解析,记录的隐藏字段, 如:
```bash
$ ./pyinnodb.sh datadir/test/all_type.ibd search --primary-key 2 --hidden-col
```

<details>
<summary>展开输出以及解释</summary>
<pre><code>found:  all_type(id=2, BIGINT=98283201, BIT=1, DATETIME=datetime.datetime(2024, 1, 1, 9, 0, 1), DOUBLE=3.1415926, FLOAT=6.189000129699707, INTEGER=8621, LONGBLOB='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', LONGTEXT='ggg', MEDIUMBLOB=None, MEDIUMINT=999999, MEDIUMTEXT=None, NUMERIC=Decimal('11'), REAL=1092.892, SMALLINT=981, TEXT='TEXT', TIME=datetime.timedelta(seconds=11040), TIMESTAMP=datetime.datetime(2024, 7, 24, 9, 5, 28), TINYBLOB='TINYBLOB', TINYINT=99, TINYTEXT='TINYTEXT', YEAR=2024, ENUM=b'a', SET='a,b,c', DECIMAL=Decimal('910.79'), CHAR=None, VARBINARY='VARBINARY', int_def_col=42, str_def_col='world', DB_TRX_ID=2064, DB_ROLL_PTR=MRollbackPointer(insert_flag=1, rollback_seg_id=1, page_number=257, page_offset=350))</code></pre>
相较于上一条输出, 多了<code>DB_ROOL_PTR</code>以及<code>DB_TRX_ID</code>
</details>

如果进一步查看数据的修改记录, 可以指定 --with-hist 以及--datadir指定mysql的数据目录来查看, 如:
```bash
$ ./pyinnodb.sh datadir/test/all_type.ibd search --primary-key 2 --hidden-col --with-hist --datadir datadir
```

<details>
<summary>展开输出以及解释</summary>
<pre><code>found:  all_type(id=2, BIGINT=98283201, BIT=1, DATETIME=datetime.datetime(2024, 1, 1, 9, 0, 1), DOUBLE=3.1415926, FLOAT=6.189000129699707, INTEGER=8621, LONGBLOB='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', LONGTEXT='ojbk', MEDIUMBLOB=None, MEDIUMINT=999999, MEDIUMTEXT=None, NUMERIC=Decimal('1314520'), REAL=1092.892, SMALLINT=981, TEXT='TEXT', TIME=datetime.timedelta(seconds=11040), TIMESTAMP=datetime.datetime(2024, 7, 24, 9, 5, 28), TINYBLOB='TINYBLOB', TINYINT=99, TINYTEXT='TINYTEXT', YEAR=2024, ENUM=b'a', SET='a,b,c', DECIMAL=Decimal('910.79'), CHAR='89', VARBINARY='VARBINARY', int_def_col=42, str_def_col='world', DB_TRX_ID=2073, DB_ROLL_PTR=MRollbackPointer(insert_flag=0, rollback_seg_id=2, page_number=277, page_offset=745))

<Update by trx[2071]: `LONGTEXT` updated original value: LONGTEXT AGAIN; `NUMERIC` updated original value: 20230304>
<Update by trx[2069]: `LONGTEXT` updated original value: None; `CHAR` updated original value: None>
<Update by trx[2064]: `LONGTEXT` updated original value: ggg; `NUMERIC` updated original value: 11>
&lt;Insert&gt;

</pre></code>
输出内容中, 多了该条记录的修改记录,如
<pre><code>&lt;Update by trx[2071]: `LONGTEXT` updated original value: LONGTEXT AGAIN; `NUMERIC` updated original value: 20230304&gt;</code></pre>
表明, 该条记录在事务ID[2071]中被修改了, 涉及的字段包括`LONGTEXT`(修改前的值为`LONGTEXT AGAIN`)以及`NUMERIC`(修改前的值为`20230304`
</details>

### mysql 5.7
mysql 5.7的文件组织方式与mysql8.0不同,表结构存储在.frm文件,而数据存储在.ibd,对ibd文件的解析需要使用:

```
./pyinnodb.sh datadir/test/all_type.ibd frm datadir/test/all_type.frm
```
