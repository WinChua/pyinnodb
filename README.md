# A parser for InnoDB file formats, in Python

This library is written to acheive a better understanding of the articles from 
jeremycole's blog[<sup>[1]</sup>](#r1) about innodb.

As jeremycole say:
> What better way to improve your understanding of a structure than to implement it in another language?

So I write it during my learning. 
Jeremycole also write a parser in ruby[<sup>[2]</sup>](#r2), which is not avaliable in mysql 8.0. Compared with it, this library is based on mysql 8.0.

# Install

This project use rye[<sup>[4]</sup>], a comprehensive project and package management tool written in Rust by [Armin](https://github.com/mitsuhiko), to manage
the dependencies, so you need to setup rye first by:

```bash
curl -sSf https://rye-up.com/get | bash
```

After that cd the root path of the project and run:

```bash
rye sync
```

## Quick Usage

As a tool to parse an ibd file, this project offer a cli with some functions below.

```bash
$ rye run cli --help
Usage: python -m pyinnodb.cli [OPTIONS] FN COMMAND [ARGS]...

Options:
  --log-level [DEBUG|ERROR|INFO]
  --help                          Show this message and exit.

Commands:
  iter-record
  list-first-page
  list-page
  static-page-usage
  tosql
  validate
```

To quickly experience these function, a test ibd file was generate under data/t1.ibd.

### validate the ibd file
to check the calculate checksum of every page in ibdfile
```bash
$ rye run cli data/t1.ibd validate
2250550672 2250550672
page[1], fil.checksum[0x3a380d58], calculate checksum[0x3a380d58]
page[2], fil.checksum[0x9bdaef0], calculate checksum[0x9bdaef0]
page[3], fil.checksum[0x457a70ec], calculate checksum[0x457a70ec]
page[4], fil.checksum[0x32175826], calculate checksum[0x32175826]
page[5] is allocated, no need to calculate checksum
page[6] is allocated, no need to calculate checksum
```

### list every page type or static the usage of page

```bash
$ rye run cli data/t1.ibd list-page
0 FSP HDR
1 INSERT BUFFER BITMAP
2 INDEX NODE PAGE
3 SDI INDEX PAGE
4 INDEX PAGE
5 FRESHLY ALLOCATED PAGE
6 FRESHLY ALLOCATED PAGE
```

```bash
$ rye run cli data/t1.ibd static-page-usage
page allocate in disk: 7
page has been init: 64
segment count: 4
        seg_id:[1], SDI INDEX PAGE
        seg_id:[1], frag page number in use: 3
        seg_id:[2], frag page number in use: empty
        seg_id:[3], INDEX PAGE
        seg_id:[3], frag page number in use: 4
        seg_id:[4], frag page number in use: empty
2 page in use.
```

### generate DDL from ibd file
```bash
$ rye run cli data/t1.ibd tosql
CREATE TABLE `test`.`t1` (
    `c1` int NOT NULL,
    `name` varchar(128) NULL,
    PRIMARY KEY (`c1`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
```

or just print the dd_object only
```bash
$ rye run cli data/t1.ibd tosql --sdionly 1 # | jnv
{"name": "t1", "mysql_version_id": 80100, "created": 20231026032415, "last_altered": 20231026032415, "hidden": 1, "options": "avg_row_length=0;encrypt_type=N;key_block_size=0;keys_disabled=0;pack_record=1;stats_auto_recalc=0;stats_sample_pages=0;", "columns": [{"name": "c1", "type": 4, "is_nullable": false, "is_zerofill": false, "is_unsigned": false, "is_auto_increment": false, "is_virtual": false, "hidden": 1, "ordinal_position": 1, "char_length": 11, "numeric_precision": 10, "numeric_scale": 0, "numeric_scale_null": false, "datetime_precision": 0, "datetime_precision_null": 1, "has_no_default": true, "default_value_null": false, "srs_id_null": true, "srs_id": 0, "default_value": "AAAAAA==", "default_value_utf8_null": true, "default_value_utf8": "", "default_option": "", "update_option": "", "comment": "", "generation_expression": "", "generation_expression_utf8": "", "options": "interval_count=0;", "se_private_data": "table_id=1065;", "engine_attribute": "", "secondary_engine_attribute": "", "column_key": 2, "column_type_utf8": "int", "elements": [], "collation_id": 255, "is_explicit_collation": false}, {"name": "name", "type": 16, "is_nullable": true, "is_zerofill": false, "is_unsigned": false, "is_auto_increment": false, "is_virtual": false, "hidden": 1, "ordinal_position": 2, "char_length": 512, "numeric_precision": 0, "numeric_scale": 0, "numeric_scale_null": true, "datetime_precision": 0, "datetime_precision_null": 1, "has_no_default": false, "default_value_null": true, "srs_id_null": true, "srs_id": 0, "default_value": "", "default_value_utf8_null": true, "default_value_utf8": "", "default_option": "", "update_option": "", "comment": "", "generation_expression": "", "generation_expression_utf8": "", "options": "interval_count=0;", "se_private_data": "table_id=1065;", "engine_attribute": "", "secondary_engine_attribute": "", "column_key": 1, "column_type_utf8": "varchar(128)", "elements": [], "collation_id": 255, "is_explicit_collation": false}, {"name": "DB_TRX_ID", "type": 10, "is_nullable": false, "is_zerofill": false, "is_unsigned": false, "is_auto_increment": false, "is_virtual": false, "hidden": 2, "ordinal_position": 3, "char_length": 6, "numeric_precision": 0, "numeric_scale": 0, "numeric_scale_null": true, "datetime_precision": 0, "datetime_precision_null": 1, "has_no_default": false, "default_value_null": true, "srs_id_null": true, "srs_id": 0, "default_value": "", "default_value_utf8_null": true, "default_value_utf8": "", "default_option": "", "update_option": "", "comment": "", "generation_expression": "", "generation_expression_utf8": "", "options": "", "se_private_data": "table_id=1065;", "engine_attribute": "", "secondary_engine_attribute": "", "column_key": 1, "column_type_utf8": "", "elements": [], "collation_id": 63, "is_explicit_collation": false}, {"name": "DB_ROLL_PTR", "type": 9, "is_nullable": false, "is_zerofill": false, "is_unsigned": false, "is_auto_increment": false, "is_virtual": false, "hidden": 2, "ordinal_position": 4, "char_length": 7, "numeric_precision": 0, "numeric_scale": 0, "numeric_scale_null": true, "datetime_precision": 0, "datetime_precision_null": 1, "has_no_default": false, "default_value_null": true, "srs_id_null": true, "srs_id": 0, "default_value": "", "default_value_utf8_null": true, "default_value_utf8": "", "default_option": "", "update_option": "", "comment": "", "generation_expression": "", "generation_expression_utf8": "", "options": "", "se_private_data": "table_id=1065;", "engine_attribute": "", "secondary_engine_attribute": "", "column_key": 1, "column_type_utf8": "", "elements": [], "collation_id": 63, "is_explicit_collation": false}], "schema_ref": "test", "se_private_id": 1065, "engine": "InnoDB", "last_checked_for_upgrade_version_id": 0, "comment": "", "se_private_data": "", "engine_attribute": "", "secondary_engine_attribute": "", "row_format": 2, "partition_type": 0, "partition_expression": "", "partition_expression_utf8": "", "default_partitioning": 0, "subpartition_type": 0, "subpartition_expression": "", "subpartition_expression_utf8": "", "default_subpartitioning": 0, "indexes": [{"name": "PRIMARY", "hidden": false, "is_generated": false, "ordinal_position": 1, "comment": "", "options": "flags=0;", "se_private_data": "id=156;root=4;space_id=3;table_id=1065;trx_id=1323;", "type": 1, "algorithm": 2, "is_algorithm_explicit": false, "is_visible": true, "engine": "InnoDB", "engine_attribute": "", "secondary_engine_attribute": "", "elements": [{"ordinal_position": 1, "length": 4, "order": 2, "hidden": false, "column_opx": 0}, {"ordinal_position": 2, "length": 4294967295, "order": 2, "hidden": true, "column_opx": 2}, {"ordinal_position": 3, "length": 4294967295, "order": 2, "hidden": true, "column_opx": 3}, {"ordinal_position": 4, "length": 4294967295, "order": 2, "hidden": true, "column_opx": 1}], "tablespace_ref": "test/t1"}], "foreign_keys": [], "check_constraints": [], "partitions": [], "collation_id": 255}
```
you can also pipe to jq or jnv for a pretty output

### iterate the data record in ibd file
```bash
$ rye run cli data/t1.ibd iter-record
MRecordHeader(instant=0, no_use_1=0, deleted=0, min_record=0, num_record_owned=0, order=2, record_type=0, next_record_offset=58) 127
c1 1
name 16 <Lob length:5 preview:b'Hello..Hello' off_page:False>
MRecordHeader(instant=0, no_use_1=0, deleted=0, min_record=0, num_record_owned=0, order=4, record_type=0, next_record_offset=-29) 185
c1 2
name 16 <Lob length:5 preview:b'World..World' off_page:False>
```
Well, an ugly output now, hhh


# Usage
```bash
python -m pyinnodb.cli --fn /opt/homebrew/var/mysql/test/t1.ibd static-page-usage

## Many other thing to learn
```
# Reference
<div id='r1'></div>

- [1] [Innodb Blog](https://blog.jcole.us/innodb/)
<div id='r2'></div>

- [2] [A parser for InnoDB file formats, in Ruby](https://github.com/jeremycole/innodb_ruby)
<div id='r3'></div>

- [3] [InnoDB Diagrams](https://github.com/jeremycole/innodb_diagrams)
<div id='r4'></div>

- [4] [Rye: a Hassle-Free Python Experience](https://rye-up.com/)
