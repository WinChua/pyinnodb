<div align="center">
<img align="center" src="https://s3.bmp.ovh/imgs/2024/07/20/13701166ef090a1e.png" alt="logo" width="200px">
<p>A parser for innodb file format</p>
</div>

----

[中文READMD](./README_zh.md)

pyinnodb.sh is a tool for dump ddl and data from ibd file, which support mysql5.7 and mysql8.0+, require py3.8+

# Download

```bash
$ wget https://github.com/WinChua/pyinnodb/releases/latest/download/pyinnodb.sh
$ chmod a+x pyinnodb.sh
$ ./pyinnodb.sh --help
```

# Usage

## Mysql 8.0+

### dump the ddl from ibd file

```bash
./pyinnodb.sh ${your_ibd_path} tosql --mode ddl
```

### dump sql script to insert data

```bash
./pyinnodb.sh ${your_ibd_path} tosql --mode sql
```

### search data with primary key(only support for int primary key now)

```bash
./pyinnodb.sh ${your_ibd_path} search --primary-key 42
```

## Mysql 5.7

### view data in ibd file, require .frm as well

```bash
./pyinnodb.sh ${your_ibd_path} frm ${your_frm_path}
```

