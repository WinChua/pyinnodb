pushd . 
root_path=`git rev-parse --show-toplevel`
version=8.0.29
ibd=${root_path}/datadir/$version/test/instant_test.ibd
dp="uv run poe dp exec --version ${version}"
cli="uv run poe cli --fn ${ibd}"
function dump() {
	sleep 3
	$cli tosql --mode dump
}
#$dp --file ${root_path}/data/sql/003_instant_table_parse/create.sql
$dp --file create.sql

$dp --sql 'insert into instant_test value(1,23);'
dump

$dp --sql 'alter table instant_test add column v2 varchar(20) default "HELLO";'
dump

$dp --sql 'insert into instant_test value(2,23,"WORLD");'
dump

$cli iter-record --header 1

$dp --sql 'alter table instant_test add column v3 int(11) default NULL;'
dump

$dp --sql 'insert into instant_test value(3, 892, "OK", 23);'
dump

$cli iter-record --header 1

$dp --sql 'alter table instant_test drop column v1;'
dump


$cli iter-record --header 1


popd

