pushd .
root_path=`git rev-parse --show-toplevel`
cd ${root_path}
version=8.4
size=6000
uv run poe dp exec --version ${version} --file ${root_path}/data/sql/001_dw/create.sql
uv run poe dp exec --version ${version} --sql "insert into sbtest values (10000,repeat('a',${size})); insert into sbtest values (10001,repeat('a',${size}));"

for id in `seq 1 8`
do
	sleep 2
	uv run poe cli --fn ${root_path}/datadir/${version}/test/sbtest.ibd tree-view --no-hidden-all
	uv run poe dp exec --version ${version} --sql "insert into sbtest values (${id},repeat('a',${size}));"
	# uv run poe cli --fn ${root_path}/datadir/${version}/test/sbtest.ibd list-page  --kind ratio
done

sleep 2
uv run poe cli --fn ${root_path}/datadir/${version}/test/sbtest.ibd tree-view --no-hidden-all

uv run poe dp exec --version ${version} --sql "alter table sbtest engine=InnoDB;"
sleep 3
uv run poe cli --fn ${root_path}/datadir/${version}/test/sbtest.ibd tree-view --no-hidden-all
uv run poe cli --fn ${root_path}/datadir/${version}/test/sbtest.ibd list-page  --kind ratio

popd
