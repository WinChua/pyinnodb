import sqlalchemy

with open(".deploy_mysqld") as f:
    url = f.readline().strip()

engine = sqlalchemy.create_engine(url)


with engine.connect() as conn:
    conn.exec_driver_sql("use test")
    conn.exec_driver_sql('''
DROP TABLE IF EXISTS test_for_instant;
''')
    conn.exec_driver_sql('''
CREATE TABLE test_for_instant (
  id int(11) primary key auto_increment,
  name varchar(255) NOT NULL,
  drop1 int(11) NOT NULL,
  drop2 varchar(255) not null default 'drop2'
)
''')
    conn.exec_driver_sql('''
insert into test_for_instant(name, drop1, drop2) values ("original record", 12, "original drop 2 column");
''')
    conn.exec_driver_sql('''
alter table test_for_instant add column add1 varchar(255) default null
''')
    conn.exec_driver_sql('''
alter table test_for_instant add column add2 varchar(255) default 'add2 default'
''')
    conn.exec_driver_sql('''
insert into test_for_instant (name, drop1, drop2, add2) values ('insert after alter', 93, 'drop2 after alter', 'add2')
''')
    conn.exec_driver_sql('''
alter table test_for_instant drop column drop1
''')
    conn.exec_driver_sql('''
alter table test_for_instant add column drop1 int(11) not null default '99'
''')
    conn.exec_driver_sql('''
insert into test_for_instant (name, drop1, drop2, add2) values ('insert after first add', 23, 'drop2 after first add', 'add2')
''')
    conn.exec_driver_sql('''
alter table test_for_instant add column add3 varchar(255) not null 
''')
    conn.exec_driver_sql('''
insert into test_for_instant (name, drop1, drop2, add2, add3) values ('insert after second add', 23, 'drop2 after second add', 'add2', 'add3')
''')
    conn.commit()
