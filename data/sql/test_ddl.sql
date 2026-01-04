use test;
drop table if exists test_dd1;
drop table if exists test_dd0;
create table test_dd0 (
	id bigint unsigned not null primary key auto_increment,
  	name varchar(200)
);

create table test_dd1 (
  `id` serial primary key auto_increment, -- serial: bigint unsigned not null
  `id_default` int default 0,
  `id_unsigned_zerofill` int unsigned zerofill,
  `int_col` int DEFAULT NULL,
  `id_invisible` int /*!80023 INVISIBLE */,
  `tinyint_col` tinyint DEFAULT '1',
  `boolean_col` boolean, -- tinyint(1)
  `smallint_col` smallint DEFAULT NULL,
  `mediumint_col` mediumint DEFAULT NULL,
  `bigint_col` bigint DEFAULT NULL,
  `float_col` float DEFAULT NULL,
  `double_col` double DEFAULT NULL,
  `decimal_col` decimal(10,2) DEFAULT NULL,
  `date_col` date DEFAULT NULL,
  `datetime_col` datetime(6),
  `timestamp_col` timestamp DEFAULT CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  `time_col` time(4) DEFAULT NULL,
  `year_col` year DEFAULT NULL,
  `char_col` char(100) CHARACTER SET utf8 COLLATE utf8_danish_ci DEFAULT NULL,
  `nchar_col` nchar(10), -- 同char(10)
  `varchar_col` varchar(100),
  `nvarchar_col` nvarchar(10), -- 同nvarchar(10)
  `binary_col` binary(10) DEFAULT NULL,
  `varbinary_col` varbinary(20) DEFAULT NULL,
  `bit_col` bit(4) DEFAULT NULL,
  `enum_col` enum('A','B','C'),
  `set_col` set('X','Y','Z'),
  `json_type_col` json DEFAULT NULL,
  `tinyblob_col` tinyblob,
  `mediumblob_col` mediumblob,
  `blob_col` blob,
  `longblob_col` longblob,
  `tinytext_col` tinytext,
  `mediumtext_col` mediumtext,
  `text_col` text,
  `longtext_col` longtext,
  `gen_stored` INT GENERATED ALWAYS AS (int_col + 1) STORED,
  `gen_virtual` INT GENERATED ALWAYS AS (id_default + 1) virtual,
  `spatial_geometry` geometry,
  `spatial_point` point /*default ST_GeomFromText('POINT(0 0)')*/ /*!80003 SRID 4326 */,
  `spatial_linestring` linestring,
  `spatial_polygon` polygon,
  `spatial_geometrycollection` geometrycollection,
  `spatial_multipoint` multipoint,
  `spatial_multilinestring` multilinestring,
  `spatial_multipolygon` multipolygon,
  `concat_char` varchar(201) as (concat(char_col,' ',varchar_col)),
  unique key(int_col),
  key(bigint_col),
  key(concat_char),
  key(varchar_col desc),
  key(int_col,time_col),
  key(int_col) /*!80000 INVISIBLE */,
  fulltext(varchar_col,text_col)
  /* check (int_col>0 and tinyint_col>0), */
  /* foreign key(id) references test_dd0(id) */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
