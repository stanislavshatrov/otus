
show databases;

show tables;

drop database if exists otus cascade;

create database otus;

create external table otus.airports
(
	ident STRING,
	type STRING,
	name STRING,
	elevation_ft STRING,
	continent STRING,
	iso_country STRING,
	iso_region STRING,
	municipality STRING,
	gps_code STRING,
	iata_code STRING,
	local_code STRING,
	coordinates STRING	
)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties(
	"quoteChar" = "@",
	"separatorChar" = ","
)
stored as textfile
location '/user/hive/warehouse/airports'
tblproperties("skip.header.line.count"="1");


select * from otus.airports;



