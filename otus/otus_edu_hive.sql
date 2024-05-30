/*
 * Подготовительные мероприятия:
 * 
 * 1. Скачать датасет: https://www.kaggle.com/datasets/guavocado/pokemon-stats-1025-pokemons
 * 2. Запустить контейнер: docker-compose up -d (git clone https://github.com/big-data-europe/docker-hive.git)
 * 3. Посмотреть имя сервера (NAMES): docker ps
 * 4. Переложить файл на сервер: docker cp "../Загрузки/pokemon_data.csv" docker-hive_hive-server_1:/opt
 * 5. Перейти в термирал сервера: docker-compose exec hive-server bash
 * 6. Заменить кавычки sed -i "s/\"/@/g" pokemon_data.csv
 * 7. Переложить файл на HDFS: hdfs dfs -put -f /opt/pokemon_data.csv /user/hive;
 * 8. Проверить наличие файла на HDFS: hdfs dfs -ls /user/hive
 */

-- Создание БД, таблицы для загрузки сырых данных, загрузка данных.

drop database if exists otus cascade;
create database otus
comment 'БД для выполнения ДЗ по Hive'
with dbproperties ('creator'='Stanislav Shatrov', 'date'='2024-05-30');
use otus;
describe database otus;

drop table if exists otus.pokemons_data;
create table otus.pokemons_data 
(
	dexnum INT,
	name STRING,
	generation INT,
	type1 STRING,
	type2 STRING,
	species STRING,
	height FLOAT,
	weight FLOAT,
	ability1 STRING,
	ability2 STRING,
	hidden_ability STRING,
	hp INT,
	attack INT,
	defense INT,
	sp_atk INT,
	sp_def INT,
	speed INT,
	total INT,
	ev_yield STRING,
	catch_rate INT,
	base_friendship INT,
	base_exp INT,
	growth_rate STRING,
	egg_group1 STRING,
	egg_group2 STRING,
	percent_male INT,
	percent_female INT,
	egg_cycles INT,
	special_group STRING
)
comment 'Таблица для сырых данных из файла (AS IS)'
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
	"quoteChar" = "@",   
	"separatorChar" = ","
) 
stored as textfile
tblproperties("skip.header.line.count"="1");

load data inpath '/user/hive/pokemon_data.csv' overwrite into table otus.pokemons_data;

select * from otus.pokemons_data;

-- Переложим данные с партициями и бакетами

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.enforce.bucketing = true;
set hive.support.quoted.identifiers=none;

drop table if exists otus.pokemons_data_pb;
create table otus.pokemons_data_pb
(
	dexnum INT,
	name STRING,
	generation INT,
	type2 STRING,
	species STRING,
	height FLOAT,
	weight FLOAT,
	ability1 STRING,
	ability2 STRING,
	hidden_ability STRING,
	hp INT,
	attack INT,
	defense INT,
	sp_atk INT,
	sp_def INT,
	speed INT,
	total INT,
	ev_yield STRING,
	catch_rate INT,
	base_friendship INT,
	base_exp INT,
	growth_rate STRING,
	egg_group1 STRING,
	egg_group2 STRING,
	percent_male INT,
	percent_female INT,
	egg_cycles INT,
	special_group STRING
)
comment 'Партиционирование по полю type1 + 3 бакета по полю dexnum'
partitioned by (type1 STRING)
clustered by (dexnum) into 3 buckets
row format delimited
fields terminated by '|'
stored as textfile;

insert overwrite table otus.pokemons_data_pb partition(type1)
select `(type1|special_group)?+.+`, lower(special_group) as special_group, type1
from otus.pokemons_data;

select * from otus.pokemons_data_pb;

-- Таблица с количеством и долей по типу1

drop table if exists hive_hw.pokemons_type_count;
create table hive_hw.pokemons_type_count
(
	type_name STRING,
	type_count INT,
	type_share FLOAT
)



select 
	 special_group  as type_name
	,sum(1) as type_count
	,round(100.00 * sum(1) / sum(sum(1)) over(), 1) as type_share
from
	hive_hw.pokemons_file_data
group by
	special_group 











