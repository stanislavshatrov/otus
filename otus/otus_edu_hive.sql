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

-- сбор статистики
analyze table otus.pokemons_data_pb partition(type1) compute statistics for columns;

-- характеристики таблицы
describe formatted otus.pokemons_data_pb;

select * from otus.pokemons_data_pb;

-- Проверим содержание каталога и файла

dfs -ls hdfs://namenode:8020/user/hive/warehouse/otus.db/pokemons_data_pb ;
dfs -cat hdfs://namenode:8020/user/hive/warehouse/otus.db/pokemons_data_pb/type1=Bug/000001_0;


-- Таблица со статистиками по типу

drop table if exists otus.pokemons_type_stats;
create table otus.pokemons_type_stats
(
	type_name STRING,
	type_count INT,
	type_share FLOAT,
	avg_hp FLOAT,
	avg_attack FLOAT,
	avg_defense FLOAT,
	avg_speed FLOAT
)
comment 'Таблица со статистиками (среднее, кол-во) по типу 1'
row format delimited
fields terminated by '|'
stored as textfile;

insert overwrite table otus.pokemons_type_stats
select 
	 type1  as type_name
	,sum(1) as type_count
	,round(1.00 * sum(1) / sum(sum(1)) over(), 4) as type_share
	,avg(hp) as avg_hp
	,avg(attack) as avg_attack
	,avg(defense) as avg_defense
	,avg(speed) as avg_speed
from
	otus.pokemons_data_pb
group by
	type1
;

select * from otus.pokemons_type_stats;

-- Таблица с рангом типов по средним значениям метрик

drop table if exists otus.pokemons_type_places;
create table otus.pokemons_type_places
(
	type_name STRING,
	place_hp INT,
	place_attack INT,
	place_defense INT,
	place_speed INT
)
comment 'Таблица с рангами типов по средним значнием метрик'
row format delimited
fields terminated by '|'
stored as textfile;

insert overwrite table otus.pokemons_type_places
select 
	 type_name
	,row_number() over(order by avg_hp desc) as place_hp
	,row_number() over(order by avg_attack desc) as place_attack
	,row_number() over(order by avg_defense desc) as place_defense
	,row_number() over(order by avg_speed desc) as place_speed
from 
	otus.pokemons_type_stats
;

select * from otus.pokemons_type_places;

-- Таблица с покемонами, тип которых входит в ТОП-3 по скорости

drop table if exists otus.pokemons_top3_types_speed;
create table otus.pokemons_top3_types_speed like otus.pokemons_data_pb;

insert overwrite table otus.pokemons_top3_types_speed partition(type1)
select 
	 *
from 
	otus.pokemons_data_pb as m
left semi join
	otus.pokemons_type_places as p
on
	m.type1 = p.type_name
	and p.place_speed <= 3
;

select * from otus.pokemons_top3_types_speed;

-- Построение "родословной" покемонов
-- "Визуально", если сложить цифры из поля ev_yield, то получится шаг эволюции для покемона
-- при этом все цепочки идут друг за другом отсортированные по dexnum

-- для отработки материал создадим view, а не table, используя инструменты работы с массивами
create view otus.pokemons_evo_step as
select
	 dexnum
	,name
	,sum(cast(split(ev, ' ')[0] as int)) as evo_step
from
	otus.pokemons_data_pb
lateral
	view explode(split(ev_yield, ', ')) adTable as ev 
group by
	 dexnum
	,name
order by
	dexnum ;

select * from otus.pokemons_evo_step;


/*
 * Заключительные мероприятия
 * Останавливаем докер:
 * docker-compose down
 * docker system prune -a
 * 
 */

