SELECT * FROM system.clusters;

DROP DATABASE adb ON CLUSTER cluster_2S_2R;

CREATE DATABASE adb ON CLUSTER cluster_2S_2R;

USE adb;

CREATE TABLE weather_pollution
ON CLUSTER cluster_2S_2R
(
    dt int,
    location String,
	temp float,
	pressure float,
	humidity float,
	clouds_all float,
	weather_main String,
	weather_description String,
	pol_aqi int,
	pol_co float,
	pol_no float,
	pol_no2 float,
	pol_o3 float,
	pol_so2 float,
	pol_pm2_5 float,
	pol_pm10 float,
	pol_nh3 float
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'openweather',
         kafka_group_name = 'clickhouse_group',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1;

CREATE TABLE shard_weather_pollution_local
ON CLUSTER cluster_2S_2R
(
    dt int,
    location String,
	temp float,
	pressure float,
	humidity float,
	clouds_all float,
	weather_main String,
	weather_description String,
	pol_aqi int,
	pol_co float,
	pol_no float,
	pol_no2 float,
	pol_o3 float,
	pol_so2 float,
	pol_pm2_5 float,
	pol_pm10 float,
	pol_nh3 float
)
ENGINE = MergeTree()
ORDER BY (dt);

CREATE TABLE weather_pollution_distributed
ON CLUSTER cluster_2S_2R
(
    dt int,
    location String,
	temp float,
	pressure float,
	humidity float,
	clouds_all float,
	weather_main String,
	weather_description String,
	pol_aqi int,
	pol_co float,
	pol_no float,
	pol_no2 float,
	pol_o3 float,
	pol_so2 float,
	pol_pm2_5 float,
	pol_pm10 float,
	pol_nh3 float
)
ENGINE = Distributed(
    cluster_2S_2R,
    adb,
    shard_weather_pollution_local,
    rand()
);

CREATE MATERIALIZED VIEW weather_pollution_view
ON CLUSTER cluster_2S_2R
TO shard_weather_pollution_local
AS
SELECT
    dt,
    location,
	temp,
	pressure,
	humidity,
	clouds_all,
	weather_main,
	weather_description,
	pol_aqi,
	pol_co,
	pol_no,
	pol_no2,
	pol_o3,
	pol_so2,
	pol_pm2_5,
	pol_pm10,
	pol_nh3
FROM weather_pollution;

SELECT * FROM weather_pollution_distributed;
