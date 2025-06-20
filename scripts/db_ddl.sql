CREATE DATABASE ETL;

USE ETL;

CREATE TABLE flights_info(
flight_id NVARCHAR(20) PRIMARY KEY,
call_sign NVARCHAR(20),
origin_country NVARCHAR(50),
time_position BIGINT,
last_contact BIGINT,
longitude FLOAT,
latitude FLOAT,
altitude FLOAT,
on_ground INT,
velocity FLOAT
)