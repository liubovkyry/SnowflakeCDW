---Create a Database and Table
--Ensure you are using the sysadmin role by selecting Switch Role > SYSADMIN.
--Navigate to the Databases tab. Click Create, name the database CITIBIKE, then click CREATE.
--Select the following context settings: Role: SYSADMIN Warehouse: COMPUTE_WH  Database: CITIBIKE Schema = PUBLIC
--Next we create a table called TRIPS to use for loading the comma-delimited data. 
create or replace table trips
(tripduration integer,
starttime timestamp,
stoptime timestamp,
start_station_id integer,
start_station_name string,
start_station_latitude float,
start_station_longitude float,
end_station_id integer,
end_station_name string,
end_station_latitude float,
end_station_longitude float,
bikeid integer,
membership_type string,
usertype string,
birth_year integer,
gender integer);

--Now let's take a look at the contents of the citibike_trips stage. Navigate to the Worksheets tab and execute the following SQL statement:
list @citibike_trips;

--create file format
create or replace file format csv type='csv'
  compression = 'auto' field_delimiter = ',' record_delimiter = '\n'
  skip_header = 0 field_optionally_enclosed_by = '\042' trim_space = false
  error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134'
  date_format = 'auto' timestamp_format = 'auto' null_if = ('') comment = 'file format for ingesting data for zero to snowflake';

--verify file format is created
show file formats in database citibike;

--Execute the following statements in the worksheet to load the staged data into the table.
copy into trips from @citibike_trips file_format=csv PATTERN = '.*csv.*' ;

--Go back to the worksheet and use the TRUNCATE TABLE command to clear the table of all data and metadata:
truncate table trips;

--Verify that the table is empty by running the following command:
select * from trips limit 10;


--Change the warehouse size to large using the following ALTER WAREHOUSE:
--change warehouse size from small to large (4x)
alter warehouse compute_wh set warehouse_size='large';
Verify the change using the following SHOW WAREHOUSES:

--load data with large warehouse
show warehouses;

---Execute Some Queries
--Go to the CITIBIKE_ZERO_TO_SNOWFLAKE worksheet and change the warehouse: Role: SYSADMIN Warehouse: ANALYTICS_WH (L) Database: CITIBIKE Schema = PUBLIC
--Run the following query to see a sample of the trips data:
select * from trips limit 20;

--let's look at some basic hourly statistics on Citi Bike usage. Run the query below in the worksheet. For each hour, it shows the number of trips, average trip duration, and average trip distance.
select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)",
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)"
from trips
group by 1 order by 1;

---Execute Another Query
--let's run the following query to see which months are the busiest:
select
monthname(starttime) as "month",
count(*) as "num trips"
from trips
group by 1 order by 2 desc;

---Clone a Table
-- create a development (dev) table clone of the trips table:
create table trips_dev clone trips;

---Working with Semi-Structured Data, Views, & Joins
--First, let's create a database named WEATHER to use for storing the semi-structured JSON data.
create database weather;

--Execute the following USE commands to set the worksheet context appropriately:
use role sysadmin;
use warehouse compute_wh;
use database weather;
use schema public;

--Next, let's create a table named JSON_WEATHER_DATA to use for loading the JSON data. 
create table json_weather_data (v variant);

--Create Another External Stage
create stage nyc_weather
url = 's3://snowflake-workshop-lab/zero-weather-nyc';

--take a look at the contents of the nyc_weather stage:
list @nyc_weather;

--Load and Verify the Semi-structured Data
Copy into json_weather_data
from @nyc_weather 
    file_format = (type = json strip_outer_array = true);


select * from json_weather_data limit 10;

---Create a View and Query Semi-Structured Data
--create a view that will put structure onto the semi-structured data
create or replace view json_weather_data_view as
select
    v:obsTime::timestamp as observation_time,
    v:station::string as station_id,
    v:name::string as city_name,
    v:country::string as country,
    v:latitude::float as city_lat,
    v:longitude::float as city_lon,
    v:weatherCondition::string as weather_conditions,
    v:coco::int as weather_conditions_code,
    v:temp::float as temp,
    v:prcp::float as rain,
    v:tsun::float as tsun,
    v:wdir::float as wind_dir,
    v:wspd::float as wind_speed,
    v:dwpt::float as dew_point,
    v:rhum::float as relative_humidity,
    v:pres::float as pressure
from
    json_weather_data
where
    station_id = '72502';

--Verify the view with the following query:
select * from json_weather_data_view
where date_trunc('month',observation_time) = '2018-01-01'
limit 20;

--Use a Join Operation to Correlate Against Data Sets
select weather_conditions as conditions
,count(*) as num_trips
from citibike.public.trips
left outer join json_weather_data_view
on date_trunc('hour', observation_time) = date_trunc('hour', starttime)
where conditions is not null
group by 1 order by 2 desc;

---Using Time Travel
--Drop and Undrop a Table
--In the CITIBIKE_ZERO_TO_SNOWFLAKE worksheet, run the following DROP command to remove the JSON_WEATHER_DATA table:
drop table json_weather_data;

--Run a query on the table:
select * from json_weather_data limit 10;

--Now, restore the table:
undrop table json_weather_data;

--verify table is undropped:
select * from json_weather_data limit 10;

--Roll Back a Table

--First, run the following SQL statements to switch your worksheet to the proper context:
use role sysadmin;
use warehouse compute_wh;
use database citibike;
use schema public;

--Run the following command to replace all of the station names in the table with the word "oops":
update trips set start_station_name = 'oops';

--Now, run a query that returns the top 20 stations by number of rides. Notice that the station names result contains only one row:
select
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;

--In Snowflake, we run a command to find the query ID of the last UPDATE command and store it in a variable named $QUERY_ID.

set query_id =
(select query_id from table(information_schema.query_history_by_session (result_limit=>5))
where query_text like 'update%' order by start_time desc limit 1);

--Use Time Travel to recreate the table with the correct station names:
create or replace table trips as
(select * from trips before (statement => $query_id));

--Run the previous query again to verify that the station names have been restored:
select
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;

---Working with Roles, Account Admin, & Account Usage
--Create a New Role and Add a User
use role accountadmin;

-- create a new role named JUNIOR_DBA and assign it to your Snowflake user. To complete this task, you need to know your username, which is the name you used to log in to the UI.
create role junior_dba;
grant role junior_dba to user YOUR_USERNAME_GOES_HERE;

grant usage on database citibike to role junior_dba;
grant usage on database weather to role junior_dba;

--Change your worksheet context to the new JUNIOR_DBA role:
use role junior_dba;

---Resetting Your Snowflake Environment
--First, ensure you are using the ACCOUNTADMIN role in the worksheet:
use role accountadmin;

--Then, run the following SQL commands to drop all the objects we created in the lab:
drop share if exists zero_to_snowflake_shared_data;
-- If necessary, replace "zero_to_snowflake-shared_data" with the name you used for the share
drop database if exists citibike;
drop database if exists weather;
drop warehouse if exists analytics_wh;
drop role if exists junior_dba;

--to create a new table out of exsisted one (filtered for trips in 2016 only for exapmle)
create table trips2016 as 
select * from (
select *
from trips
where STARTTIME between '2016-01-01T00:00:00Z' and '2016-12-31T23:59:59Z'
limit 100)
