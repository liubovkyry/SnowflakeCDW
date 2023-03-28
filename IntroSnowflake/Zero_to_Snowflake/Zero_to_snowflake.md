1. Overview
This entry-level guide designed for database and data warehouse administrators and architects will help you navigate the Snowflake interface and introduce you to some of our core capabilities.
What You'll Learn:
How to create stages, databases, tables, views, and virtual warehouses.
How to load structured and semi-structured data.
How to perform analytical queries on data in Snowflake, including joins between tables.
How to clone objects.
How to undo user errors using Time Travel.
How to create roles and users, and grant them privileges.
How to securely and easily share data with other accounts.
How to consume datasets in the Snowflake Data Marketplace.

The Lab Story
This lab is based on the analytics team at Citi Bike, a real, citywide bike sharing system in New York City, USA. The team wants to run analytics on data from their internal transactional systems to better understand their riders and how to best serve them.

We will first load structured .csv data from rider transactions into Snowflake. Later we will work with open-source, semi-structured JSON weather data to determine if there is any correlation between the number of bike rides and the weather.

Preparing to Load Data
This section walks you through the steps to:

 - Create a database and table.
 - Create an external stage.
 - Create a file format for the data.

Getting Data into Snowflake There are many ways to get data into Snowflake from many locations including the COPY command, Snowpipe auto-ingestion, external connectors, or third-party ETL/ELT solutions. For more information on getting data into Snowflake, see the Snowflake documentation. For the purposes of this lab, we use the COPY command and AWS S3 storage to load data manually. In a real-world scenario, you would more likely use an automated process or ETL solution.
https://docs.snowflake.com/en/user-guide-data-load

The data consists of information about trip times, locations, user type, gender, age, etc. On AWS S3, the data represents 61.5M rows, 377 objects, and 1.9GB compressed.

Create a Database and Table
First, let's create a database called CITIBIKE to use for loading the structured data.

Ensure you are using the sysadmin role by selecting Switch Role > SYSADMIN.

Navigate to the Databases tab. Click Create, name the database CITIBIKE, then click CREATE.

Now navigate to the Worksheets tab. You should see the worksheet we created in step 3.

We need to set the context appropriately within the worksheet. In the upper right corner of the worksheet, click the box to the left of the Share button to show the context menu. Here we control the elements you can see and run from each worksheet. We are using the UI here to set the context. Later in the lab, we will accomplish the same thing via SQL commands within the worksheet.

Select the following context settings:

Role: SYSADMIN Warehouse: COMPUTE_WH

Next, in the drop-down for the database, select the following context settings:

Database: CITIBIKE Schema = PUBLIC

To make working in the worksheet easier, let's rename it. In the top left corner, click the worksheet name, which is the timestamp when the worksheet was created, and change it to CITIBIKE_ZERO_TO_SNOWFLAKE.

Next we create a table called TRIPS to use for loading the comma-delimited data. Instead of using the UI, we use the worksheet to run the DDL that creates the table. Copy the following SQL text into your worksheet:
```
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
```

Navigate to the Databases tab by clicking the HOME icon in the upper left corner of the worksheet. Then click Data > Databases. In the list of databases, click CITIBIKE > PUBLIC > TABLES to see your newly created TRIPS table. If you don't see any databases on the left, expand your browser because they may be hidden.
Click TRIPS and the Columns tab to see the table structure you just created.

Create an External Stage
We are working with structured, comma-delimited data that has already been staged in a public, external S3 bucket. Before we can use this data, we first need to create a Stage that specifies the location of our external bucket.

Note: For this lab we are using an AWS-East bucket. To prevent data egress/transfer costs in the future, you should select a staging location from the same cloud provider and region as your Snowflake account.

From the Databases tab, click the CITIBIKE database and PUBLIC schema. In the Stages tab, click the Create button, then Stage > Amazon S3.

In the "Create Securable Object" dialog that opens, replace the following values in the SQL statement:
```
stage_name: citibike_trips
```
```
url: s3://snowflake-workshop-lab/citibike-trips-csv/
```
Note: Make sure to include the final forward slash (/) at the end of the URL or you will encounter errors later when loading data from the bucket. Also ensure you have removed ‘credentials = (...)' statejment which is not required. The create stage command should resemble that show above exactly.

The S3 bucket for this lab is public so you can leave the credentials options in the statement empty. In a real-world scenario, the bucket used for an external stage would likely require key information.

Now let's take a look at the contents of the citibike_trips stage. Navigate to the Worksheets tab and execute the following SQL statement:

```
list @citibike_trips;
```
In the results in the bottom pane, you should see the list of files in the stage.

Create a File Format
Before we can load the data into Snowflake, we have to create a file format that matches the data structure.

In the worksheet, run the following command to create the file format:


```
create or replace file format csv type='csv'
  compression = 'auto' field_delimiter = ',' record_delimiter = '\n'
  skip_header = 0 field_optionally_enclosed_by = '\042' trim_space = false
  error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134'
  date_format = 'auto' timestamp_format = 'auto' null_if = ('') comment = 'file format for ingesting data for zero to snowflake';
  ```

Verify that the file format has been created with the correct settings by executing the following command:

```
show file formats in database citibike;
```

 Loading Data
 In this section, we will use a virtual warehouse and the COPY command to initiate bulk loading of structured data into the Snowflake table we created in the last section

 Resize and Use a Warehouse for Data Loading
Compute resources are needed for loading data. Snowflake's compute nodes are called virtual warehouses and they can be dynamically sized up or out according to workload, whether you are loading data, running a query, or performing a DML operation. Each workload can have its own warehouse so there is no resource contention.

Navigate to the Warehouses tab (under Admin). This is where you can view all of your existing warehouses, as well as analyze their usage trends.

Note the + Warehouse option in the upper right corner of the top. This is where you can quickly add a new warehouse. However, we want to use the existing warehouse COMPUTE_WH included in the 30-day trial environment.

Click the row of the COMPUTE_WH warehouse. Then click the ... (dot dot dot) in the upper right corner text above it to see the actions you can perform on the warehouse. We will use this warehouse to load the data from AWS S3.
Click Edit to walk through the options of this warehouse and learn some of Snowflake's unique functionality.

The Size drop-down is where the capacity of the warehouse is selected. For larger data loading operations or more compute-intensive queries, a larger warehouse is recommended. The sizes translate to the underlying compute resources provisioned from the cloud provider (AWS, Azure, or GCP) where your Snowflake account is hosted. It also determines the number of credits consumed by the warehouse for each full hour it runs. The larger the size, the more compute resources from the cloud provider are allocated to the warehouse and the more credits it consumes. For example, the 4X-Large setting consumes 128 credits for each full hour. This sizing can be changed up or down at any time with a simple click.
If you are using Snowflake Enterprise Edition (or higher) the Query Acceleration option is available. When it is enabled for a warehouse, it can improve overall warehouse performance by reducing the impact of outlier queries, which are queries that use more resources than the typical query. Leave this disabled
If you are using Snowflake Enterprise Edition (or higher) and the Multi-cluster Warehouse option is enabled, you will see additional options. This is where you can set up a warehouse to use multiple clusters of compute resources, up to 10 clusters. For example, if a 4X-Large multi-cluster warehouse is assigned a maximum cluster size of 10, it can scale out to 10 times the compute resources powering that warehouse...and it can do this in seconds! However, note that this will increase the number of credits consumed by the warehouse to 1280 if all 10 clusters run for a full hour (128 credits/hour x 10 clusters). Multi-cluster is ideal for concurrency scenarios, such as many business analysts simultaneously running different queries using the same warehouse. In this use case, the various queries are allocated across multiple clusters to ensure they run quickly.
Under Advanced Warehouse Options, the options allow you to automatically suspend the warehouse when not in use so no credits are needlessly consumed. There is also an option to automatically resume a suspended warehouse so when a new workload is sent to it, it automatically starts back up. This functionality enables Snowflake's efficient "pay only for what you use" billing model which allows you to scale your resources when necessary and automatically scale down or turn off when not needed, nearly eliminating idle resources. Additionally, there is an option to change the Warehouse type from Standard to Snowpark-optimized. Snowpark-optmized warehouses provide 16x memory per node and are recommended for workloads that have large memory requirements such as ML training use cases using a stored procedure on a single virtual warehouse node. Leave this type as Standard

We are going to use this virtual warehouse to load the structured data in the CSV files (stored in the AWS S3 bucket) into Snowflake. However, we are first going to change the size of the warehouse to increase the compute resources it uses. After the load, note the time taken and then, in a later step in this section, we will re-do the same load operation with an even larger warehouse, observing its faster load time.

Change the Size of this data warehouse from X-Small to Small. then click the Save Warehouse button

Load the Data
Now we can run a COPY command to load the data into the TRIPS table we created earlier.

Navigate back to the CITIBIKE_ZERO_TO_SNOWFLAKE worksheet in the Worksheets tab. Make sure the worksheet context is correctly set:

Role: SYSADMIN Warehouse: COMPUTE_WH Database: CITIBIKE Schema = PUBLIC

Execute the following statements in the worksheet to load the staged data into the table. This may take up to 30 seconds.
```
copy into trips from @citibike_trips file_format=csv PATTERN = '.*csv.*' ;
```

Next, navigate to the Query History tab by clicking the Home icon and then Activity > Query History. Select the query at the top of the list, which should be the COPY INTO statement that was last executed. Select the Query Profile tab and note the steps taken by the query to execute, query details, most expensive nodes, and additional statistics.

Now let's reload the TRIPS table with a larger warehouse to see the impact the additional compute resources have on the loading time.

Go back to the worksheet and use the TRUNCATE TABLE command to clear the table of all data and metadata:
```
truncate table trips;
```
Verify that the table is empty by running the following command:

--verify table is clear
```
select * from trips limit 10;
```

The result should show "Query produced no results".

Change the warehouse size to large using the following ALTER WAREHOUSE:
```
--change warehouse size from small to large (4x)
alter warehouse compute_wh set warehouse_size='large';
Verify the change using the following SHOW WAREHOUSES:

--load data with large warehouse
show warehouses;
```
Execute the same COPY INTO statement as before to load the same data again:
```
copy into trips from @citibike_trips
file_format=CSV;
```
Once the load is done, navigate back to the Queries page (Home icon > Activity > Query History). Compare the times of the two COPY INTO commands. The load using the Large warehouse was significantly faster.

Create a New Warehouse for Data Analytics

Going back to the lab story, let's assume the Citi Bike team wants to eliminate resource contention between their data loading/ETL workloads and the analytical end users using BI tools to query Snowflake. As mentioned earlier, Snowflake can easily do this by assigning different, appropriately-sized warehouses to various workloads. Since Citi Bike already has a warehouse for data loading, let's create a new warehouse for the end users running analytics. We will use this warehouse to perform analytics in the next section.

Navigate to the Admin > Warehouses tab, click + Warehouse, and name the new warehouse - ANALYTICS_WH `` and set the size to Large.

If you are using Snowflake Enterprise Edition (or higher) and Multi-cluster Warehouses is enabled, you will see additional settings:

Make sure Max Clusters is set to 1.
Leave all the other settings at their defaults.

Working with Queries, the Results Cache, & Cloning

In the previous exercises, we loaded data into two tables using Snowflake's COPY bulk loader command and the COMPUTE_WH virtual warehouse. Now we are going to take on the role of the analytics users at Citi Bike who need to query data in those tables using the worksheet and the second warehouse ANALYTICS_WH.

Note: Real World Roles and Querying Within a real company, analytics users would likely have a different role than SYSADMIN. To keep the lab simple, we are going to stay with the SYSADMIN role for this section. Additionally, querying would typically be done with a business intelligence product like Tableau, Looker, PowerBI, etc. For more advanced analytics, data science tools like Datarobot, Dataiku, AWS Sagemaker or many others can query Snowflake. Any technology that leverages JDBC/ODBC, Spark, Python, or any of the other supported programmatic interfaces can run analytics on the data in Snowflake. To keep this lab simple, all queries are being executed via the Snowflake worksheet.

Execute Some Queries

Go to the CITIBIKE_ZERO_TO_SNOWFLAKE worksheet and change the warehouse to use the new warehouse you created in the last section. Your worksheet context should be the following:

Role: SYSADMIN Warehouse: ANALYTICS_WH (L) Database: CITIBIKE Schema = PUBLIC

Run the following query to see a sample of the trips data:
```
select * from trips limit 20;
```

Now, let's look at some basic hourly statistics on Citi Bike usage. Run the query below in the worksheet. For each hour, it shows the number of trips, average trip duration, and average trip distance.
```
select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)",
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)"
from trips
group by 1 order by 1;
```

Use the Result Cache
Snowflake has a result cache that holds the results of every query executed in the past 24 hours. These are available across warehouses, so query results returned to one user are available to any other user on the system who executes the same query, provided the underlying data has not changed. Not only do these repeated queries return extremely fast, but they also use no compute credits.

Execute Another Query
Next, let's run the following query to see which months are the busiest:
```
select
monthname(starttime) as "month",
count(*) as "num trips"
from trips
group by 1 order by 2 desc;
```

Clone a Table
Snowflake allows you to create clones, also known as "zero-copy clones" of tables, schemas, and databases in seconds. When a clone is created, Snowflake takes a snapshot of data present in the source object and makes it available to the cloned object. The cloned object is writable and independent of the clone source. Therefore, changes made to either the source object or the clone object are not included in the other.

Note: Zero-Copy Cloning A massive benefit of zero-copy cloning is that the underlying data is not copied. Only the metadata and pointers to the underlying data change. Hence, clones are "zero-copy" and storage requirements are not doubled when the data is cloned. Most data warehouses cannot do this, but for Snowflake it is easy!

Run the following command in the worksheet to create a development (dev) table clone of the trips table:
```
create table trips_dev clone trips;
```

Working with Semi-Structured Data, Views, & Joins

This section requires loading additional data and, therefore, provides a review of data loading while also introducing loading semi-structured data.

Going back to the lab's example, the Citi Bike analytics team wants to determine how weather impacts ride counts. To do this, in this section, we will:

 - Load weather data in semi-structured JSON format held in a public S3 bucket.
 - Create a view and query the JSON data using SQL dot notation.
 - Run a query that joins the JSON data to the previously loaded TRIPS data.
 - Analyze the weather and ride count data to determine their relationship.
The JSON data consists of weather information provided by MeteoStat detailing the historical conditions of New York City from 2016-07-05 to 2019-06-25. It is also staged on AWS S3 where the data consists of 75k rows, 36 objects, and 1.1MB compressed. If viewed in a text editor, the raw JSON in the GZ files looks like:

Note: SEMI-STRUCTURED DATA Snowflake can easily load and query semi-structured data such as JSON, Parquet, or Avro without transformation. This is a key Snowflake feature because an increasing amount of business-relevant data being generated today is semi-structured, and many traditional data warehouses cannot easily load and query such data. Snowflake makes it easy!

Create a New Database and Table for the Data
First, in the worksheet, let's create a database named WEATHER to use for storing the semi-structured JSON data.
```
create database weather;
```
Execute the following USE commands to set the worksheet context appropriately:

```
use role sysadmin;

use warehouse compute_wh;

use database weather;

use schema public;
```

Executing Multiple Commands Remember that you need to execute each command individually. However, you can execute them in sequence together by selecting all of the commands and then clicking the Play/Run button (or using the keyboard shortcut).

Next, let's create a table named JSON_WEATHER_DATA to use for loading the JSON data. In the worksheet, execute the following CREATE TABLE command:
```
create table json_weather_data (v variant);
```
Note that Snowflake has a special column data type called VARIANT that allows storing the entire JSON object as a single row and eventually query the object directly.

Create Another External Stage
In the CITIBIKE_ZERO_TO_SNOWFLAKE worksheet, use the following command to create a stage that points to the bucket where the semi-structured JSON data is stored on AWS S3:

```
create stage nyc_weather
url = 's3://snowflake-workshop-lab/zero-weather-nyc';
```
Now let's take a look at the contents of the nyc_weather stage. Execute the following LIST command to display the list of files:
```
list @nyc_weather;

```
In the results pane, you should see a list of .gz files from S3

Load and Verify the Semi-structured Data
In this section, we will use a warehouse to load the data from the S3 bucket into the JSON_WEATHER_DATA table we created earlier.

In the CITIBIKE_ZERO_TO_SNOWFLAKE worksheet, execute the COPY command below to load the data.

Note that you can specify a FILE FORMAT object inline in the command. In the previous section where we loaded structured data in CSV format, we had to define a file format to support the CSV structure. Because the JSON data here is well-formed, we are able to simply specify the JSON type and use all the default settings:
```
copy into json_weather_data
from @nyc_weather 
    file_format = (type = json strip_outer_array = true);
    ```

Create a View and Query Semi-Structured Data

Views & Materialized Views A view allows the result of a query to be accessed as if it were a table. Views can help present data to end users in a cleaner manner, limit what end users can view in a source table, and write more modular SQL. Snowflake also supports materialized views in which the query results are stored as though the results are a table. This allows faster access, but requires storage space. Materialized views can be created and queried if you are using Snowflake Enterprise Edition (or higher).

Run the following command to create a columnar view of the semi-structured JSON weather data so it is easier for analysts to understand and query. The 72502 value for station_id corresponds to Newark Airport, the closest station that has weather conditions for the whole period.

-- create a view that will put structure onto the semi-structured data
```
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
```
 Verify the view with the following query:
```
select * from json_weather_data_view
where date_trunc('month',observation_time) = '2018-01-01'
limit 20;   
```

Use a Join Operation to Correlate Against Data Sets
We will now join the JSON weather data to our CITIBIKE.PUBLIC.TRIPS data to answer our original question of how weather impacts the number of rides.

Run the query below to join WEATHER to TRIPS and count the number of trips associated with certain weather conditions:

Note: Because we are still in the worksheet, the WEATHER database is still in use. You must, therefore, fully qualify the reference to the TRIPS table by providing its database and schema name.

```
select weather_conditions as conditions
,count(*) as num_trips
from citibike.public.trips
left outer join json_weather_data_view
on date_trunc('hour', observation_time) = date_trunc('hour', starttime)
where conditions is not null
group by 1 order by 2 desc;
```

Using Time Travel
Snowflake's powerful Time Travel feature enables accessing historical data, as well as the objects storing the data, at any point within a period of time. The default window is 24 hours and, if you are using Snowflake Enterprise Edition, can be increased up to 90 days. Most data warehouses cannot offer this functionality, but - you guessed it - Snowflake makes it easy!

Some useful applications include:

Restoring data-related objects such as tables, schemas, and databases that may have been deleted.
Duplicating and backing up data from key points in the past.
Analyzing data usage and manipulation over specified periods of time.

Drop and Undrop a Table
First let's see how we can restore data objects that have been accidentally or intentionally deleted.

In the CITIBIKE_ZERO_TO_SNOWFLAKE worksheet, run the following DROP command to remove the JSON_WEATHER_DATA table:
```
drop table json_weather_data;
```
Run a query on the table:
```
select * from json_weather_data limit 10;
```
In the results pane at the bottom, you should see an error because the underlying table has been dropped.

Now, restore the table:
```
undrop table json_weather_data;
```
The json_weather_data table should be restored. Verify by running the following query:

```
select * from json_weather_data limit 10;
```

Roll Back a Table

Let's roll back the TRIPS table in the CITIBIKE database to a previous state to fix an unintentional DML error that replaces all the station names in the table with the word "oops".

First, run the following SQL statements to switch your worksheet to the proper context:
```
use role sysadmin;

use warehouse compute_wh;

use database citibike;

use schema public;
```
Run the following command to replace all of the station names in the table with the word "oops":

```
update trips set start_station_name = 'oops';
```

Now, run a query that returns the top 20 stations by number of rides. Notice that the station names result contains only one row:

```
select
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;
```

Normally we would need to scramble and hope we have a backup lying around.

In Snowflake, we can simply run a command to find the query ID of the last UPDATE command and store it in a variable named $QUERY_ID.
```
set query_id =
(select query_id from table(information_schema.query_history_by_session (result_limit=>5))
where query_text like 'update%' order by start_time desc limit 1);
```

Use Time Travel to recreate the table with the correct station names:
```
create or replace table trips as
(select * from trips before (statement => $query_id));
```

Run the previous query again to verify that the station names have been restored:

```
select
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;
```

Working with Roles, Account Admin, & Account Usage
Continuing with the lab story, let's assume a junior DBA has joined Citi Bike and we want to create a new role for them with less privileges than the system-defined, default role of SYSADMIN.

Create a New Role and Add a User
In the CITIBIKE_ZERO_TO_SNOWFLAKE worksheet, switch to the ACCOUNTADMIN role to create a new role. ACCOUNTADMIN encapsulates the SYSADMIN and SECURITYADMIN system-defined roles. It is the top-level role in the account and should be granted only to a limited number of users.

In the CITIBIKE_ZERO_TO_SNOWFLAKE worksheet, run the following command:

```
use role accountadmin;
```
Before a role can be used for access control, at least one user must be assigned to it. So let's create a new role named JUNIOR_DBA and assign it to your Snowflake user. To complete this task, you need to know your username, which is the name you used to log in to the UI.

Use the following commands to create the role and assign it to you. Before you run the GRANT ROLE command, replace YOUR_USERNAME_GOES_HERE with your username:
```
create role junior_dba;

grant role junior_dba to user YOUR_USERNAME_GOES_HERE;
```
Note: If you try to perform this operation while in a role such as SYSADMIN, it would fail due to insufficient privileges. By default (and design), the SYSADMIN role cannot create new roles or users.

Change your worksheet context to the new JUNIOR_DBA role:
```
use role junior_dba;
```
Also, the warehouse is not selected because the newly created role does not have usage privileges on any warehouse. Let's fix it by switching back to ADMIN role and grant usage privileges to COMPUTE_WH warehouse.
```
use role accountadmin;
```
grant usage on warehouse compute_wh to role junior_dba;
```
Switch back to the JUNIOR_DBA role. You should be able to use COMPUTE_WH now.
```
use role junior_dba;

use warehouse compute_wh;
```
Finally, you can notice that in the database object browser panel on the left, the CITIBIKE and WEATHER databases no longer appear. This is because the JUNIOR_DBA role does not have privileges to access them.

Switch back to the ACCOUNTADMIN role and grant the JUNIOR_DBA the USAGE privilege required to view and use the CITIBIKE and WEATHER databases:
```
use role accountadmin;

grant usage on database citibike to role junior_dba;

grant usage on database weather to role junior_dba;
```
Switch to the JUNIOR_DBA role:
```
use role junior_dba;
```
Notice that the CITIBIKE and WEATHER databases now appear in the database object browser panel on the left. If they don't appear, try clicking ... in the panel, then clicking Refresh.

Sharing Data Securely & the Data Marketplace
Snowflake enables data access between accounts through the secure data sharing features. Shares are created by data providers and imported by data consumers, either through their own Snowflake account or a provisioned Snowflake Reader account. The consumer can be an external entity or a different internal business unit that is required to have its own unique Snowflake account.

With secure data sharing:

There is only one copy of the data that lives in the data provider's account.
Shared data is always live, real-time, and immediately available to consumers.
Providers can establish revocable, fine-grained access to shares.
Data sharing is simple and safe, especially compared to older data sharing methods, which were often manual and insecure, such as transferring large .csv files across the internet.
Cross-region & cross-cloud data sharing To share data across regions or cloud platforms, you must set up replication. This is outside the scope of this lab, but more information is available in this Snowflake article. https://www.snowflake.com/trending/what-is-data-replication

View Existing Shares
In the home page, navigate to Data > Databases. In the list of databases, look at the SOURCE column. You should see two databases with Local in the column. These are the two databases we created previously in the lab. The other database, SNOWFLAKE, shows Share in the column, indicating it's shared from a provider.

Create an Outbound Share
Let's go back to the Citi Bike story and assume we are the Account Administrator for Snowflake at Citi Bike. We have a trusted partner who wants to analyze the data in our TRIPS database on a near real-time basis. This partner also has their own Snowflake account in the same region as our account. So let's use secure data sharing to allow them to access this information.

Navigate to Data > Private Sharing, then at the top of the tab click Shared by My Account. Click the Share button in the top right corner and select Create a Direct Share.

Click + Select Data and navigate to the CITIBIKE database and PUBLIC schema. Select the 2 tables we created in the schema and click the Done button.

The default name of the share is a generic name with a random numeric value appended. Edit the default name to a more descriptive value that will help identify the share in the future (e.g. ZERO_TO_SNOWFLAKE_SHARED_DATA. You can also add a comment.

In a real-world scenario, the Citi Bike Account Administrator would next add one or more consumer accounts to the share, but we'll stop here for the purposes of this lab.

Click the Create Share button at the bottom of the dialog:

Snowflake Data Marketplace
Make sure you're using the ACCOUNTADMIN role and, navigate to the Marketplace.

Resetting Your Snowflake Environment
If you would like to reset your environment by deleting all the objects created as part of this lab, run the SQL statements in a worksheet.

First, ensure you are using the ACCOUNTADMIN role in the worksheet:
```
use role accountadmin;
```
Then, run the following SQL commands to drop all the objects we created in the lab:

drop share if exists zero_to_snowflake_shared_data;

```
-- If necessary, replace "zero_to_snowflake-shared_data" with the name you used for the share

drop database if exists citibike;

drop database if exists weather;

drop warehouse if exists analytics_wh;

drop role if exists junior_dba;
```