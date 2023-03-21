```
create schema new_project
```
```

drop TABLE YELP_BUSINESSES

create TABLE YELP_BUSINESSES 
    (business_id varchar(100),
    name varchar(100),
    address varchar(100),
    city varchar(100),
    state varchar(100),
    postal_code varchar(100),
    latitude float,
    longitude float,
    stars float,
    review_count smallint,
    is_open smallint,
    attributes json,
    categories varchar(100),
    hours json,
    constraint pk_business_id primary key (business_id));
```
```
CREATE TABLE temperature (
    years smallint,
    tmin float,
    tmax float
);

 CREATE TABLE precipitation (
    years smallint,
    pcpn float);
    ```
    create TABLE yelp_businesses
    (business_id varchar(100),
    name varchar(100),
    address varchar(100),
    city varchar(100),
    state varchar(100),
    postal_code varchar(100),
    latitude float,
    longitude float,
    stars float,
    review_count smallint,
    is_open smallint,
    attributes jsonb,
    categories varchar(100),
    hours jsonb,
    constraint pk_business_id primary key (business_id));
   
   CREATE TABLE yelp_chekin 
   (business_id varchar(100),
    date timestamp,
    constraint fk_business_id foreign key (business_id) references yelp_businesses(business_id));

   
   CREATE table yelp_users (
    user_id varchar(100),
    name varchar(100),
    review_count varchar(100),
    yelping_since timestamp,
    useful int,
    funny int,
    cool int,
    elite varchar(100),
    friends int,
    fans int,
    average_stars float,
    compliment_hot int,
    compliment_more int,
    compliment_profile int,
    compliment_cute int,
    compliment_list int,
    compliment_note int,
    compliment_plain int,
    compliment_cool int,
    compliment_funny int,
    compliment_writer int,
    compliment_photos int,
    constraint pk_user_id primary key (user_id)
);

CREATE TABLE yelp_reviews (
    review_id varchar(100),
    user_id varchar(100),
    business_id varchar(100),
    stars float,
    useful int,
    funny int,
    cool int,
    text varchar(100), 
    date timestamp,
    constraint pk_review_id primary key (review_id), 
    constraint fk_user_id foreign key (user_id) references yelp_users(user_id),
    constraint fk_business_id foreign key (business_id) references yelp_businesses(business_id)
);

CREATE TABLE yelp_tips (
    user_id varchar(100),
    business_id varchar(100),
    text varchar(100),
    date timestamp,
    compliment_count int,
    constraint fk_user_id foreign key (user_id) references yelp_users(user_id),
    constraint fk_business_id foreign key (business_id) references yelp_businesses(business_id)
);

create table yelp_covid (
   business_id varchar(100),
   highlights varchar(100),
   "delivery_or_takeout" boolean,
   "Grubhub_enabled" boolean,
   "Call_To_Action_enabled" boolean,
   "Request_a_Quote_Enabled" boolean,
   "Covid_Banner" varchar(100),
   "Temporary_Closed_Until" varchar(100),
   "Virtual_Services_Offered" varchar(100),
   foreign key (business_id) references yelp_businesses(business_id)
);

select years, pcpn
from precipitation
where years between 1990 and 2000;
```