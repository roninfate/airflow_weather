drop table if exists public.location_stage;

create table public.location_stage(
    zipcode varchar(20) NULL, 
    cityname varchar(50) null,
    region varchar(50) null,
    country varchar(50) null, 
    latitude numeric(9,4) null,
    longitude numeric(9,4) null, 
    timezone varchar(100) null,
    localtime_epoch varchar(50) null,
    local_time timestamp null 
    );


