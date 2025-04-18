drop table if exists public.location;

create table public.location(
    locationid int primary key generated always as identity,
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

/**************************************************************
insert into public.locations(cityname, region, country, latitude, longitude, timezone, localtime_epoch, local_time)
select 'Royse City',
       'Texas',
	   'USA',
	   32.97,
	   -96.3,
	   'America/Chicago',
	   '1705022349',
	   '2024-01-11 19:19';
	   
select *
from public.locations;
 **************************************************************/
