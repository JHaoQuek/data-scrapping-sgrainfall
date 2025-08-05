
-- Rainfall Distribution across Singapore

select 
	area as "Planning Area"
	,station_name as "Area"
	,cast(json_extract_path_text(REPLACE(location, '''', '"'), 'latitude') as float) as latitude
	,cast(json_extract_path_text(REPLACE(location, '''', '"'), 'longitude') as float) as longitude
	,sum(rainfall) as Rainfall 
from rainfall_data
group by 
	area
	,station_name
	,json_extract_path_text(REPLACE(location, '''', '"'), 'latitude')
	,json_extract_path_text(REPLACE(location, '''', '"'), 'longitude')
order by Rainfall desc 
;



-- SG Top 10 Highest Rainfall

select 
	area as "Planning Area"
	,sum(rainfall) as Rainfall 
from rainfall_data
group by 
	area
order by Rainfall desc 
limit 10 
;



-- SG Top 10 Lowest Rainfall


select 
	area as "Planning Area"
	,sum(rainfall) as Rainfall 
from rainfall_data
group by 
	area
order by Rainfall  
limit 10 
;



-- Hourly rainfall distribution across Singapore

select 
	area
	,sum(total_rainfall) as Rainfall 
from rainfall_data_view
where 
	{{date}}
group by 
	area
order by 
	area 
;

