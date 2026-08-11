
-- Verificación integridad de la base de datos

select * from trips
where departure_datetime > arrival_datetime ;

select max(t.total_weight_kg) as max_weigh_kg ,
		v.vehicle_type,
		v.capacity_kg 
from trips as t
join vehicles as v on t.vehicle_id=v.vehicle_id 
group by v.vehicle_type, v.capacity_kg  

select min (t.total_weight_kg) as max_weigh_kg ,
		v.vehicle_type,
		v.capacity_kg 
from trips as t
join vehicles as v on t.vehicle_id=v.vehicle_id 
group by v.vehicle_type ,v.capacity_kg 

select route_code, distance_km, estimated_duration_hours from routes 
where origin_city = 'Bogotá' and destination_city='Barranquilla'

select * from trips