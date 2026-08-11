----------------------------------QUERYS BÁSICAS---------------------------------
--------------------------------------------------------------------------------
---Query 1: Contar vehículos por tipo
 explain ANALYZE
select vehicle_type,
		count(vehicle_id) as Num_vehiculos
from vehicles 
group by vehicle_type; 

-- Query 2: Listar conductores con licencia próxima a vencer (30 dias o 60 dias)---
explain ANALYZE
select driver_id,first_name,last_name,license_expiry,status
from drivers 
where license_expiry - current_date  < 60  and status='active'
order by license_expiry asc;
-- Query 3: Total de viajes por estado
explain analyze
select status,count(trip_id) as Num_viajes
from trips
group by status

-- Query 4: Total de entregas por ciudad destino en los últimos 2 meses
explain analyze
select r.destination_city, count(d.delivery_id) as Num_entregas
from deliveries as d
join trips as t on t.trip_id=d.trip_id 
join routes as r on r.route_id=t.route_id 
where d.delivered_datetime >= current_date-interval '60 days'
group by r.destination_city 
order by num_entregas desc

-- Query 5: Conductores activos con cantidad de viajes completados
explain analyze
select d.driver_id,d.first_name,d.last_name,count(t.trip_id) as num_viajes_completados
from drivers as d
join trips as t on t.driver_id=d.driver_id 
where t.status='completed' and d.status='active'
group by d.driver_id,d.first_name,d.last_name
order by num_viajes_completados desc

-- Query 6: Promedio de entregas por conductor en los últimos 6 meses
explain analyze
with Entregas_conductor as
	(select dr.driver_id, 
			dr.first_name,
			dr.last_name,
			extract(month from d.delivered_datetime) as MONTH,
			COUNT(d.delivery_id) as NUM_ENTREGAS
	from deliveries as d 
	join trips as t on d.trip_id = t.trip_id
	join drivers as dr on dr.driver_id = t.driver_id 
	where d.delivery_status ='delivered' and  d.delivered_datetime >= current_date-interval '6 months'and dr.status= 'active'
	group by dr.driver_id, dr.first_name,dr.last_name, extract(month from d.delivered_datetime)
	order by driver_id asc)
select driver_id, first_name,last_name,cast(avg(num_entregas) as decimal(10,2)) as prom_entregas
from Entregas_conductor
group by driver_id, first_name,last_name


-- Query 7: Rutas con mayor consumo de combustible por kilómetro
explain analyze
select  r.route_id,
		r. origin_city,
		r.destination_city,
		sum(r.distance_km) as kilom_recorridos,
		sum(t.fuel_consumed_liters) as Consumo_total_combustible_lt,
		cast((sum(t.fuel_consumed_liters)/ sum(r.distance_km)) as decimal (10,4)) as consumo_por_Km
from routes as r
join trips as t on r.route_id=t.route_id 
group by r.route_id,r.origin_city,r.destination_city
order by consumo_por_km desc

-- Query 8: Análisis de entregas retrasadas por día de la semana
explain analyze
select TO_CHAR(delivered_datetime,'day') as dia_semana,
		count(delivery_id) as Num_pedidos_retrasados
from deliveries
where ABS(extract(epoch from ((delivered_datetime - scheduled_datetime)/60)))>= 60 and delivery_status='delivered'
group by TO_CHAR(delivered_datetime,'day')
ORDER BY EXTRACT(DOW FROM MIN(delivered_datetime));

-- Query 9: Costo de mantenimiento por kilómetro recorrido

explain analyze
with Kilometros_recorridos as
	(select   t.vehicle_id,
			  v.vehicle_type,
			  count(t.trip_id) as cant_viajes,
			   sum(r.distance_km) as kilom_recorridos
		from trips as t
		join routes as r on t.route_id=r.route_id
		join vehicles as v on t.vehicle_id= v.vehicle_id  
		where t.status='completed'
		group by t.vehicle_id,v.vehicle_type),
		
	metricas_mantenimiento as
		( select vehicle_id,
				count(maintenance_id) as mantenimientos,
				sum(cost) as costo_mantenimiento
		  from maintenance
		  group by vehicle_id)
	
	select kr.vehicle_type,
			sum(kr.cant_viajes) as Total_viajes,
			sum(mm.costo_mantenimiento) as costo_total,
			sum(kr.kilom_recorridos) as total_km,
		  sum(mm.costo_mantenimiento)/sum( kr.kilom_recorridos) as costo_mante_porKm
	from kilometros_recorridos as kr
	join metricas_mantenimiento as mm on kr.vehicle_id=mm.vehicle_id
	group by vehicle_type

	
--Query 10: Ranking de conductores por eficiencia usando Window Functions
-- Problema de negocio: Identificar top performers para incentivos
	explain analyze
	with metricas_conductores as
		(select t.driver_id,
			   dr.first_name,
			   dr.last_name,
			   to_char(DATE_TRUNC('month', delivered_datetime),'TMMonth') AS mes,
			   count(d.delivery_id ) as num_entregas,	
			   count(d.delivery_id)/count(distinct date(delivered_datetime)):: float as num_entrega_dia
		from trips as t
		join deliveries as d on t.trip_id=d.trip_id 
		join drivers as dr on dr.driver_id=t.driver_id
		where d.delivery_status='delivered' and d.delivered_datetime >= current_date - interval '6 months' and dr.status='active'
		group by DATE_TRUNC('month', delivered_datetime),t.driver_id,dr.first_name,dr.last_name),
		
		Ranking_Mensual as
		(select  mes,
			    first_name,
				last_name,
				num_entregas,
				cast(num_entrega_dia as decimal(10,2)) as promedio_entregas_diario,
				DENSE_RANK() OVER (PARTITION BY mes ORDER BY num_entregas DESC) AS posicion_ranking
		from metricas_conductores
		order by mes desc, posicion_ranking asc)
		
		SELECT * 
FROM Ranking_Mensual 
WHERE posicion_ranking <= 3
ORDER BY mes DESC, posicion_ranking ASC;
		
		

-- Query 11: Análisis de tendencia de viajes con LAG y LEAD	
explain analyze
with viajes_mensuales as
	(select date_trunc('month',t.arrival_datetime) as Mes,
			count(t.trip_id) as TOTAL_VIAJES
	from trips as t
	where t.status='completed'
	group by date_trunc('month',t.arrival_datetime)),
	
	tendencias as 
			(select Mes,
				    total_viajes as viajes_actuales,
				    lag(total_viajes) over (order by Mes) as viajes_mes_anterior,
				    lead(total_viajes) over (order by Mes) as viajes_mes_siguiente
			from viajes_mensuales)
select to_char(Mes,'FMTMM YYYY') as Periodo,
		viajes_actuales,
		viajes_mes_anterior,
		ROUND(
        ((viajes_actuales - viajes_mes_anterior)::numeric / NULLIF(viajes_mes_anterior, 0)) * 100
        ) as Porcentaje_crecimiento,
        ROUND(
        viajes_actuales * (1 + (viajes_actuales - viajes_mes_anterior)::numeric / NULLIF(viajes_mes_anterior, 0))
        ) AS proyeccion_sig_mes
 from tendencias
 order by mes desc;
    	
-- Query 12: Pivot de entregas por hora y día de la semana
explain analyze
	SELECT 
    EXTRACT(HOUR FROM delivered_datetime) AS hora,
    COUNT(*) FILTER (WHERE EXTRACT(DOW FROM delivered_datetime) = 1) AS Lunes,
    COUNT(*) FILTER (WHERE EXTRACT(DOW FROM delivered_datetime) = 2) AS Martes,
    COUNT(*) FILTER (WHERE EXTRACT(DOW FROM delivered_datetime) = 3) AS Miercoles,
    COUNT(*) FILTER (WHERE EXTRACT(DOW FROM delivered_datetime) = 4) AS Jueves,
    COUNT(*) FILTER (WHERE EXTRACT(DOW FROM delivered_datetime) = 5) AS Viernes,
    COUNT(*) FILTER (WHERE EXTRACT(DOW FROM delivered_datetime) = 6) AS Sabado,
    COUNT(*) FILTER (WHERE EXTRACT(DOW FROM delivered_datetime) = 0) AS Domingo
FROM deliveries
WHERE delivery_status = 'delivered'
GROUP BY EXTRACT(HOUR FROM delivered_datetime)
ORDER BY hora;

        
			
