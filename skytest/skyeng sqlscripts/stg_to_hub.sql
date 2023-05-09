insert into skyeng.hub_course( id, crs_load_dts, crs_rec_src)
select 
	stg.id, 
	now(), 
	'skying.bd', 
from skyeng.stg_coures stg
left join skyeng.hub_course tgt
	on stg.id = tgt.id
where tgt.id is null;

insert into skyeng.hub_stream( id, crs_load_dts, crs_rec_src)
select 
	stg.id, 
	now(), 
	'skying.bd', 
from skyeng.stg_stream stg
left join skyeng.hub_stream tgt
	on stg.id = tgt.id
where tgt.id is null;

insert into skyeng.hub_stream_module( id, crs_load_dts, crs_rec_src)
select 
	stg.id, 
	now(), 
	'skying.bd', 
from skyeng.stg_stream_module stg
left join skyeng.hub_stream_module tgt
	on stg.id = tgt.id
where tgt.id is null;

insert into skyeng.hub_stream_module_lesson( id, crs_load_dts, crs_rec_src)
select 
	stg.id, 
	now(), 
	'skying.bd', 
from skyeng.stg_stream_module_lesson stg
left join skyeng.hub_stream_module_lesson tgt
	on stg.id = tgt.id
where tgt.id is null;
