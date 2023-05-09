insert into skyeng.sat_course( course_id, title, effective_from, effective_to, deleted_at, icon_url, is_auto_course_enroll, is_demo_enroll, sat_crs_load_dts )
select 
	stg.cousre_id, 
	stg.title,
	stg.created_at,
	to_date('9999-12-31','YYYY-MM-DD'),
	stg.deleted_at,
	stg.icon_url,
	stg.is_auto_corse_enroll,
	stg.is_demo_enroll
	now() 
from skyeng.stg_coures stg
left join skyeng.sat_course tgt
	on stg.id = tgt.course_id
where tgt.course_id is null;

insert into skyeng.sat_stream( stream_id, start_dt, end_dt, is_open, stream_name, homework_deadline_days, effective_from, effective_to, deleted_at, sat_strm_load_dts )
select 
	stg.stream_id, 
	stg.start_dt,
	stg.end_dt,
	stg.is_open,
	stg.stream_name,
	stg.homework_deadline_days,
	stg.created_at,
	to_date('9999-12-31','YYYY-MM-DD'),
	stg.deleted_at,
	now() 
from skyeng.stg_stream stg
left join skyeng.sat_stream tgt
	on stg.id = tgt.stream_id
where tgt.stream_id is null;

insert into skyeng.sat_stream_module( stream_module_id, order_in_stream, title, effective_from, effective_to, deleted_at, sat_sml_load_dts )
select 
	stg.stream_module_id, 
	stg.order_in_stream,
	stg.title,
	stg.created_at,
	to_date('9999-12-31','YYYY-MM-DD'),
	stg.deleted_at,
	now() 
from skyeng.stg_stream_module stg
left join skyeng.sat_stream_module tgt
	on stg.id = tgt.stream_module_id
where tgt.stream_module_id is null;

insert into skyeng.sat_stream_module_lesson( stream_module_lesson_id, title, description, start_at, end_at, homework_url, online_lesson_recording_url, online_lesson_join_url, deleted_at, sat_sml_load_dts )
select 
	stg.stream_module_lesson_id, 
	stg.title,
	stg.description,
	stg.start_at,
	stg.end_at,
	stg.homework_url,
	stg.online_lesson_recording_url,
	stg.online_lesson_join_url
	stg.deleted_at,
	now() 
from skyeng.stg_stream_module_lesson stg
left join skyeng.sat_stream_module_lesson tgt
	on stg.id = tgt.stream_module_lesson_id
where tgt.stream_module_lesson_id is null;