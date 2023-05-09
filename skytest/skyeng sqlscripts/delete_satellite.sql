insert into skyeng.sat_course( course_id, title, effective_from, effective_to, deleted_at, icon_url, is_auto_course_enroll, is_demo_enroll, sat_crs_load_dts )
select 
	stg.cousre_id, 
	stg.title,
	stg.deleted_at,
	to_date('9999-12-31','YYYY-MM-DD'),
	stg.deleted_at,
	stg.icon_url,
	stg.is_auto_course_enroll,
	stg.is_demo_enroll
	now() 
from skyeng.stg_coures stg
inner join skyeng.sat_course tgt
	 on stg.id = tgt.course_id
where stg.deleted_at is not null;

update skyeng.sat_course
set 
	effective_to = tmp.deleted_at
from (
	select 
		stg.id, 
		stg.deleted_at 
	from skyeng.stg_course stg
	inner join skyeng.sat_course tgt
		on stg.id = tgt.course_id
	where tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
	  and stg.deleted_at is not null) tmp
where skyeng.sat_course.effective_to  = to_date('9999-12-31','YYYY-MM-DD')
  and tmp.deleted_at is not null;
 
insert into skyeng.sat_stream( stream_id, start_dt, end_dt, is_open, stream_name, homework_deadline_days,  effective_from, effective_to, deleted_at, sat_strm_load_dts )
select 
	stg.stream_id, 
	stg.start_dt,
	stg.end_dt,
	stg.is_open,
	stg.stream_name,
	stg.homework_deadline_days,
	stg.deleted_at,
	to_date('9999-12-31','YYYY-MM-DD'),
	stg.deleted_at,
	now() 
from skyeng.stg_stream stg
inner join skyeng.sat_stream tgt
	 on stg.id = tgt.stream_id
where stg.deleted_at is not null;

update skyeng.sat_stream
set 
	effective_to = tmp.deleted_at
from (
	select 
		stg.id, 
		stg.deleted_at 
	from skyeng.stg_stream stg
	inner join skyeng.sat_stream tgt
		 on stg.id = tgt.stream_id
	where tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
	  and stg.deleted_at is not null) tmp
where skyeng.sat_stream.effective_to  = to_date('9999-12-31','YYYY-MM-DD')
  and tmp.deleted_at is not null;
 
insert into skyeng.sat_stream_module( stream_module_id, order_in_stream, title,effective_from, effective_to, deleted_at, sat_sml_load_dts )
select 
	stg.stream_module_id, 
	stg.order_in_stream,
	stg.title,
	stg.deleted_at,
	to_date('9999-12-31','YYYY-MM-DD'),
	stg.deleted_at,
	now() 
from skyeng.stg_stream_module stg
inner join skyeng.sat_stream_module tgt
	on stg.id = tgt.stream_module_id
where stg.deleted_at is not null;

update skyeng.sat_stream_module
set 
	effective_to = tmp.deleted_at
from (
	select 
		stg.id, 
		stg.deleted_at 
	from skyeng.stg_stream_module stg
	inner join skyeng.sat_stream_module tgt
		 on stg.id = tgt.stream_id
	where tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
	  and stg.deleted_at is not null) tmp
where skyeng.sat_stream_module.effective_to  = to_date('9999-12-31','YYYY-MM-DD')
  and tmp.deleted_at is not null;