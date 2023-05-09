update skyeng.sat_course
set 
	effective_to = tmp.update_dt - interval '1 second'
from (
	select 
		stg.id, 
		stg.update_dt 
	from skyeng.stg_course stg
	inner join skyeng.sat_course tgt
		on stg.id = tgt.course_id
		and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
	where stg.title <> tgt.title or ( stg.title is null and tgt.title is not null ) or ( stg.title is not null and tgt.title is null )
	   or stg.icon_url <> tgt.icon_url or ( stg.icon_url is null and tgt.icon_url is not null ) or ( stg.icon_url is not null and tgt.icon_url is null )
	   or stg.is_auto_course_enroll <> tgt.is_auto_course_enroll or ( stg.is_auto_course_enroll is null and tgt.is_auto_course_enroll is not null ) or ( stg.is_auto_course_enroll is not null and tgt.is_auto_course_enroll is null )
	   or stg.is_demo_enroll <> tgt.is_demo_enroll or ( stg.is_demo_enroll is null and tgt.is_demo_enroll is not null ) or ( stg.is_demo_enroll is not null and tgt.is_demo_enroll is null )
) tmp
where skyeng.sat_course.course_id = tmp.id
  and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD'); 
 
insert into skyeng.sat_course( course_id, title, effective_from, effective_to, deleted_at, icon_url, is_auto_course_enroll, is_demo_enroll, sat_crs_load_dts )
select 
	stg.cousre_id, 
	stg.title,
	stg.updated_at,
	to_date('9999-12-31','YYYY-MM-DD'),
	stg.deleted_at,
	stg.icon_url,
	stg.is_auto_course_enroll,
	stg.is_demo_enroll
	now() 
from skyeng.stg_coures stg
inner join skyeng.sat_course tgt
on stg.id = tgt.course_id
where stg.title <> tgt.title or ( stg.title is null and tgt.title is not null ) or ( stg.title is not null and tgt.title is null )
   or stg.icon_url <> tgt.icon_url or ( stg.icon_url is null and tgt.icon_url is not null ) or ( stg.icon_url is not null and tgt.icon_url is null )
   or stg.is_auto_course_enroll <> tgt.is_auto_course_enroll or ( stg.is_auto_course_enroll is null and tgt.is_auto_course_enroll is not null ) or ( stg.is_auto_course_enroll is not null and tgt.is_auto_course_enroll is null )
   or stg.is_demo_enroll <> tgt.is_demo_enroll or ( stg.is_demo_enroll is null and tgt.is_demo_enroll is not null ) or ( stg.is_demo_enroll is not null and tgt.is_demo_enroll is null );


update skyeng.sat_stream
set 
	effective_to = tmp.update_dt - interval '1 second'
from (
	select 
		stg.id, 
		stg.update_dt 
	from skyeng.stg_stream stg
	inner join skyeng.sat_stream tgt
		on stg.id = tgt.stream_id
		and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
	where   stg.start_at <> tgt.start_at or ( stg.start_at is null and tgt.start_at is not null ) or ( stg.start_at is not null and tgt.start_at is null )
       or   stg.end_at <> tgt.end_at or ( stg.end_at is null and tgt.end_at is not null ) or ( stg.end_at is not null and tgt.end_at is null )
       or   stg.is_open <> tgt.is_open or ( stg.is_open is null and tgt.is_open is not null ) or ( stg.is_open is not null and tgt.is_open is null )
  	   or   stg.stream_name <> tgt.stream_name or ( stg.stream_name is null and tgt.stream_name is not null ) or ( stg.stream_name is not null and tgt.stream_name is null )
       or   stg.homework_deadline_days <> tgt.homework_deadline_days or ( stg.homework_deadline_days is null and tgt.homework_deadline_days is not null ) or ( stg.homework_deadline_days is not null and tgt.homework_deadline_days is null )
) tmp
where skyeng.sat_stream.stream_id = tmp.id
  and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD'); 

insert into skyeng.sat_stream( stream_id, start_dt, end_dt, is_open, stream_name, homework_deadline_days,  effective_from, effective_to, deleted_at, sat_strm_load_dts )
select 
	stg.stream_id, 
	stg.start_dt,
	stg.end_dt,
	stg.is_open,
	stg.stream_name,
	stg.homework_deadline_days,
	stg.updated_at,
	to_date('9999-12-31','YYYY-MM-DD'),
	stg.deleted_at,
	now() 
from skyeng.stg_stream stg
inner join skyeng.sat_stream tgt
on stg.id = tgt.stream_id
where   stg.start_at <> tgt.start_at or ( stg.start_at is null and tgt.start_at is not null ) or ( stg.start_at is not null and tgt.start_at is null )
   or   stg.end_at <> tgt.end_at or ( stg.end_at is null and tgt.end_at is not null ) or ( stg.end_at is not null and tgt.end_at is null )
   or   stg.is_open <> tgt.is_open or ( stg.is_open is null and tgt.is_open is not null ) or ( stg.is_open is not null and tgt.is_open is null )
   or   stg.stream_name <> tgt.stream_name or ( stg.stream_name is null and tgt.stream_name is not null ) or ( stg.stream_name is not null and tgt.stream_name is null )
   or   stg.homework_deadline_days <> tgt.homework_deadline_days or ( stg.homework_deadline_days is null and tgt.homework_deadline_days is not null ) or ( stg.homework_deadline_days is not null and tgt.homework_deadline_days is null );

update skyeng.sat_stream_module
set 
	effective_to = tmp.update_dt - interval '1 second'
from (
	select 
		stg.id, 
		stg.update_dt 
	from skyeng.stg_stream_module stg
	inner join skyeng.sat_stream_module tgt
		on stg.id = tgt.stream_module_id
		and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
	where  stg.order_in_stream <> tgt.order_in_stream or ( stg.order_in_stream is null and tgt.order_in_stream is not null ) or ( stg.order_in_stream is not null and tgt.order_in_stream is null )
	   or  stg.title <> tgt.title or ( stg.title is null and tgt.title is not null ) or ( stg.title is not null and tgt.title is null 
) tmp
where skyeng.sat_stream.stream_module_id = tmp.id
  and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD'); 

insert into skyeng.sat_stream_module( stream_module_id, order_in_stream, title,effective_from, effective_to, deleted_at, sat_sml_load_dts )
select 
	stg.stream_module_id, 
	stg.order_in_stream,
	stg.title,
	stg.updated_at,
	to_date('9999-12-31','YYYY-MM-DD'),
	stg.deleted_at,
	now() 
from skyeng.stg_stream_module stg
inner join skyeng.sat_stream_module tgt
on stg.id = tgt.stream_module_id
where  stg.order_in_stream <> tgt.order_in_stream or ( stg.order_in_stream is null and tgt.order_in_stream is not null ) or ( stg.order_in_stream is not null and tgt.order_in_stream is null )
   or  stg.title <> tgt.title or ( stg.title is null and tgt.title is not null ) or ( stg.title is not null and tgt.title is null );
----------------------------------------
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
inner join skyeng.sat_stream_module_lesson tgt
on stg.id = tgt.stream_module_lesson_id
where stg.title <> tgt.title or ( stg.title is null and tgt.title is not null ) or ( stg.title is not null and tgt.title is null )
   or stg.description <> tgt.description or ( stg.description is null and tgt.description is not null ) or ( stg.description is not null and tgt.description is null )
   or stg.start_at <> tgt.start_at or ( stg.start_at is null and tgt.start_at is not null ) or ( stg.start_at is not null and tgt.start_at is null )
   or stg.end_at <> tgt.end_at or ( stg.end_at is null and tgt.end_at is not null ) or ( stg.end_at is not null and tgt.end_at is null )
   or stg.homework_url <> tgt.homework_url or ( stg.homework_url is null and tgt.homework_url is not null ) or ( stg.homework_url is not null and tgt.homework_url is null )
   or stg.online_lesson_recording_url <> tgt.online_lesson_recording_url or ( stg.online_lesson_recording_url is null and tgt.online_lesson_recording_url is not null ) or 
   ( stg.online_lesson_recording_url is not null and tgt.online_lesson_recording_url is null )
   or (tgt.deleted_at is not null and stg.deleted_at is null)
