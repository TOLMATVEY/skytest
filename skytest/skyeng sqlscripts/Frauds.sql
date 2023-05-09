insert into all_about_lessons( id, lesson_title, start_at, end_at, lesson_deleted_at, teacher_fio, stream_module_name, module_is_deleted, stream_name, stream_is_deleted, course_name, course_is_deleted, all_load_dts)
select ssml.id,
	   ssml.title,
	   ssml.start_at,
	   ssml.end_at,
	   ssml.deleted_at,
	   'default', -- Пока в нашей схеме нет таблицы с учителями
	   ssm.title,
	   case when ssm.deleted_at is null then False else True end,
	   ss.stream_name,
	   case when ss.deleted_at is null then False else True end,
	   sc.title,
	   case when cs.deleted_at is null then False else True end,
	   now()
from stg_course sc
inner join stg_stream ss
	on sc.id = ss.course_id
inner join stg_stream_module ssm 
	on ssm.stream_id = ss.id
inner join stg_stream_module_lesson ssml 
	on ssml.stream_module_id = ssm.id