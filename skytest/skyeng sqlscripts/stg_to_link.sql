insert into lnk_course_X_stream( course_id, stream_id, lnk_cs_load_dts, lnk_cs_rec_src)
select 
	stg.course_id
	stg.stream_id, 
	now(), 
	'skying.bd', 
from skyeng.stg_stream stg
left join skyeng.lnk_course_X_stream tgt
	on (stg.stream_id = tgt.stream_id
	and stg.course_id = tgt.course_id)
where tgt.stream_id is null
and tgt.course_id is null;

insert into lnk_stream_X_stream_module(  stream_id, stream_module_id, lnk_sxsm_load_dts, lnk_sxsm_rec_src)
select 
	stg.stream_id, 
	stg.stream_module_id,
	now(), 
	'skying.bd', 
from skyeng.stg_stream_module stg
left join skyeng.lnk_stream_X_stream_module tgt
	on (stg.stream_id = tgt.stream_id
	and stg.stream_module_id = tgt.stream_module_id)
where tgt.stream_id is null
and tgt.stream_module_id is null;

insert into lnk_stream_module_x_lesson( stream_module_id, stream_module_lesson_id, lnk_cs_load_dts, lnk_cs_rec_src)
select 
	stg.stream_module_idm
	stg.stream_module_lesson_id, 
	now(), 
	'skying.bd', 
from skyeng.stg_stream_module_lesson stg
left join skyeng.lnk_stream_module_x_lesson tgt
	on (stg.stream_module_id = tgt.stream_module_id
	and stg.stream_module_lesson_id = tgt.stream_module_lesson_id)
where tgt.stream_module_id is null
or tgt.stream_module_lesson_id is null;
 
insert into lnk_stream_module_lesson_x_teacher( stream_module_lesson_id, teacher_id, lnk_cs_load_dts, lnk_cs_rec_src)
select 
	stg.stream_module_lesson_id
	stg.teacher_id, 
	now(), 
	'skying.bd', 
from skyeng.stg_stream_module_lesson stg
left join skyeng.lnk_stream_module_lesson_x_teacher tgt
	on (stg.stream_module_lesson_id = tgt.stream_module_lesson_id
	and stg.teacher_id = tgt.teacher_id)
where tgt.stream_id is null
or tgt.course_id is null