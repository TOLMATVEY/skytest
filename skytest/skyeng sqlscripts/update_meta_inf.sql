update skyeng.meta_inf
set max_update_dt = coalesce(select max(updated_at) from skyeng.sat_course ), max_update_dt)
where schema_name='info' and table_name = 'course';

update skyeng.meta_inf
set max_update_dt = coalesce(select max(updated_at) from skyeng.sat_stream ), max_update_dt)
where schema_name='info' and table_name = 'stream';

update skyeng.meta_inf
set max_update_dt = coalesce(select max(updated_at) from skyeng.sat_stream_module ), max_update_dt)
where schema_name='info' and table_name = 'stream_module';

update skyeng.meta_inf
set max_update_dt = coalesce(select max(end_dt) from skyeng.sat_stream_module_lesson), max_update_dt)
where schema_name='info' and table_name = 'stream_module_lesson';