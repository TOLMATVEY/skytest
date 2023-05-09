#!usr/bin/python3
import psycopg2 as ps

# Создание необходимых таблиц,
# загрузка в таблицу мета начальных значений
def createDDL():
    try:
        connect1 = ps.connect(database = "mydb",
                              host =     "mydb-skyeng-chronosavant.ru",
                              user =     "tolmat",
                              password = "sarumanthewhite",
                              port =     "5432")
        
        cursor1 = connect1.cursor()
        connect1.autocommit = False
        
        cursor1.execute(''' drop table if exists skyeng.stg_course;
                            drop table if exists skyeng.stg_stream;
                            drop table if exists skyeng.stg_stream_module;
                            drop table if exists skyeng.stg_stream_module_lesson;
                        ''')
 
        cursor1.execute(''' drop table if exists skyeng.hub_course;
                            drop table if exists skyeng.hub_stream;
                            drop table if exists skyeng.hub_stream_module;
                            drop table if exists skyeng.hub_stream_module_lesson;
                            drop table if exists skyeng.sat_course;
                            drop table if exists skyeng.sat_stream;
                            drop table if exists skyeng.sat_stream_module;
                            drop table if exists skyeng.sat_stream_module_lesson;
                            
                            drop table if exists skyeng.lnk_course_X_stream;
                            drop table if exists skyeng.lnk_stream_module_x_lesson;
                            drop table if exists skyeng.lnk_stream_module_x_lesson;
                            drop table if exists skyeng.lnk_stream_module_lesson_x_teacher;
                        ''')
                                     

        cursor1.execute(''' CREATE TABLE "stream_module"(
                                "id" INTEGER NOT NULL,
                                "sm_load_dts" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
                                "sm_rec_src" VARCHAR(20) NOT NULL
                            );
                            ALTER TABLE
                                "stream_module" ADD PRIMARY KEY("id");
                            CREATE TABLE "stream_x_stream_module"(
                                "stream_id" INTEGER NOT NULL,
                                "stream_module_id" INTEGER NOT NULL,
                                "lnk_sxsm_load_dts" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
                                "lnk_sxsm_rec_src" VARCHAR(20) NOT NULL
                            );
                            CREATE TABLE "sat_stream_module_lesson"(
                                "stream_module_lesson_id" INTEGER NOT NULL,
                                "description" TEXT NOT NULL,
                                "start_at" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
                                "end_at" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
                                "homework_url" VARCHAR(500) NOT NULL,
                                "online_lesson_recording_url" VARCHAR(255) NOT NULL,
                                "oline_lesson_join_url" VARCHAR(255) NOT NULL,
                                "deleted_at" TIMESTAMP(0) WITHOUT TIME ZONE,
                                "sat_sml_load_dts" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
                                "sat_sml_rec_src" VARCHAR(20) NOT NULL,
                                "title" VARCHAR(255) NOT NULL
                            );
                            CREATE TABLE "stream_module_lesson_x_teacher"(
                                "stream_module_lesson_id" INTEGER NOT NULL,
                                "teacher_id" INTEGER NOT NULL,
                                "lnk_smlt_load_dts" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
                                "lnk_smlt_rec_src" VARCHAR(20) NOT NULL
                            );
                            CREATE TABLE "sat_stream"(
                                "stream_id" INTEGER NOT NULL,
                                "icon_url" VARCHAR(255) NOT NULL,
                                "is_auto_course_enroll" BOOLEAN NOT NULL,
                                "is_demo_enroll" BOOLEAN NOT NULL,
                                "created_at" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
                                "updated_at" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
                                "deleted_at" TIMESTAMP(0) WITHOUT TIME ZONE,
                                "sat_strm_load_dts" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
                                "stream_name" VARCHAR(255) NOT NULL
                            );
                            CREATE TABLE "sat_course"(
                                "course_id" INTEGER NOT NULL,
                                "start_at" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
                                "end_at" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
                                "is_open" BOOLEAN NOT NULL,
                                "homework_deadline_days" INTEGER NOT NULL,
                                "created_at" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
                                "updated_at" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
                                "deleted_at" TIMESTAMP(0) WITHOUT TIME ZONE,
                                "sat_crs_load_dts" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
                                "title" VARCHAR(255) NOT NULL
                            );
                            CREATE TABLE "stream"(
                                "id" INTEGER NOT NULL,
                                "strm_load_dts" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
                                "strm_rec_src" VARCHAR(20) NOT NULL
                            );
                            ALTER TABLE
                                "stream" ADD PRIMARY KEY("id");
                            CREATE TABLE "stream_module_x_lesson"(
                                "stream_module_id" INTEGER NOT NULL,
                                "stream_module_lesson_id" INTEGER NOT NULL,
                                "lnk_smxl_load_dts" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
                                "lnk_smxl_rec_src" VARCHAR(20) NOT NULL
                            );
                            CREATE TABLE "sat_stream_module"(
                                "stream_module_id" INTEGER NOT NULL,
                                "order_in_stream" INTEGER NOT NULL,
                                "created_at" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
                                "updated_at" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
                                "" TIMESTAMP(0) WITHOUT TIME ZONE,
                                "sat_sm_load_dts" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
                                "sat_sm_rec_dts" VARCHAR(20) NOT NULL,
                                "title" VARCHAR(255) NOT NULL
                            );
                            CREATE TABLE "course_x_stream"(
                                "course_id" INTEGER NOT NULL,
                                "stream_id" INTEGER NOT NULL,
                                "lnk_cs_load_dts" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
                                "lnk_cs_rec_src" VARCHAR(20) NOT NULL
                            );
                            CREATE TABLE "course"(
                                "id" INTEGER NOT NULL,
                                "crs_load_dts" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
                                "crs_rec_src" VARCHAR(20) NOT NULL
                            );
                            ALTER TABLE
                                "course" ADD PRIMARY KEY("id");
                            CREATE TABLE "stream_module_lesson"(
                                "id" INTEGER NOT NULL,
                                "sml_load_dts" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
                                "sml_rec_src" VARCHAR(20) NOT NULL
                            );
                            ALTER TABLE
                                "stream_module_lesson" ADD PRIMARY KEY("id");
                            ALTER TABLE
                                "course_x_stream" ADD CONSTRAINT "course_x_stream_stream_id_foreign" FOREIGN KEY("stream_id") REFERENCES "stream"("id");
                            ALTER TABLE
                                "stream_module_x_lesson" ADD CONSTRAINT "stream_module_x_lesson_stream_module_lesson_id_foreign" FOREIGN KEY("stream_module_lesson_id") REFERENCES "stream_module_lesson"("id");
                            ALTER TABLE
                                "stream_x_stream_module" ADD CONSTRAINT "stream_x_stream_module_stream_id_foreign" FOREIGN KEY("stream_id") REFERENCES "stream"("id");
                            ALTER TABLE
                                "stream_module_lesson_x_teacher" ADD CONSTRAINT "stream_module_lesson_x_teacher_stream_module_lesson_id_foreign" FOREIGN KEY("stream_module_lesson_id") REFERENCES "stream_module_lesson"("id");
                            ALTER TABLE
                                "sat_stream_module_lesson" ADD CONSTRAINT "sat_stream_module_lesson_stream_module_lesson_id_foreign" FOREIGN KEY("stream_module_lesson_id") REFERENCES "stream_module_lesson"("id");
                            ALTER TABLE
                                "stream_x_stream_module" ADD CONSTRAINT "stream_x_stream_module_stream_module_id_foreign" FOREIGN KEY("stream_module_id") REFERENCES "stream_module"("id");
                            ALTER TABLE
                                "stream_module_x_lesson" ADD CONSTRAINT "stream_module_x_lesson_stream_module_id_foreign" FOREIGN KEY("stream_module_id") REFERENCES "stream_module"("id");
                            ALTER TABLE
                                "sat_stream" ADD CONSTRAINT "sat_stream_stream_id_foreign" FOREIGN KEY("stream_id") REFERENCES "stream"("id");
                            ALTER TABLE
                                "sat_course" ADD CONSTRAINT "sat_course_course_id_foreign" FOREIGN KEY("course_id") REFERENCES "course"("id");
                            ALTER TABLE
                                "course_x_stream" ADD CONSTRAINT "course_x_stream_course_id_foreign" FOREIGN KEY("course_id") REFERENCES "course"("id");
                            ALTER TABLE
                                "sat_stream_module" ADD CONSTRAINT "sat_stream_module_stream_module_id_foreign" FOREIGN KEY("stream_module_id") REFERENCES "stream_module"("id");
                                
                            insert into skyeng.toma_meta_inf( schema_name, table_name, max_update_dt )
                            values ('info', 'course', to_timestamp('1900-01-01','YYYY-MM-DD')),
                            	   ('info', 'stream', to_timestamp('1900-01-01','YYYY-MM-DD')),
                            	   ('info', 'stream_module', to_timestamp('1900-01-01','YYYY-MM-DD')),
                            	   ('info', 'stream_module_Lesson', to_timestamp('1900-01-01','YYYY-MM-DD'));
                            
                            create view all_about_lesson(
                            	id int primary key,
                            	lesson_title varchar(255),
                            	start_at varchar(0),
                            	end_at varchar(0),
                            	teacher_fio varchar(255),
                            	stream_module_name varchar(255),
                            	stream_name varchar(255),
                            	course_name varchar(255),
                            	all_load_dts timestamp(0));
                        ''')
        connect1.commit()
    except (Exception, ps.Error) as error:
          return ("Ошибка при работе с PostgreSQL", error)
     
    finally:
        if connect1:                                             
            connect1.close()
            cursor1.close()
            return "Соединение с PostgreSQL закрыто"


print(createDDL())

