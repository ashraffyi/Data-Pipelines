from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
                                DropTablesOperator, CreateTablesOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
  # for more info see https://airflow.apache.org/docs/stable/_api/airflow/models/index.html#airflow.models.BaseOperator
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
  # 'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#Drop Tables

drop_tables_operator = DummyOperator(task_id='drop_tables',  dag=dag)

drop_table_staging_events = DropTablesOperator(
    task_id='drop_table_staging_events',
    dag=dag,
    target_table="staging_events"
)

drop_table_staging_songs = DropTablesOperator(
    task_id='drop_table_staging_songs',
    dag=dag,
    target_table="staging_songs"
)

drop_table_songplays = DropTablesOperator(
    task_id='drop_table_songplays',
    dag=dag,
    target_table="songplays"
)

drop_table_users = DropTablesOperator(
    task_id='drop_table_users',
    dag=dag,
    target_table="users"
)

drop_table_songs = DropTablesOperator(
    task_id='drop_table_songs',
    dag=dag,
    target_table="songs"
)

drop_table_artists = DropTablesOperator(
    task_id='drop_table_artists',
    dag=dag,
    target_table="artists"
)

drop_table_time = DropTablesOperator(
    task_id='drop_table_time',
    dag=dag,
    target_table="time"
)

create_tables_operator = DummyOperator(task_id='create_tables',  dag=dag)

create_tables_staging_events = CreateTablesOperator(
    task_id='create_tables_staging_events',
    dag=dag,
    sql=SqlQueries.staging_events_table_create,
    table = 'staging_events_table_create'
)

create_tables_staging_songs = CreateTablesOperator(
    task_id='create_tables_staging_songs',
    dag=dag,
    sql=SqlQueries.staging_songs_table_create,
    table = 'staging_songs_table_create'
)

create_tables_songplays = CreateTablesOperator(
    task_id='create_tables_songplays',
    dag=dag,
    sql=SqlQueries.songplays_table_create,
    table = 'songplays_table_create'
)

create_tables_users = CreateTablesOperator(
    task_id='create_tables_users',
    dag=dag,
    sql=SqlQueries.users_table_create,
    table = 'users_table_create'
    
)

create_tables_songs = CreateTablesOperator(
    task_id='create_tables_songs',
    dag=dag,
    sql=SqlQueries.songs_table_create,
    table = 'songs_table_create'
)

create_tables_artists = CreateTablesOperator(
    task_id='create_tables_artists',
    dag=dag,
    sql=SqlQueries.artists_table_create,
    table = 'artists_table_create'
)

create_tables_time = CreateTablesOperator(
    task_id='create_tables_time',
    dag=dag,
    sql=SqlQueries.time_table_create,
    table = 'time_table_create'
)


stage_events_operator = DummyOperator(task_id='stage_events',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    option="Log_data"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    option="Song_data"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    sql=SqlQueries.songplay_table_insert,
    table="songplays"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    sql=SqlQueries.user_table_insert,
    table="users"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    sql=SqlQueries.song_table_insert,
    table="songs",
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    sql=SqlQueries.artist_table_insert,
    table="artists",
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    sql=SqlQueries.time_table_insert,
    table="time",
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    tableNames=['songplays','users', 'songs','artists', 'time']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> drop_tables_operator                

drop_tables_operator >> drop_table_staging_events
drop_tables_operator >> drop_table_staging_songs
drop_tables_operator >> drop_table_songplays
drop_tables_operator >> drop_table_users                
drop_tables_operator >> drop_table_songs
drop_tables_operator >> drop_table_artists
drop_tables_operator >> drop_table_time     
                
drop_table_staging_events >> create_tables_operator
drop_table_staging_songs >> create_tables_operator
drop_table_songplays >> create_tables_operator
drop_table_users >> create_tables_operator                 
drop_table_songs >> create_tables_operator
drop_table_artists >> create_tables_operator
drop_table_time >> create_tables_operator

create_tables_operator >> create_tables_staging_events
create_tables_operator >> create_tables_staging_songs
create_tables_operator >> create_tables_songplays
create_tables_operator >> create_tables_users
create_tables_operator >> create_tables_songs
create_tables_operator >> create_tables_artists
create_tables_operator >> create_tables_time

create_tables_staging_events >> stage_events_operator
create_tables_staging_songs >> stage_events_operator
create_tables_songplays >> stage_events_operator
create_tables_users >> stage_events_operator
create_tables_songs >> stage_events_operator
create_tables_artists >> stage_events_operator
create_tables_time >> stage_events_operator


stage_events_operator >> stage_songs_to_redshift
stage_events_operator >> stage_events_to_redshift
                
stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_songplays_table >> load_user_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
