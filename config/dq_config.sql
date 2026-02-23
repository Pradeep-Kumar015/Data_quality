create or replace table demo_db.public.dq_config
(
family string,
database_name string,
schema_name string,
table_name string,
rule_id string,
column_names string,
min_value int,
max_value int,
threshold float,
severity string,
is_active boolean,
created_by string,
create_date timestamp
)