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
pattern string, --regex pattern for regex check
custom_sql string,  -- custom sql for custom sql check
threshold float,
severity string,
is_active boolean,
created_by string,
create_date timestamp
)