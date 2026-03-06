from snowflake.snowpark.functions import col

def execute(df, column_name, min_val=None, max_val=None):
    failed_df = df.filter(col(column_name).is_null())
    failed_count = failed_df.count()
    rule_expression = f"{column_name} IS NOT NULL"

    return failed_df, failed_count, rule_expression