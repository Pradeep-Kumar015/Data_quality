from snowflake.snowpark.functions import col, length

def execute(df, column_name, min_val, max_val=None):
    failed_df = df.filter(
        col(column_name).is_not_null() &
        (length(col(column_name)) < min_val)
    )
    failed_count = failed_df.count()
    rule_expression = f"LENGTH({column_name}) >= {min_val}"

    return failed_df, failed_count, rule_expression