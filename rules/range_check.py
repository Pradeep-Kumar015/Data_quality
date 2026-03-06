from snowflake.snowpark.functions import col

def execute(df, column_name, min_val, max_val):
    failed_df = df.filter(
        (col(column_name) < min_val) |
        (col(column_name) > max_val)
    )
    failed_count = failed_df.count()
    rule_expression = f"{column_name} BETWEEN {min_val} AND {max_val}"

    return failed_df, failed_count, rule_expression