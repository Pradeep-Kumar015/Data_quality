from snowflake.snowpark.functions import col, length

def execute(df, column_name, min_len):

    failed_df = df.filter(length(col(column_name)) < min_len)

    failed_count = failed_df.count()

    rule_expression = f"LENGTH({column_name}) >= {min_len}"

    return failed_df, failed_count, rule_expression