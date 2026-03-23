from snowflake.snowpark.functions import col, length


# --------------------------------------------------
# NUMERIC RANGE VALIDATION
# Example: column BETWEEN min_val AND max_val
# --------------------------------------------------
def execute_range(df, column_name, min_val, max_val):

    failed_df = df.filter(
        (col(column_name) < min_val) |
        (col(column_name) > max_val)
    )

    failed_count = failed_df.count()

    rule_expression = (
        f"{column_name} BETWEEN {min_val} AND {max_val}"
    )

    return failed_df, failed_count, rule_expression


# --------------------------------------------------
# MIN LENGTH VALIDATION
# Example: LENGTH(column) >= min_len
# --------------------------------------------------
def execute_min_length(df, column_name, min_len):

    failed_df = df.filter(
        length(col(column_name)) < min_len
    )

    failed_count = failed_df.count()

    rule_expression = (
        f"LENGTH({column_name}) >= {min_len}"
    )

    return failed_df, failed_count, rule_expression