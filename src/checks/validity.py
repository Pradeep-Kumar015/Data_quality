from snowflake.snowpark.functions import col, length


# --------------------------------------------------
# RANGE CHECK
# --------------------------------------------------
def execute_range(df, column_name, min_val, max_val):

    try:

        min_val = float(min_val)
        max_val = float(max_val)

        failed_df = df.filter(
            (col(column_name).cast("DOUBLE") < min_val) |
            (col(column_name).cast("DOUBLE") > max_val)
        )

    except Exception:
        # Non-numeric column -> no failures
        failed_df = df.limit(0)

    failed_count = failed_df.count()

    rule_expression = (
        f"{column_name} BETWEEN {min_val} AND {max_val}"
    )

    return failed_df, failed_count, rule_expression


# --------------------------------------------------
# MIN LENGTH CHECK
# --------------------------------------------------
def execute_min_length(df, column_name, min_len):

    min_len = int(min_len)

    failed_df = df.filter(
        length(col(column_name)) < min_len
    )

    failed_count = failed_df.count()

    rule_expression = (
        f"LENGTH({column_name}) >= {min_len}"
    )

    return failed_df, failed_count, rule_expression


# --------------------------------------------------
# VALID VALUE CHECK
# --------------------------------------------------
def execute_valid_value_check(df, column_name, valid_values):

    column_name = (column_name or "").strip()

    if valid_values is None:
        valid_values = []

    # Convert string "[A,B,C]" or "A,B,C" to list
    if isinstance(valid_values, str):

        valid_values = (
            valid_values
            .replace("[", "")
            .replace("]", "")
            .split(",")
        )

        valid_values = [
            x.strip()
            for x in valid_values
            if x.strip()
        ]

    failed_df = df.filter(
        ~col(column_name).isin(valid_values)
    )

    failed_count = failed_df.count()

    rule_expression = (
        f"{column_name} IN ({','.join(map(str, valid_values))})"
    )

    return failed_df, failed_count, rule_expression