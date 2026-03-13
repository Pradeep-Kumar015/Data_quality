from snowflake.snowpark.functions import col


def execute(df, column_name):

    duplicate_df = (
        df.group_by(column_name)
        .count()
        .filter(col("COUNT") > 1)
    )

    failed_df = df.join(
        duplicate_df,
        column_name,
        "inner"
    )

    failed_count = failed_df.count()

    rule_expression = f"{column_name} UNIQUE"

    return failed_df, failed_count, rule_expression