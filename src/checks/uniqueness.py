from snowflake.snowpark.functions import count


def execute(df, column_name=None, key_columns=None):
    """
    Duplicate check.

    If key_columns is provided:
        Checks duplicates based on the supplied key columns.

    If column_name is provided:
        Checks duplicates for that column.

    Otherwise:
        Checks duplicates across all business columns.
    """

    if key_columns:
        if isinstance(key_columns, str):
            key_columns = [x.strip() for x in key_columns.split(",") if x.strip()]

        failed_df = (
            df.group_by(*key_columns)
              .agg(count("*").alias("DUP_COUNT"))
              .filter("DUP_COUNT > 1")
        )

        failed_count = failed_df.count()

        rule_expression = (
            f"Duplicate Check Across {key_columns}"
        )

    elif column_name:

        failed_df = (
            df.group_by(column_name)
              .agg(count("*").alias("DUP_COUNT"))
              .filter("DUP_COUNT > 1")
        )

        failed_count = failed_df.count()

        rule_expression = (
            f"{column_name} should be unique"
        )

    else:

        ignore_columns = [
            "LOAD_DATE",
            "CREATE_DATE",
            "UPDATE_DATE",
            "CREATED_TIMESTAMP",
            "UPDATED_TIMESTAMP"
        ]

        business_columns = [
            c
            for c in df.columns
            if c.upper() not in ignore_columns
        ]

        failed_df = (
            df.group_by(*business_columns)
              .agg(count("*").alias("DUP_COUNT"))
              .filter("DUP_COUNT > 1")
        )

        failed_count = failed_df.count()

        rule_expression = (
            f"Duplicate Check Across {business_columns}"
        )

    return failed_df, failed_count, rule_expression