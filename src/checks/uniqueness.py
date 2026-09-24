from snowflake.snowpark.functions import col, count
from snowflake.snowpark.window import Window


def execute(df, duplicate_columns):

    if not duplicate_columns:
        raise ValueError(
            "DQ_002 requires at least one column for duplicate checking."
        )

    # Create a window using all columns that participate
    # in the duplicate comparison.
    window_spec = Window.partition_by(
        *[col(column) for column in duplicate_columns]
    )

    # Identify every row that belongs to a duplicate group.
    # Example:
    #   A,A,A -> all 3 rows are failures
    #   B,B   -> both rows are failures
    #   C     -> not a failure
    duplicate_df = (
        df.with_column(
            "_DUPLICATE_COUNT",
            count("*").over(window_spec)
        )
        .filter(
            col("_DUPLICATE_COUNT") > 1
        )
    )

    # Number of rows that belong to duplicate groups.
    failed_count = duplicate_df.count()

    # Remove the temporary column from the failed output.
    failed_df = duplicate_df.drop("_DUPLICATE_COUNT")

    rule_expression = (
        "WHOLE ROW DUPLICATE CHECK "
        f"EXCLUDING TECHNICAL COLUMNS; "
        f"PARTITION BY {', '.join(duplicate_columns)}"
    )

    return failed_df, failed_count, rule_expression