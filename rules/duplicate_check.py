from snowflake.snowpark.functions import col

def execute(df, column_name, min_val=None, max_val=None):
    col_list = [c.strip() for c in column_name.split(",")]

    dup_df = (
        df.group_by([col(c) for c in col_list])
          .count()
          .filter(col("COUNT") > 1)
    )

    failed_count = dup_df.count()
    rule_expression = f"DUPLICATE CHECK ON ({column_name})"

    return dup_df, failed_count, rule_expression