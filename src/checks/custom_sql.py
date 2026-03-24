def execute(session, sql_query):

    failed_df = session.sql(sql_query)

    failed_count = failed_df.count()

    rule_expression = sql_query

    return failed_df, failed_count, rule_expression