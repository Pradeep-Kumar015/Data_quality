 # ================= RULE LOGIC =================

        if rule_type == "NULL_CHECK":

            failed_df = df.filter(col(column_name).is_null())
            failed_count = failed_df.count()
            rule_expression = f"{column_name} IS NOT NULL"

        elif rule_type == "RANGE_CHECK":

            failed_df = df.filter(
                (col(column_name) < min_val) |
                (col(column_name) > max_val)
            )
            failed_count = failed_df.count()
            rule_expression = f"{column_name} BETWEEN {min_val} AND {max_val}"

        elif rule_type == "MIN_LENGTH_CHECK":

            failed_df = df.filter(
            col(column_name).is_not_null() &
            (length(col(column_name)) < min_val)  # the value is hard coded!!!!!!!!!!!!!!!!!!!!!!!!!
            )
            failed_count = failed_df.count()
            rule_expression = f"LENGTH({column_name}) >= {min_val}"

        elif rule_type == "DUPLICATE_CHECK":

            col_list = [c.strip() for c in column_name.split(",")]

            dup_df = (
                df.group_by([col(c) for c in col_list])
                  .count()
                  .filter(col("COUNT") > 1)
            )

            failed_df = dup_df
            failed_count = dup_df.count()
            rule_expression = f"DUPLICATE CHECK ON ({column_name})"


        #new rule types can be added here with additional elif blocks
        # 1. this rule apply only for string columns
        elif rule_type == "NOT_NULL_CHECK" and df.schema[column_name].dataType == StringType():

            failed_df = df.filter(
                col(column_name).is_null() |
                (col(column_name) == "") |
                (length(col(column_name)) == 0)
            )
            failed_count = failed_df.count()
            rule_expression = f"{column_name} IS NOT NULL AND NOT EMPTY"

        # 2. max length check for string columns
        elif rule_type == "MAX_LENGTH_CHECK":

            failed_df = df.filter(
                length(col(column_name)) > max_val
            )

            failed_count = failed_df.count()
            rule_expression = f"LENGTH({column_name}) <= {max_val}"

        # 3. Exact match check for string columns
        elif rule_type == "EXACT_LENGTH_CHECK":

            failed_df = df.filter(
                length(col(column_name)) != row["THRESHOLD"]
            )

            failed_count = failed_df.count()
            rule_expression = f"LENGTH({column_name}) = {row['THRESHOLD']}"
        
        # 4. positive number check for numeric columns
        elif rule_type == "POSITIVE_CHECK":

            failed_df = df.filter(col(column_name) <= 0)
            failed_count = failed_df.count()
            rule_expression = f"{column_name} > 0"

        # 5. regular expression check for string columns
        elif rule_type == "REGEX_CHECK":

            pattern = row["PATTERN"]  #regex pattern is stored in this column in config table
            failed_df = df.filter(
                #pattern should be another column in config table eg : "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$" for email validation
                ~regexp_like(col(column_name), pattern) 
            )

            failed_count = failed_df.count()
            rule_expression = f"{column_name} MATCHES {pattern}"

        # 6. future date check for date columns
        elif rule_type == "NOT_FUTURE_DATE_CHECK":

            failed_df = df.filter(col(column_name) > current_date())

            failed_count = failed_df.count()
            rule_expression = f"{column_name} <= CURRENT_DATE"

        # 7. custom SQL expression check - this allows users to write their own SQL expression in the config table 
        elif rule_type == "CUSTOM_SQL":

            custom_query = row["CUSTOM_SQL"]

            failed_df = session.sql(custom_query)
            failed_count = failed_df.count()
            rule_expression = f"CUSTOM SQL: {custom_query}"
        else:
            print(f"Unsupported rule type: {rule_type}")
            continue
