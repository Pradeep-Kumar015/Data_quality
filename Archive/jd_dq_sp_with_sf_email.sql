CREATE OR REPLACE PROCEDURE RUN_DQ_FOR_INVENTORY()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run_dq_for_inventory'
AS
$$

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import uuid
from datetime import datetime

def run_dq_for_inventory(session: Session):

    run_id = str(uuid.uuid4())
    table_name = "INVENTORY"

    html_rows = ""

    try:

        # -----------------------------------
        # Start Run
        # -----------------------------------
        session.sql(f"""
        INSERT INTO RESULT_RUN_MASTER
        (RUN_ID, TABLE_NAME, RUN_STATUS, START_TIME)
        VALUES
        ('{run_id}', '{table_name}', 'STARTED', CURRENT_TIMESTAMP)
        """).collect()


        configs = session.table("FACT_OBJECT") \
            .filter(col("IS_ACTIVE") == True) \
            .filter(col("TABLE_NAME") == table_name) \
            .collect()


        for config in configs:

            rule_start = datetime.now()

            rule_id = config["RULE_ID"]

            rule = session.table("DIM_RULE") \
                .filter(col("RULE_ID") == rule_id) \
                .filter(col("IS_ACTIVE") == True) \
                .collect()[0]


            rule_type = rule["RULE_TYPE"]
            operator = rule["OPERATOR"]
            value1 = rule["VALUE_1"]
            value2 = rule["VALUE_2"]
            severity = rule["SEVERITY"] if rule["SEVERITY"] else "MEDIUM"

            database = config["TABLE_CATALOG"]
            schema = config["TABLE_SCHEMA"]
            table = config["TABLE_NAME"]
            column = config["COLUMN_NAME"]

            threshold = config["THRESHOLD_PCT"]

            full_table = f"{database}.{schema}.{table}"


            # -----------------------------------
            # CONDITION BUILDING
            # IMPORTANT: VALUE already has quotes
            # -----------------------------------

            error_message = None

            try:

                if rule_type == "NOT_NULL":

                    condition = f"{column} IS NOT NULL"
                    failure_condition = f"{column} IS NULL"


                elif rule_type == "COMPARISON":

                    condition = f"{column} {operator} {value1}"
                    failure_condition = f"NOT ({condition}) OR {column} IS NULL"


                elif rule_type == "RANGE":

                    condition = f"{column} BETWEEN {value1} AND {value2}"
                    failure_condition = f"NOT ({condition}) OR {column} IS NULL"


                elif rule_type == "BINARY":

                    condition = f"{column} {operator} {value1}"
                    failure_condition = f"NOT ({condition}) OR {column} IS NULL"


                elif rule_type == "DOMAIN":

                    condition = f"{column} IN ({value1})"
                    failure_condition = f"{column} IS NULL OR {column} NOT IN ({value1})"


                else:

                    error_message = f"Unsupported rule type {rule_type}"
                    condition = "1=1"
                    failure_condition = "1=0"


            except Exception as ex:

                error_message = str(ex)
                condition = "1=1"
                failure_condition = "1=0"


            # -----------------------------------
            # SINGLE SCAN COUNT (FAST)
            # -----------------------------------

            result = session.sql(f"""
                SELECT
                COUNT(*) TOTAL,
                SUM(CASE WHEN {failure_condition} THEN 1 ELSE 0 END) FAILED
                FROM {full_table}
            """).collect()[0]


            total = result[0]
            failed = result[1]

            passed = total - failed

            failure_pct = (failed/total*100) if total > 0 else 0

            breached = failure_pct > threshold

            status = "FAIL" if breached else "PASS"


            # -----------------------------------
            # FAILED SAMPLE (SAFE STRING)
            # -----------------------------------

            failed_sample_sql = "NULL"

            failed_sample = session.sql(f"""
                SELECT {column}
                FROM {full_table}
                WHERE {failure_condition}
                LIMIT 1
            """).collect()

            if failed_sample:

                val = failed_sample[0][0]

                if val is not None:

                    safe_val = str(val).replace("'", "''")

                    failed_sample_sql = "'" + safe_val + "'"


            # -----------------------------------
            # ERROR MESSAGE SAFE
            # -----------------------------------

            if error_message:
                safe_error = error_message.replace("'", "''")
                error_sql = "'" + safe_error + "'"
            else:
                error_sql = "NULL"


            safe_condition = condition.replace("'", "''")


            # -----------------------------------
            # INSERT RESULT
            # -----------------------------------

            session.sql(f"""
            INSERT INTO DQ_RESULT_TABLE
            (
            RULE_ID,
            RULE_TYPE,
            DATABASE_NAME,
            SCHEMA_NAME,
            TABLE_NAME,
            COLUMN_NAME,
            RULE_EXPRESSION,
            THRESHOLD,
            SEVERITY,
            TOTAL_RECORD_COUNT,
            FAILED_RECORD_COUNT,
            PASSED_RECORD_COUNT,
            FAILURE_PERCENTAGE,
            RULE_STATUS,
            IS_THRESHOLD_BREACHED,
            START_TIME,
            END_TIME,
            EXECUTION_DURATION_SEC,
            QUERY_ID,
            WAREHOUSE_NAME,
            SOURCE_TYPE,
            SOURCE_LOCATION,
            FAILED_SAMPLE_DATA,
            ERROR_MESSAGE,
            IS_ACTIVE,
            EXECUTED_BY,
            EXECUTION_MODE,
            CREATED_TIMESTAMP,
            UPDATED_TIMESTAMP
            )
            VALUES
            (
            '{rule_id}',
            '{rule_type}',
            '{database}',
            '{schema}',
            '{table}',
            '{column}',
            '{safe_condition}',
            {threshold},
            '{severity}',
            {total},
            {failed},
            {passed},
            {round(failure_pct,2)},
            '{status}',
            {str(breached).upper()},
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP,
            0,
            LAST_QUERY_ID(),
            CURRENT_WAREHOUSE(),
            'TABLE',
            '{full_table}',
            {failed_sample_sql},
            {error_sql},
            TRUE,
            CURRENT_USER(),
            'AUTO',
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
            )
            """).collect()


            # -----------------------------------
            # BUILD EMAIL HTML
            # -----------------------------------

            if breached:

                html_rows += f"""
                <tr>
                <td>{table}</td>
                <td>{column}</td>
                <td>{rule_type}</td>
                <td>{failed}</td>
                <td>{round(failure_pct,2)}%</td>
                <td>{severity}</td>
                </tr>
                """


        # -----------------------------------
        # SEND EMAIL IF FAILURES
        # -----------------------------------

        if html_rows != "":

            html_body = f"""
            <html>
            <body>
            <h2>DQ Alert - Failures Detected</h2>
            <table border="1">
            <tr>
            <th>Table</th>
            <th>Column</th>
            <th>Rule</th>
            <th>Failed</th>
            <th>Failure %</th>
            <th>Severity</th>
            </tr>
            {html_rows}
            </table>
            </body>
            </html>
            """

            safe_html = html_body.replace("'", "''")

            session.sql(f"""
            CALL SYSTEM$SEND_EMAIL(
            'DQ_EMAIL_INT',
            'C-Jerin.Dougles@UDX.com',
            'DQ Alert',
            '{safe_html}',
            'text/html'
            )
            """).collect()


        session.sql(f"""
        UPDATE RESULT_RUN_MASTER
        SET RUN_STATUS='COMPLETED',
        END_TIME=CURRENT_TIMESTAMP
        WHERE RUN_ID='{run_id}'
        """).collect()


        return "DQ Run Completed " + run_id


    except Exception as e:

        session.sql(f"""
        UPDATE RESULT_RUN_MASTER
        SET RUN_STATUS='FAILED',
        END_TIME=CURRENT_TIMESTAMP
        WHERE RUN_ID='{run_id}'
        """).collect()

        return "DQ Run Failed " + str(e)

$$;
