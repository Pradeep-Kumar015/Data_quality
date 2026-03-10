from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.email import send_email


def send_dq_failure_email():

    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    # Fetch FAIL records
    failures = hook.get_records("""
        SELECT 
            TABLE_NAME,
            COLUMN_NAME,
            RULE_TYPE,
            FAILED_RECORD_COUNT,
            FAILURE_PERCENTAGE,
            SEVERITY
        FROM DQ.DATA_QUALITY.DQ_RESULT_TABLE
        WHERE RULE_STATUS = 'FAIL'
    """)

    # ✅ If no failures → do nothing
    if not failures:
        print("No failed rules found. Email not sent.")
        return "No email sent"

    # ---------------------------------------
    # Build HTML Body
    # ---------------------------------------

    html_rows = ""

    for row in failures:
        html_rows += f"""
        <tr>
            <td>{row[0]}</td>
            <td>{row[1]}</td>
            <td>{row[2]}</td>
            <td align="right">{row[3]}</td>
            <td align="right">{row[4]}%</td>
            <td>{row[5]}</td>
        </tr>
        """

    html_body = f"""
    <html>
    <body style="font-family: Arial, sans-serif;">

        <h2 style="color:#d9534f;">
            Data Quality Alert - Failures Detected
        </h2>

        <table border="1" cellpadding="6" cellspacing="0"
               style="border-collapse: collapse; font-size: 14px;">

            <tr style="background-color:#f2f2f2;">
                <th>Table</th>
                <th>Column</th>
                <th>Rule</th>
                <th>Failed Count</th>
                <th>Failure %</th>
                <th>Severity</th>
            </tr>

            {html_rows}

        </table>

        <br>

        <p>Please review the DQ results in Snowflake.</p>

        <hr>

        <p style="font-size:12px;color:gray;">
            This is an automated message from Airflow.
        </p>

    </body>
    </html>
    """

    # ---------------------------------------
    # Send Email
    # ---------------------------------------

    send_email(
        to=["C-Jerin.Dougles@UDX.com"],
        subject="DQ Alert - Failed Rules Detected",
        html_content=html_body
    )

    return "Failure email sent"