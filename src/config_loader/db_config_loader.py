from snowflake.snowpark.functions import col


class DBConfigLoader:

    def __init__(self, session):
        self.session = session

    def load_active_rules(self):

        dq_config_df = (
            self.session.table("DEMO_DB.PUBLIC.DQ_CONFIG")
            .filter(col("IS_ACTIVE") == True)
        )

        return dq_config_df

    def load_rule_lookup(self):

        rows = self.session.table("DEMO_DB.PUBLIC.DQ_RULES").collect()

        return {
            r["RULE_ID"]: r["RULE_NAME"]
            for r in rows
        }