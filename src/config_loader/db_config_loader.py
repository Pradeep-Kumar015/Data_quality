from snowflake.snowpark.functions import col
from src.checks import completeness, uniqueness, validity


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

        return {
            "DQ_001": completeness.execute,
            "DQ_002": uniqueness.execute,
            "DQ_003": validity.execute_range,
            "DQ_004": validity.execute_min_length
        }