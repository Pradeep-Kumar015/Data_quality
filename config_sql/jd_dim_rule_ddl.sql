CREATE OR REPLACE TABLE Dim_RULE (
    RULE_ID              NUMBER ,
    RULE_NAME            STRING,
    RULE_TYPE            STRING,   -- NOT_NULL, BINARY, COMPARISON, RANGE, DOMAIN
    OPERATOR             STRING,   -- =, !=, >, <, >=, <=, BETWEEN, IN
    VALUE_1              STRING,   -- comparison value
    VALUE_2              STRING,   -- range upper value
    SEVERITY             STRING,   -- CRITICAL, HIGH, MEDIUM, LOW
    SEVERITY_WEIGHT      NUMBER,
    --DQ_DIMENSION         STRING,   -- COMPLETENESS, VALIDITY, ACCURACY
    IS_ACTIVE            BOOLEAN DEFAULT TRUE,
    CREATED_DATE         TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);