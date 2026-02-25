# METHOD 1
#compare with yesterday's count to identify percentage drop
yesterday_df = session.table(source_table).filter(
    col("LOAD_DATE") == current_date() - 1
)

yesterday_count = yesterday_df.count()

count_drop_percentage = (
    (yesterday_count - total_count) / yesterday_count
    if yesterday_count > 0 else 0
)
# METHOD 2
#Detect Abnormal Drop Using Moving Average (Advanced)
history_df = session.table("DEMO_DB.PUBLIC.DQ_TABLE_VOLUME_HISTORY") \
    .filter(col("TABLE_NAME") == table) \
    .sort(col("LOAD_DATE").desc()) \
    .limit(7)

avg_count = history_df.select(avg(col("RECORD_COUNT"))).collect()[0][0]