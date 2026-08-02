# Databricks notebook source
# MAGIC %md
# MAGIC # HR Analytics Pipeline — Gold Layer
# MAGIC Reads cleaned Silver Delta tables and produces aggregated, business-ready
# MAGIC summary tables: department-level headcount/salary metrics and per-employee
# MAGIC attendance summaries.

# COMMAND ----------

storage_account_name = "retailanalyticsadls"
container_name = "bronzedata"

silver_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver"
gold_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold table 1: department_summary
# MAGIC Joins employees to departments, then aggregates headcount and average
# MAGIC salary per department.

# COMMAND ----------

df_employees_silver = spark.read.format("delta").load(f"{silver_path}/employees")
df_departments_silver = spark.read.format("delta").load(f"{silver_path}/departments")

# COMMAND ----------

from pyspark.sql.functions import col, count, avg, round as spark_round

df_department_summary = df_employees_silver.join(
    df_departments_silver,
    on="department_id",
    how="inner"
)

df_department_summary.show()

# COMMAND ----------

df_gold_department_summary = df_department_summary.groupBy("department_id", "department_name", "location") \
    .agg(
        count("employee_id").alias("headcount"),
        spark_round(avg("salary"), 2).alias("avg_salary")
    )

df_gold_department_summary.show()

# COMMAND ----------

df_gold_department_summary.write.format("delta").mode("overwrite").save(f"{gold_path}/department_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold table 2: employee_attendance_summary
# MAGIC Per-employee: days present/absent/WFH/leave (via conditional-sum pattern),
# MAGIC total hours worked, and average hours per working day.

# COMMAND ----------

df_attendance_silver = spark.read.format("delta").load(f"{silver_path}/attendance")
df_attendance_silver.show(5)

# COMMAND ----------

from pyspark.sql.functions import col, count, sum as spark_sum, avg, round as spark_round, when

df_gold_employee_attendance = df_attendance_silver.groupBy("employee_id") \
    .agg(
        count("attendance_id").alias("total_days_recorded"),
        spark_sum(when(col("status") == "PRESENT", 1).otherwise(0)).alias("days_present"),
        spark_sum(when(col("status") == "ABSENT", 1).otherwise(0)).alias("days_absent"),
        spark_sum(when(col("status") == "WFH", 1).otherwise(0)).alias("days_wfh"),
        spark_sum(when(col("status") == "LEAVE", 1).otherwise(0)).alias("days_leave"),
        spark_round(spark_sum("hours_worked"), 2).alias("total_hours_worked"),
        spark_round(avg("hours_worked"), 2).alias("avg_hours_per_working_day")
    )

df_gold_employee_attendance.show()

# COMMAND ----------

df_gold_employee_attendance.write.format("delta").mode("overwrite").save(f"{gold_path}/employee_attendance_summary")
