# Databricks notebook source
# MAGIC %md
# MAGIC # HR Analytics Pipeline — Silver Layer
# MAGIC Reads raw Bronze Delta tables and produces cleaned, validated, business-ready
# MAGIC versions: text standardization, meaningful null-handling, and reconstruction
# MAGIC of correct timestamps for the attendance data.

# COMMAND ----------

storage_account_name = "retailanalyticsadls"
container_name = "bronzedata"

bronze_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Departments — text cleaning

# COMMAND ----------

df_departments_bronze = spark.read.format("delta").load(f"{bronze_path}/departments")
df_departments_bronze.show()

# COMMAND ----------

from pyspark.sql.functions import col, trim, initcap

df_departments_silver = df_departments_bronze \
    .withColumn("department_name", trim(col("department_name"))) \
    .withColumn("location", trim(initcap(col("location"))))

df_departments_silver.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation test
# MAGIC The source data was already clean, so the transformation above produces
# MAGIC no visible change. To verify the cleaning logic actually works, it's applied
# MAGIC here to a deliberately messy synthetic row.

# COMMAND ----------

from pyspark.sql import Row

test_df = spark.createDataFrame([
    Row(department_id=99, department_name="  engineering  ", location="bengaluru")
])

test_df.withColumn("department_name", trim(col("department_name"))) \
       .withColumn("location", trim(initcap(col("location")))) \
       .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Employees — text cleaning + meaningful null-handling
# MAGIC `manager_id` is null for department leads. Rather than treat this as missing
# MAGIC data, it's flagged with a derived `is_department_lead` boolean column so the
# MAGIC null is self-documenting for downstream consumers.

# COMMAND ----------

from pyspark.sql.functions import col, trim, initcap, when

df_employees_bronze = spark.read.format("delta").load(f"{bronze_path}/employees")

df_employees_silver = df_employees_bronze \
    .withColumn("employee_name", trim(initcap(col("employee_name")))) \
    .withColumn("designation", trim(col("designation"))) \
    .withColumn("is_department_lead", when(col("manager_id").isNull(), True).otherwise(False))

df_employees_silver.show()
df_employees_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write departments and employees to Silver

# COMMAND ----------

silver_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver"

df_departments_silver.write.format("delta").mode("overwrite").save(f"{silver_path}/departments")
df_employees_silver.write.format("delta").mode("overwrite").save(f"{silver_path}/employees")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Attendance — reconstructing correct timestamps
# MAGIC In Bronze, `check_in_time` / `check_out_time` are kept as plain strings
# MAGIC (e.g. `"08:15:00"`) to avoid Spark's `inferSchema` silently misparsing them
# MAGIC with today's date. Here, they're deliberately recombined with the correct
# MAGIC `attendance_date` to build valid timestamps.

# COMMAND ----------

df_attendance_bronze = spark.read.format("delta").load(f"{bronze_path}/attendance")
df_attendance_bronze.show(5)
df_attendance_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC `try_to_timestamp` (rather than `to_timestamp`) is used because `concat_ws`
# MAGIC produces a date-only string (e.g. `"2025-06-02"`, no time part) on rows where
# MAGIC `check_in_time` is null (ABSENT/LEAVE days). `to_timestamp` would throw a hard
# MAGIC parsing error on those rows; `try_to_timestamp` returns `NULL` instead, which
# MAGIC is the correct, intended behavior here.

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, try_to_timestamp, lit

df_attendance_silver = df_attendance_bronze \
    .withColumn(
        "check_in_datetime",
        try_to_timestamp(concat_ws(" ", col("attendance_date"), col("check_in_time")), lit("yyyy-MM-dd HH:mm:ss"))
    ) \
    .withColumn(
        "check_out_datetime",
        try_to_timestamp(concat_ws(" ", col("attendance_date"), col("check_out_time")), lit("yyyy-MM-dd HH:mm:ss"))
    )

df_attendance_silver.select("attendance_date", "check_in_time", "check_in_datetime", "check_out_time", "check_out_datetime").show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Derive hours_worked

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, try_to_timestamp, lit, round as spark_round, unix_timestamp

df_attendance_silver = df_attendance_silver \
    .withColumn(
        "hours_worked",
        spark_round((unix_timestamp(col("check_out_datetime")) - unix_timestamp(col("check_in_datetime"))) / 3600, 2)
    )

df_attendance_silver.select("attendance_date", "status", "check_in_datetime", "check_out_datetime", "hours_worked").show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality check
# MAGIC Confirm no negative or unrealistic (>16h) shift durations before writing.

# COMMAND ----------

df_attendance_silver.filter((col("hours_worked") < 0) | (col("hours_worked") > 16)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write attendance to Silver

# COMMAND ----------

df_attendance_silver.write.format("delta").mode("overwrite").save(f"{silver_path}/attendance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify — read all three Silver tables back

# COMMAND ----------

spark.read.format("delta").load(f"{silver_path}/departments").show()
spark.read.format("delta").load(f"{silver_path}/employees").show()
spark.read.format("delta").load(f"{silver_path}/attendance").select("attendance_date", "status", "check_in_datetime", "check_out_datetime", "hours_worked").show(10)
