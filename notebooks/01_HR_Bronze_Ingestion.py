# Databricks notebook source
# MAGIC %md
# MAGIC # HR Analytics Pipeline — Bronze Layer
# MAGIC Reads raw source CSVs (departments, employees, attendance) from ADLS Gen2
# MAGIC and lands them as-is into Delta tables. No transformation happens here —
# MAGIC Bronze is a faithful, auditable copy of the source data.
# MAGIC
# MAGIC Storage access is via a Unity Catalog External Location (governed access
# MAGIC through an Azure Databricks Access Connector + Storage Credential) — no
# MAGIC storage account keys are used or stored in this notebook.

# COMMAND ----------

storage_account_name = "retailanalyticsadls"
container_name = "bronzedata"

def abfss_path(container, path):
    return f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/{path}"

departments_path = abfss_path(container_name, "departments.csv")
employees_path = abfss_path(container_name, "employees.csv")
attendance_path = abfss_path(container_name, "attendance.csv")

bronze_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read departments.csv
# MAGIC Small, simple table — safe to use `inferSchema` here.

# COMMAND ----------

df_departments = spark.read.csv(
    departments_path,
    header=True,
    inferSchema=True
)

df_departments.show()
df_departments.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read employees.csv
# MAGIC `manager_id` has real nulls (department leads have no manager) — `inferSchema`
# MAGIC correctly infers this as `integer` with nullable values.

# COMMAND ----------

df_employees = spark.read.csv(
    employees_path,
    header=True,
    inferSchema=True
)

df_employees.show()
df_employees.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read attendance.csv — explicit schema required
# MAGIC **Data quality finding:** letting Spark `inferSchema` this file causes it to
# MAGIC misread `check_in_time` / `check_out_time` (values like `"08:15:00"`) as a
# MAGIC `timestamp` type. Since these are time-only strings with no date component,
# MAGIC Spark silently attaches **today's system date** instead of the correct
# MAGIC `attendance_date`, producing corrupted timestamps with no error or warning.
# MAGIC
# MAGIC Fix: define an explicit schema and keep these two columns as `StringType`,
# MAGIC so Bronze holds a faithful, untouched copy of the source text. The correct
# MAGIC timestamp reconstruction (combining date + time properly) happens
# MAGIC deliberately in the Silver layer.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

attendance_schema = StructType([
    StructField("attendance_id", IntegerType(), True),
    StructField("employee_id", IntegerType(), True),
    StructField("attendance_date", DateType(), True),
    StructField("status", StringType(), True),
    StructField("check_in_time", StringType(), True),
    StructField("check_out_time", StringType(), True)
])

df_attendance = spark.read.csv(
    attendance_path,
    header=True,
    schema=attendance_schema
)

df_attendance.show()
df_attendance.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write all three tables to Bronze (Delta)

# COMMAND ----------

df_departments.write.format("delta").mode("overwrite").save(f"{bronze_path}/departments")
df_employees.write.format("delta").mode("overwrite").save(f"{bronze_path}/employees")
df_attendance.write.format("delta").mode("overwrite").save(f"{bronze_path}/attendance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify — read back from Delta to confirm the write persisted correctly

# COMMAND ----------

spark.read.format("delta").load(f"{bronze_path}/departments").show()
spark.read.format("delta").load(f"{bronze_path}/employees").show()
spark.read.format("delta").load(f"{bronze_path}/attendance").show()
