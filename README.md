# HR Analytics Pipeline on Azure

An end-to-end data engineering pipeline that ingests raw HR source data (employees, departments, attendance) and transforms it through a **Medallion Architecture** (Bronze → Silver → Gold) using Azure Databricks and PySpark, orchestrated with Azure Data Factory.

Built as a hands-on portfolio project to demonstrate practical Azure data engineering skills: schema design, data quality handling, governed storage access, and pipeline orchestration.

## Architecture

```
Source CSVs (departments, employees, attendance)
        │
        ▼
┌─────────────────┐
│  BRONZE LAYER    │  Raw ingestion — faithful, untouched copy of source data
│  (Delta tables)  │  01_HR_Bronze_Ingestion.py
└────────┬─────────┘
         ▼
┌─────────────────┐
│  SILVER LAYER    │  Cleaned, validated, business-ready data
│  (Delta tables)  │  02_HR_Silver_Transformation.py
└────────┬─────────┘
         ▼
┌─────────────────┐
│  GOLD LAYER      │  Aggregated business metrics
│  (Delta tables)  │  03_HR_Gold_Aggregation.py
└──────────────────┘

Orchestrated end-to-end by: PL_HR_Bronze_to_Gold (Azure Data Factory)
```

Storage: **ADLS Gen2**, accessed via a **Unity Catalog External Location** (Storage Credential + Azure Databricks Access Connector) — no storage account keys are used anywhere in the pipeline.

## Tech stack

| Layer | Technology |
|---|---|
| Storage | Azure Data Lake Storage Gen2 |
| Compute / Transformation | Azure Databricks, PySpark |
| Table format | Delta Lake |
| Governance | Unity Catalog (Storage Credentials, External Locations) |
| Orchestration | Azure Data Factory |

## What each layer does

**Bronze** — reads the 3 source CSVs and lands them as Delta tables with no transformation, preserving an auditable copy of exactly what the source system sent.

**Silver** — cleans text fields, adds a derived `is_department_lead` flag (rather than treating manager_id nulls as missing data), and reconstructs correct attendance timestamps by combining date and time fields. Includes a data quality check that flags any negative or unrealistic (>16h) shift durations before writing.

**Gold** — joins and aggregates Silver data into two business-ready tables:
- `department_summary` — headcount and average salary per department
- `employee_attendance_summary` — days present/absent/WFH/leave and total/average hours worked per employee

## Key technical decisions

**`inferSchema` silently corrupts time-only columns.** Letting Spark infer the schema of `attendance.csv` causes `check_in_time`/`check_out_time` (values like `"08:15:00"`) to be misread as `timestamp` type, with Spark silently attaching the *current system date* instead of the actual attendance date — no error, no warning. Fixed by defining an explicit `StructType` schema in Bronze to keep these as strings, then deliberately reconstructing correct timestamps in Silver using `concat_ws()` + `try_to_timestamp()`.

**`try_to_timestamp` over `to_timestamp`.** On ABSENT/LEAVE days, the time columns are null, which produces a date-only string with no time component. `to_timestamp` throws a hard parsing error on this input; `try_to_timestamp` returns `NULL` instead — the correct behavior for genuinely missing data.

**Nulls with business meaning are preserved, not replaced.** `manager_id` is null for department leads — this is valid data, not missing data. Rather than replacing it with a placeholder value (which would break the self-referencing join semantics), Silver adds a derived `is_department_lead` boolean column to make the null self-documenting.

**Governed storage access over account keys.** Initial Bronze access used a raw storage account key via `spark.conf.set()`. This was replaced with a proper Unity Catalog Storage Credential (backed by an Azure Databricks Access Connector with `Storage Blob Data Contributor` IAM role) and an External Location — no secrets are stored or referenced in any notebook.

**ADF dependency conditions verified explicitly.** The orchestration pipeline's activity dependencies were checked to confirm they use `Succeeded` conditions (not `Skipped`), ensuring Silver and Gold notebooks only execute if the upstream layer genuinely completed successfully — not merely "did not fail."

## Repository structure

```
notebooks/
├── 01_HR_Bronze_Ingestion.py
├── 02_HR_Silver_Transformation.py
└── 03_HR_Gold_Aggregation.py
adf/
└── PL_HR_Bronze_to_Gold.json
sample_data/
├── departments.csv
├── employees.csv
└── attendance.csv
```

## Running this project

1. Upload the CSVs in `sample_data/` to an ADLS Gen2 container
2. Set up a Unity Catalog Storage Credential + External Location pointing to that container (see Microsoft's [Unity Catalog external locations guide](https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/external-locations))
3. Import the 3 notebooks into a Databricks workspace, update `storage_account_name` / `container_name` to match your environment
4. Import `adf/PL_HR_Bronze_to_Gold.json` into an Azure Data Factory instance, create a Databricks linked service, and update the `notebookPath` values to match your workspace
5. Run the pipeline via Debug or trigger it manually

## Author

Anand Koujalagi — Senior Data Engineer
