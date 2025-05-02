# 🛠️ ETL Sales Pipeline with Apache Airflow

This project implements an end-to-end ETL pipeline using **Apache Airflow**, **AWS S3**, **AWS Glue**, and **Amazon Redshift**. It automates the daily ingestion, transformation, and loading of synthetic sales data.

---

## 📋 Pipeline Overview

### 1. `upload_sales_to_s3`
- **Generates** synthetic sales data (order ID, date, customer, amount).
- **Formats** it into Newline-Delimited JSON (`.ndjson`).
- **Uploads** the data to an Amazon S3 bucket (`raw/` folder).

### 2. `run_glue_job`
- **Triggers** an AWS Glue job (`clean_raw_sales_data`).
- The job is assumed to **transform and clean** raw S3 data and move it to a `processed/` folder.

### 3. `load_to_redshift`
- **Loads** the processed data from S3 into an **Amazon Redshift** table using the `COPY` command.
- Requires a pre-configured **IAM Role** with Redshift COPY permissions.

---

## 🔧 Technologies Used

- **Airflow**: DAG orchestration and scheduling
- **AWS S3**: Raw and processed data storage
- **AWS Glue**: Data transformation
- **Amazon Redshift**: Data warehouse for loading final records
- **Python**: Task logic
- **Boto3**: AWS SDK for Python

---

## 🗓️ DAG Configuration

| Parameter        | Value                          |
|------------------|---------------------------------|
| `dag_id`         | `etl_sales_pipeline`            |
| `schedule`       | Daily at 2:00 AM UTC (`0 2 * * *`) |
| `start_date`     | DAG starts from current date    |
| `catchup`        | `False`                         |
| `tags`           | `etl`, `s3`, `glue`, `redshift` |

---

## 🔐 AWS and Airflow Connections

- `aws_default`: AWS access configured in Airflow
- `redshift_conn_id`: Connection ID pointing to Redshift (must be pre-configured)
- IAM Role ARN required in Redshift for `COPY FROM S3`

---

## 📁 S3 Structure

```
sales-data-pipeline-buckett/
├── raw/
│   └── orders_YYYY-MM-DD.json
└── processed/
    └── cleaned_sales_YYYY-MM-DD.json (output from Glue job)
```

---

## ✅ Setup Checklist

- [ ] Create S3 bucket `sales-data-pipeline-buckett`
- [ ] Create and configure Redshift cluster and table (`public.sales_orders`)
- [ ] Configure IAM role with Redshift and S3 access
- [ ] Define Airflow connections: `aws_default`, `redshift_conn_id`
- [ ] Create AWS Glue job named `clean_raw_sales_data`

---

## 📦 Running the Pipeline

1. Place `etl_sales_pipeline.py` into your Airflow DAGs directory.
2. Ensure AWS credentials and Redshift access are configured in Airflow.
3. Trigger the DAG manually or let it run based on its schedule.

---

## 📜 License

This project is licensed for educational and internal use. Customize it as needed for production deployment.
