# DEPLOYMENT.md — AWS Deployment Guide (Lambda + EventBridge + RDS + Secrets Manager)

This guide describes how to deploy the **daily incremental ETL** pipelines to AWS:

- `etl/daily_load/weather_etl_daily.py`
- `etl/daily_load/violation_etl_daily.py`

The production design uses:

- **AWS Lambda** for execution
- **Amazon EventBridge** for scheduling
- **AWS Secrets Manager** for secure DB credential injection
- **Amazon RDS (MySQL)** as persistent relational storage

> ✅ No secrets are stored in the repository. Secrets are injected at runtime.

---

## 1) Prerequisites

### AWS resources

- An **RDS MySQL** instance is running and reachable from Lambda (VPC + Security Group rules).
- A database exists (e.g., `mis664_project`) and required tables are created.

### Local requirements (optional)

- AWS Console access and permission to create IAM roles, Lambda functions, EventBridge rules, and Secrets.

---

## 2) Database Setup (One-time)

Run the SQL to create tables (recommended order):

1) Create DB + tables

   - `sql/create_tables.sql`

2) (Optional) validate analytical queries

   - `sql/analysis_queries.sql`

---

## 3) Create a Secrets Manager Secret (One-time)

Create a secret in **AWS Secrets Manager** to store DB connection values.

### Secret name (example)

- `mis664_project_db_secret`

### Secret value (JSON)

Use a JSON object similar to:

```json
{
  "host": "YOUR_RDS_ENDPOINT",
  "port": 3306,
  "dbname": "mis664_project",
  "username": "YOUR_DB_USER",
  "password": "YOUR_DB_PASSWORD"
}

```

Tip: Keep the secret JSON keys consistent with your code.
If your code expects different keys, adjust either the secret JSON or the code mapping.

---

## 4) Create an IAM Role for Lambda (One-time)

Create an IAM Role for Lambda with:

Trust policy: Lambda service can assume this role

Permissions:

- Read from Secrets Manager
- Write logs to CloudWatch
- (If Lambda is in a VPC) Manage network interfaces for VPC access

### Required policies (minimum)

Attach:

- `AWSLambdaBasicExecutionRole` (CloudWatch logs)
- Secrets Manager read permission (custom policy below)
- `AWSLambdaVPCAccessExecutionRole` (only if Lambda runs in VPC)

#### Secrets Manager read policy (custom)

Replace region/account/secret name pattern as needed:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadSecret",
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue",
        "secretsmanager:DescribeSecret"
      ],
      "Resource": "arn:aws:secretsmanager:*:*:secret:*"
    }
  ]
}
```
If you want stricter security, scope Resource to the exact secret ARN.

---

## 5) Package and Deploy Lambda

You will create two Lambda functions:


- `weather-etl-daily`
- `violations-etl-daily`


### 5.1 Decide dependency strategy

Your ETL code likely depends on some third-party libraries (common examples):

- `requests`
- `pandas`
- `pymysql` or `mysql-connector-python`
- `boto3` (already available in Lambda runtime)

Option A (recommended): Lambda Layer

- Put heavy libs like pandas in a Layer
- Keep function package small and simple

Option B: Package dependencies inside a ZIP

- Zip your code with dependencies
- Simpler for a class project; can hit size limits if using pandas

If your current daily ETL code uses pandas, expect to use a Layer or container-based Lambda.

### 5.2 Handler configuration

Your Python file must expose a Lambda handler function like:

```python
def lambda_handler(event, context):
```
If your file name is `weather_etl_daily.py`:

- Handler should be: `weather_etl_daily.lambda_handler`

For violations:

- `violation_etl_daily.lambda_handler`

### 5.3 Environment variables (per function)

Shared (both Lambdas)

- `AWS_REGION` = e.g., `us-east-2`
- `DB_SECRET_NAME` = e.g., `mis664_project_db_secret`

Weather Lambda only

- `WEATHER_API_KEY` = your VisualCrossing key
- `WEATHER_LOCATION` = e.g., `Washington,DC`

Do not store these values in code or repo.

### 5.4 Networking (VPC) configuration

If your RDS is not publicly accessible (recommended), configure Lambda:

- VPC: same VPC as RDS
- Subnets: private subnets (or subnets with route to NAT if needed)
- Security Group: allow outbound to RDS port 3306
- RDS Security Group must allow inbound from the Lambda Security Group on port 3306.

---

## 6) Create EventBridge Rules (Scheduling)

Create two EventBridge rules:

### 6.1 Weather daily schedule (example)

- Run daily at 06:00 UTC (adjust to your preference)
- EventBridge Schedule expression example:
  
  `cron(0 6 * * ? *)`

- Target:
  
  - `weather-etl-daily` Lambda

### 6.2 Violations daily schedule (example)

- Run daily at 06:10 UTC
- Expression:
  
  `cron(10 6 * * ? *)`

- Target:
  
  - `violations-etl-daily` Lambda

Stagger schedules to avoid concurrent DB load.

---

## 7) Verification Checklist

After deployment, verify:

### 7.1 CloudWatch Logs

- Each Lambda invocation produces logs
- No stack traces or timeout errors

### 7.2 Database table updates

Run checks (examples):

```sql
-- Weather latest date
SELECT MAX(weather_date) FROM weather_daily;

-- Violations latest date
SELECT MAX(violation_date) FROM violations;

-- Row counts
SELECT COUNT(*) FROM weather_daily;
SELECT COUNT(*) FROM violations;

```

### 7.3 Idempotency test (no duplicates)

Trigger the same Lambda twice and confirm:

- No duplicate primary keys inserted
- Updates are safe (UPSERT / key checks)

---

## 8) Common Issues & Fixes

Issue A: AccessDeniedException for Secrets Manager

- Cause: IAM role missing secretsmanager:GetSecretValue
- Fix: Attach/adjust Secrets Manager permission policy.

Issue B: Lambda can't connect to RDS (timeout)

- Cause: VPC/Security Group/Subnet misconfiguration
- Fix checklist:
  
  - Lambda is in same VPC as RDS
  - RDS SG allows inbound from Lambda SG on 3306
  - Lambda has AWSLambdaVPCAccessExecutionRole

Issue C: Package too large (especially with pandas)

- Cause: Zipped dependencies exceed Lambda limits
- Fix: Use a Lambda Layer, or container image-based Lambda.

Issue D: Missing env variables

- Cause: DB_SECRET_NAME, AWS_REGION, WEATHER_API_KEY, WEATHER_LOCATION not set
- Fix: Add them in Lambda → Configuration → Environment variables.

---

## 9) Recommended Repo Add-ons (Optional, Portfolio Boosters)

- ARCHITECTURE.md: one-page system diagram + design rationale
- RUNBOOK.md: how to monitor failures + how to re-run safely
- requirements-lambda.txt: pin key dependencies for packaging
- Makefile: helper commands for packaging ZIPs

---

## 10) Security Notes

- Do not commit .env files.
- Do not store credentials in code.
- Secrets are retrieved at runtime via Secrets Manager.
- Prefer least-privilege IAM policies when possible.

---

## Contact / Notes

This deployment guide is intended for portfolio demonstration and reproducible setup.  
For a production setting, consider:

- CloudWatch alarms + SNS notifications
- CI/CD deployment (GitHub Actions)
- Secrets rotation policies