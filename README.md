# dbt on AWS Glue with Airflow and GitHub Actions CI/CD

End-to-end setup guide (Windows + PowerShell + Docker Desktop). It covers a dbt project that reads raw CSV data from S3 through the Glue Data Catalog, transforms it with Spark on AWS Glue Interactive Sessions, is orchestrated by Airflow, and is tested and deployed through GitHub Actions.

## Contents

1. [Architecture](#1-architecture)
2. [Prerequisites](#2-prerequisites)
3. [Repository layout](#3-repository-layout)
4. [AWS account setup](#4-aws-account-setup)
5. [Python and dbt installation](#5-python-and-dbt-installation)
6. [dbt project and profile](#6-dbt-project-and-profile)
7. [First model](#7-first-model)
8. [Optional: Iceberg](#8-optional-iceberg)
9. [Git and GitHub](#9-git-and-github)
10. [Airflow in Docker](#10-airflow-in-docker)
11. [CI/CD with GitHub Actions](#11-cicd-with-github-actions)
12. [Daily workflow](#12-daily-workflow)
13. [Troubleshooting](#13-troubleshooting)
14. [Cost and cleanup](#14-cost-and-cleanup)
15. [dbt concepts roadmap](#15-dbt-concepts-roadmap)

Replace these placeholders everywhere:

| Placeholder | Meaning | Example |
|---|---|---|
| `<ACCOUNT_ID>` | 12-digit AWS account ID | `123456789012` |
| `<BUCKET>` | your S3 bucket | `my-dbt-lab-aravind` |
| `<GITHUB_USER>` | your GitHub username | |
| Region | used everywhere in this guide | `us-east-1` |

The S3 bucket, Glue databases, Athena and the Glue session role must all be in the **same region**.

---

## 1. Architecture

```
S3 raw CSV  ->  Glue Data Catalog (raw_db)
                      |
   dbt (dbt-glue adapter) -> Glue Interactive Session (Spark) -> S3 /dbt/ + Glue DB (dbt_dev / dbt_ci_pr_N / dbt_prod)
                      ^
        Airflow (Docker) runs dbt build on a schedule
                      ^
   GitHub Actions: CI on pull requests, CD on merge to main
```

dbt does not process data itself. It compiles SQL and sends it to Spark on Glue. Airflow only schedules dbt commands.

Three environments, separated by Glue database (schema) name:

| Environment | Who runs it | Glue database |
|---|---|---|
| dev | you, from your laptop | `dbt_dev` |
| ci | GitHub Actions, per pull request | `dbt_ci_pr_<N>` (temporary) |
| prod | Airflow | `dbt_prod` |

---

## 2. Prerequisites

| Tool | Version | Check |
|---|---|---|
| Python | **3.11** (not 3.12+) | `py -0` |
| AWS CLI v2 | recent | `aws --version` |
| Docker Desktop (WSL2 backend) | recent | `docker compose version` |
| Git | recent | `git --version` |
| VS Code (optional) | | |

Python 3.14/3.15 will fail to install dbt dependencies on Windows (`pyyaml` tries to compile and asks for C++ Build Tools). Install Python 3.11 next to your other versions:

```powershell
winget install Python.Python.3.11
py -0          # must list -V:3.11
```

---

## 3. Repository layout

```
dbtprojectonaws/                 <- Git repo root
  .github/workflows/ci.yml
  .github/workflows/cd.yml
  .gitignore
  requirements.txt
  env.ps1                        <- local only, git-ignored
  dbtbyaravind/                  <- dbt project
    dbt_project.yml
    profiles.yml                 <- no secrets, uses env vars
    models/ seeds/ snapshots/ macros/ tests/ analyses/
  airflow/
    Dockerfile
    docker-compose.yaml
    .env                         <- git-ignored
    dags/dbt_glue_prod.py
```

---

## 4. AWS account setup

### 4.1 IAM user and access key

Use an IAM user, never the root account.

1. Console -> **IAM -> Users -> Create user** (e.g. `dbt-lab-user`), no console access.
2. Attach permissions. For a lab, `AdministratorAccess` is simplest. For least privilege, attach the inline policy from section 4.5 plus S3 and Athena access.
3. Open the user -> **Security credentials -> Create access key -> Command Line Interface (CLI)**.
4. Copy the Access key ID (`AKIA...`) and Secret access key. The secret is shown **once**.
5. Never commit keys to Git or paste them into chats.

```powershell
aws configure
# Access key ID, Secret access key
# Default region name: us-east-1
# Default output format: json

aws sts get-caller-identity       # must print your account ID and user ARN
```

If your company uses SSO, use `aws configure sso` and `aws sso login` instead of long-lived keys.

### 4.2 S3 bucket and raw data

```powershell
$env:BUCKET = "<BUCKET>"
aws s3 mb "s3://$env:BUCKET" --region us-east-1

mkdir data ; cd data
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/raw_customers.csv" -OutFile raw_customers.csv
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/raw_orders.csv"    -OutFile raw_orders.csv
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/raw_payments.csv"  -OutFile raw_payments.csv
```

If a URL returns 404, download any customers/orders/payments CSVs manually. Upload each file into **its own folder** (tables point at folders, not files):

```powershell
aws s3 cp raw_customers.csv "s3://$env:BUCKET/raw/customers/raw_customers.csv"
aws s3 cp raw_orders.csv    "s3://$env:BUCKET/raw/orders/raw_orders.csv"
aws s3 cp raw_payments.csv  "s3://$env:BUCKET/raw/payments/raw_payments.csv"
aws s3 ls "s3://$env:BUCKET/raw/" --recursive
```

### 4.3 Glue databases and source tables

```powershell
aws glue create-database --database-input Name=raw_db
aws glue create-database --database-input Name=dbt_dev
aws glue create-database --database-input Name=dbt_prod
```

In the **Athena console** (same region; set a query result location such as `s3://<BUCKET>/athena-results/` first), run:

```sql
CREATE EXTERNAL TABLE raw_db.customers (
  id int, first_name string, last_name string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://<BUCKET>/raw/customers/'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE raw_db.orders (
  id int, user_id int, order_date date, status string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://<BUCKET>/raw/orders/'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE raw_db.payments (
  id int, order_id int, payment_method string, amount int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://<BUCKET>/raw/payments/'
TBLPROPERTIES ('skip.header.line.count'='1');
```

Verify: `SELECT * FROM raw_db.orders LIMIT 5;` returns rows, and

```powershell
aws glue get-tables --database-name raw_db --query "TableList[].Name"
```

lists the three tables.

### 4.4 IAM role for Glue sessions

dbt-glue starts Glue Interactive Sessions. The session runs under a role, which is a separate identity from your user.

| Identity | Purpose |
|---|---|
| Your user (access key) | asks Glue to start a session |
| Glue session role | used by Spark while the session runs (reads/writes S3 and the catalog) |

Create the role in the console:

1. **IAM -> Roles -> Create role -> AWS service -> use case Glue** (this creates the trust policy for `glue.amazonaws.com`).
2. Attach `AWSGlueServiceRole`.
3. Name it, for example `AWSglueservicerolefordbt`, and create it.
4. Open the role -> **Add permissions -> Create inline policy -> JSON**, name it `S3Access`:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:ListBucket","s3:GetBucketLocation"],
    "Resource": [
      "arn:aws:s3:::<BUCKET>",
      "arn:aws:s3:::<BUCKET>/*"
    ]
  }]
}
```

`AWSGlueServiceRole` only covers buckets named `aws-glue-*`, so this explicit S3 policy is required.

5. Copy the role ARN: `arn:aws:iam::<ACCOUNT_ID>:role/AWSglueservicerolefordbt`.

### 4.5 Permission for your user to start sessions and pass the role

Skip if your user already has `AdministratorAccess`. Otherwise add this inline policy to the user:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": ["glue:*"], "Resource": "*"},
    {"Effect": "Allow", "Action": "iam:PassRole",
     "Resource": "arn:aws:iam::<ACCOUNT_ID>:role/AWSglueservicerolefordbt"}
  ]
}
```

`iam:PassRole` means "you may hand this role to an AWS service". Without it you get `AccessDeniedException ... iam:PassRole`.

If **Lake Formation** manages your databases, IAM alone is not enough. Grant the Glue role `Describe` and `Select` on `raw_db`, create/alter/drop and `Select`/`Insert`/`Delete` on `dbt_dev` and `dbt_prod`, and access to the S3 data location (Lake Formation -> Permissions -> Data permissions / Data locations).

---

## 5. Python and dbt installation

```powershell
cd C:\Users\HP\Downloads\dbtprojectonaws
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python --version                      # must print 3.11.x
python -m pip install --upgrade pip
pip install dbt-core dbt-glue
dbt --version                         # shows dbt-core and the glue plugin
```

If activation is blocked: `Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned`.

Always create the venv with `py -3.11 -m venv .venv`. Plain `python -m venv` uses your newest Python.

Pin the versions that work:

```powershell
pip freeze | findstr /i "dbt-core dbt-glue"
```

Put those lines (for example `dbt-core==1.12.6`, `dbt-glue==1.12.4`) in `requirements.txt` at the repo root.

---

## 6. dbt project and profile

```powershell
dbt init dbtbyaravind          # choose [1] glue
```

`dbt init` writes a **sample** `profiles.yml` containing a placeholder (`{{ var('_dbt_random_suffix') }}`), which is why the first `dbt debug` shows `profiles.yml file [ERROR invalid]`. Ignore that error. The next step replaces the profile.

Check that `dbt_project.yml` is directly inside `dbtbyaravind/` and contains `profile: 'dbtbyaravind'`. If `dbt init` created a nested `dbtbyaravind/dbtbyaravind/`, use the inner folder as your project.

### 6.1 One profile for every environment

Create `dbtbyaravind/profiles.yml`. It holds **no secrets**, so it is safe to commit. Environment-specific values come from environment variables, and the same file works on your laptop, in Airflow and in GitHub Actions.

```yaml
dbtbyaravind:
  target: default
  outputs:
    default:
      type: glue
      role_arn: "{{ env_var('DBT_GLUE_ROLE_ARN') }}"
      region: us-east-1
      glue_version: "4.0"
      workers: 2
      worker_type: G.1X
      idle_timeout: 10
      session_provisioning_timeout_in_seconds: 300
      schema: "{{ env_var('DBT_SCHEMA', 'dbt_dev') }}"
      database: "{{ env_var('DBT_SCHEMA', 'dbt_dev') }}"
      location: "s3://{{ env_var('DBT_BUCKET') }}/dbt/"
```

Notes:

- `location` must end in `/dbt/`, **not** `/raw/`. Output written into `raw/` mixes dbt tables with source data.
- The top-level key must equal `profile:` in `dbt_project.yml`.
- Use spaces, not tabs. Indentation errors break the whole file.
- Keep `region` equal to the region of your bucket, Glue databases and `AWS_DEFAULT_REGION`.

Delete the sample file `C:\Users\HP\.dbt\profiles.yml` (or leave it, it is no longer used once `DBT_PROFILES_DIR` is set).

### 6.2 Local environment script

Create `env.ps1` in the repo root (git-ignored):

```powershell
$env:DBT_GLUE_ROLE_ARN = "arn:aws:iam::<ACCOUNT_ID>:role/AWSglueservicerolefordbt"
$env:DBT_BUCKET        = "<BUCKET>"
$env:DBT_SCHEMA        = "dbt_dev"
$env:DBT_PROFILES_DIR  = "."
```

Load it and run debug from inside the project folder:

```powershell
. .\env.ps1
cd dbtbyaravind
dbt debug
```

The first run takes 1 to 3 minutes while Glue provisions a session. Expect `All checks passed!`.

---

## 7. First model

`dbtbyaravind/models/staging/_sources.yml`

```yaml
version: 2
sources:
  - name: raw
    database: raw_db
    schema: raw_db
    tables:
      - name: customers
      - name: orders
      - name: payments
```

`dbtbyaravind/models/staging/stg_orders.sql`

```sql
{{ config(materialized='table') }}

select
    id as order_id,
    user_id as customer_id,
    order_date,
    status
from {{ source('raw', 'orders') }}
```

Delete the sample `models/example` folder created by `dbt init`. Then:

```powershell
dbt run --select stg_orders
```

Verify in Athena: `SELECT * FROM dbt_dev.stg_orders LIMIT 5;`

Tables are used instead of views because Spark views are awkward to read from Athena. You can set this project-wide in `dbt_project.yml`:

```yaml
models:
  dbtbyaravind:
    staging:
      +materialized: table
```

---

## 8. Optional: Iceberg

Iceberg is needed for `incremental_strategy='merge'` and for snapshots on Glue. Get the plain-Parquet version working first (section 7), then add these keys to the `default:` output, at the same indentation as `location`:

```yaml
      datalake_formats: iceberg
      conf: >-
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
        --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
        --conf spark.sql.catalog.glue_catalog.warehouse=s3://{{ env_var('DBT_BUCKET') }}/dbt/
        --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
        --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
```

`datalake_formats` must be indented exactly like `conf`, inside the output block. Test with a model:

```sql
{{ config(materialized='table', file_format='iceberg') }}
select * from {{ ref('stg_orders') }}
```

If you also add `--conf spark.sql.defaultCatalog=glue_catalog` and your raw tables stop resolving (`Table or view not found: raw_db.orders`), remove that line. Raw CSV tables are Hive-style, not Iceberg. The Iceberg configuration changes between adapter versions, so compare with the `dbt-glue` README (github.com/aws-samples/dbt-glue) if the session fails to start.

Optional speed-up: `glue_session_reuse: True` reuses one session across commands instead of starting a new one each time.

---

## 9. Git and GitHub

### 9.1 `.gitignore` (repo root)

```
.venv/
target/
dbt_packages/
logs/
.user.yml
.env
env.ps1
__pycache__/
airflow/logs/
```

### 9.2 Initialize and push

```powershell
cd C:\Users\HP\Downloads\dbtprojectonaws
git init -b main
git config user.name "Your Name"
git config user.email "you@example.com"
git status
```

`.venv`, `target`, `logs`, `env.ps1` and `.env` must **not** appear in `git status`. Then create a **private** repo on github.com (no README, no .gitignore), and:

```powershell
git add .
git commit -m "Initial dbt project"
git remote add origin https://github.com/<GITHUB_USER>/<REPO>.git
git push -u origin main
```

A browser login appears the first time (Git Credential Manager).

### 9.3 Branch workflow

```powershell
git switch -c feature/add-stg-customers
# edit models
git add . ; git commit -m "Add stg_customers"
git push -u origin feature/add-stg-customers
# open a Pull Request on GitHub
```

---

## 10. Airflow in Docker

Docker Desktop must be running.

### 10.1 Files and folders

```powershell
cd C:\Users\HP\Downloads\dbtprojectonaws
mkdir airflow, airflow\dags, airflow\logs, airflow\plugins, airflow\config
cd airflow
curl.exe -LfO "https://airflow.apache.org/docs/apache-airflow/2.9.3/docker-compose.yaml"
```

In PowerShell, plain `curl` is an alias of `Invoke-WebRequest` and does not accept `-LfO`. Use `curl.exe`, or `Invoke-WebRequest -Uri <url> -OutFile docker-compose.yaml`.

Create `airflow\.env` (git-ignored). `id -u` does not exist on Windows, so use `50000`:

```
AIRFLOW_UID=50000
DBT_GLUE_ROLE_ARN=arn:aws:iam::<ACCOUNT_ID>:role/AWSglueservicerolefordbt
DBT_BUCKET=<BUCKET>
```

Compose automatically reads `.env` from this folder.

### 10.2 Dockerfile

Create a **file** named `Dockerfile` (no extension) in `airflow\`. For example `notepad Dockerfile`, and choose "All files" when saving so Notepad does not add `.txt`. Do **not** paste these lines into the terminal, since PowerShell will try to run them as commands.

```dockerfile
FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
```

`git` is installed because the Airflow image does not include it, and `dbt debug` reports `git [ERROR]` without it (and `dbt deps` fails for git-based packages). Copy the pinned requirements next to the Dockerfile:

```powershell
copy ..\requirements.txt .\requirements.txt
```

(Or replace the `COPY`/`pip install` lines with `RUN pip install --no-cache-dir dbt-core dbt-glue`.)

### 10.3 Edit `docker-compose.yaml`

Under `x-airflow-common:`:

1. Replace the `image: ...` line with:

```yaml
  build: .
```

2. Add to `environment:`:

```yaml
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AWS_DEFAULT_REGION: us-east-1
    DBT_PROFILES_DIR: /opt/airflow/dbt/dbtbyaravind
    DBT_GLUE_ROLE_ARN: ${DBT_GLUE_ROLE_ARN}
    DBT_BUCKET: ${DBT_BUCKET}
    DBT_TARGET_PATH: /tmp/dbt_target
    DBT_LOG_PATH: /tmp/dbt_logs
```

3. Add to `volumes:`:

```yaml
    - ../dbtbyaravind:/opt/airflow/dbt/dbtbyaravind
    - ${USERPROFILE}/.aws:/home/airflow/.aws:ro
```

The left side of the first mount is the folder on your PC. The right side is the path **inside** the container, so all container commands and DAGs must use `/opt/airflow/dbt/dbtbyaravind`. Because `profiles.yml` lives inside the dbt project and `DBT_PROFILES_DIR` points to it, there is no separate copy of the profile to keep in sync.

`AWS_DEFAULT_REGION` must match the region in `profiles.yml`. If you use SSO or temporary credentials, pass `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN` as environment variables instead of mounting `~/.aws`.

### 10.4 Build and start

```powershell
docker compose build
docker compose up airflow-init
docker compose up -d
```

Open http://localhost:8080 (login `airflow` / `airflow`).

After changing volumes or environment, run `docker compose up -d` again to recreate containers. A plain restart does not apply the change.

### 10.5 Verify dbt inside the container

```powershell
docker compose exec airflow-scheduler dbt --version
docker compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt/dbtbyaravind && dbt debug"
```

Expect `profiles.yml file [OK found and valid]`, `git [OK found]` and `Connection test: [OK connection ok]`.

If the folder is missing, inspect what the container sees:

```powershell
docker compose exec airflow-scheduler ls /opt/airflow/dbt
docker compose exec airflow-scheduler ls /opt/airflow/dbt/dbtbyaravind
```

An empty folder means the host path `..\dbtbyaravind` does not exist next to `airflow\`.

### 10.6 DAG

`airflow/dags/dbt_glue_prod.py`

```python
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/airflow/dbt/dbtbyaravind"
ENV = {"DBT_SCHEMA": "dbt_prod"}   # other variables come from docker-compose

with DAG(
    "dbt_glue_prod",
    start_date=datetime(2026, 11, 1),
    schedule=None,          # change to "@daily" when ready
    catchup=False,
    tags=["dbt", "prod"],
) as dag:

    deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {PROJECT} && dbt deps",
        env=ENV, append_env=True,
    )
    build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {PROJECT} && dbt build",
        env=ENV, append_env=True,
    )

    deps >> build
```

If the project has no `packages.yml`, remove the `deps` task. In the UI, unpause `dbt_glue_prod` and click Trigger. The `dbt_prod` database fills with tables. When you outgrow `BashOperator`, move to Astronomer Cosmos, which renders each dbt model as its own Airflow task.

Stop everything with `docker compose down` when you finish.

---

## 11. CI/CD with GitHub Actions

### 11.1 Let GitHub assume an AWS role (OIDC, no stored keys)

1. **IAM -> Identity providers -> Add provider**: type OpenID Connect, URL `https://token.actions.githubusercontent.com`, audience `sts.amazonaws.com`.
2. **IAM -> Roles -> Create role -> Custom trust policy**, name `GitHubActionsDbtRole`:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {
      "Federated": "arn:aws:iam::<ACCOUNT_ID>:oidc-provider/token.actions.githubusercontent.com"
    },
    "Action": "sts:AssumeRoleWithWebIdentity",
    "Condition": {
      "StringEquals": {"token.actions.githubusercontent.com:aud": "sts.amazonaws.com"},
      "StringLike":   {"token.actions.githubusercontent.com:sub": "repo:<GITHUB_USER>/<REPO>:*"}
    }
  }]
}
```

The `sub` condition restricts the role to your repository. Do not skip it.

3. Inline permissions for that role:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": "glue:*", "Resource": "*"},
    {"Effect": "Allow", "Action": "iam:PassRole",
     "Resource": "arn:aws:iam::<ACCOUNT_ID>:role/AWSglueservicerolefordbt"},
    {"Effect": "Allow",
     "Action": ["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:ListBucket"],
     "Resource": ["arn:aws:s3:::<BUCKET>","arn:aws:s3:::<BUCKET>/*"]}
  ]
}
```

4. In GitHub: **Settings -> Secrets and variables -> Actions**
   - Secret `AWS_CI_ROLE_ARN` = ARN of `GitHubActionsDbtRole`
   - Secret `DBT_GLUE_ROLE_ARN` = ARN of `AWSglueservicerolefordbt`
   - Variable `DBT_BUCKET` = `<BUCKET>`

### 11.2 CI: runs on every pull request

`.github/workflows/ci.yml`

```yaml
name: dbt-ci

on:
  pull_request:
    branches: [main]

permissions:
  id-token: write
  contents: read

concurrency:
  group: dbt-ci-${{ github.event.number }}
  cancel-in-progress: true

env:
  DBT_PROFILES_DIR: ${{ github.workspace }}/dbtbyaravind
  DBT_GLUE_ROLE_ARN: ${{ secrets.DBT_GLUE_ROLE_ARN }}
  DBT_BUCKET: ${{ vars.DBT_BUCKET }}
  DBT_SCHEMA: dbt_ci_pr_${{ github.event.number }}

jobs:
  ci:
    runs-on: ubuntu-latest
    timeout-minutes: 45
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: pip

      - run: pip install -r requirements.txt

      - uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.AWS_CI_ROLE_ARN }}
          aws-region: us-east-1

      - name: Install packages and parse
        working-directory: dbtbyaravind
        run: |
          dbt deps
          dbt parse

      - name: Build into temporary CI schema
        working-directory: dbtbyaravind
        run: dbt build

      - name: Drop CI schema and files
        if: always()
        run: |
          aws glue delete-database --name "$DBT_SCHEMA" || true
          aws s3 rm "s3://$DBT_BUCKET/dbt/$DBT_SCHEMA/" --recursive || true
```

- `DBT_PROFILES_DIR` is an **absolute** path (`github.workspace`). A relative path would resolve against `working-directory` and break.
- `dbt parse` fails fast on syntax and `ref()` errors at no Glue cost.
- `dbt build` runs seeds, models, snapshots and tests in DAG order in an isolated database.
- The cleanup step runs even when the build fails.
- Make CI mandatory: **Settings -> Branches -> Add branch protection rule** for `main`, enable "Require a pull request" and "Require status checks to pass".

Cost: each CI run starts a Glue session and builds all models. Once the project grows, move to slim CI by saving the prod `manifest.json` and running `dbt build --select state:modified+ --defer --state <dir>`.

### 11.3 CD: runs on merge to main

`.github/workflows/cd.yml`

```yaml
name: dbt-cd

on:
  push:
    branches: [main]

permissions:
  id-token: write
  contents: read

env:
  DBT_PROFILES_DIR: ${{ github.workspace }}/dbtbyaravind
  DBT_GLUE_ROLE_ARN: ${{ secrets.DBT_GLUE_ROLE_ARN }}
  DBT_BUCKET: ${{ vars.DBT_BUCKET }}
  DBT_SCHEMA: dbt_prod

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: pip
      - run: pip install -r requirements.txt
      - uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.AWS_CI_ROLE_ARN }}
          aws-region: us-east-1

      - name: Parse project with prod settings
        working-directory: dbtbyaravind
        run: |
          dbt deps
          dbt parse

      - name: Publish project and manifest to S3
        run: |
          aws s3 sync dbtbyaravind "s3://$DBT_BUCKET/deploy/dbtbyaravind/" \
            --exclude "target/*" --exclude "logs/*" --exclude "dbt_packages/*" --delete
          aws s3 cp dbtbyaravind/target/manifest.json "s3://$DBT_BUCKET/artifacts/prod/manifest.json"
```

A GitHub runner cannot push code into Docker on your laptop. With the bind mount in section 10, Airflow runs whatever is in your local checkout, so deploying locally means `git pull` on the `main` branch. The S3 copy exists for an Airflow that runs elsewhere (for example EC2) and for slim CI.

Where Airflow runs:

| Option | Notes |
|---|---|
| Local Docker | free, ideal for learning, runs only while your PC is on |
| EC2 + Docker Compose | cheap, always on, can use an instance role instead of keys |
| MWAA | managed but costs several hundred dollars per month at the smallest size, and installing `dbt-glue` there is awkward |

Action versions (`actions/checkout@v4`, `actions/setup-python@v5`, `aws-actions/configure-aws-credentials@v4`) change over time. Check the marketplace pages for newer major versions.

---

## 12. Daily workflow

```powershell
# one-time per terminal session
.\.venv\Scripts\Activate.ps1
. .\env.ps1
cd dbtbyaravind

git switch -c feature/my-change      # work on a branch
dbt build --select my_model+         # test locally in dbt_dev
git add . ; git commit -m "..." ; git push -u origin feature/my-change
# open PR -> CI builds in dbt_ci_pr_N -> merge -> CD publishes -> Airflow runs prod
```

Useful commands: `dbt debug`, `dbt parse`, `dbt compile`, `dbt run --select <model>`, `dbt test`, `dbt seed`, `dbt snapshot`, `dbt build`, `dbt docs generate`, `dbt source freshness`.

---

## 13. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Failed building wheel for pyyaml`, "Microsoft Visual C++ 14.0 required" | Python 3.14/3.15 has no prebuilt wheels | Use Python 3.11: `py -3.11 -m venv .venv` |
| `.venv` still shows Python 3.15 | venv was created with the default Python | Delete `.venv`, recreate with `py -3.11` |
| `Activate.ps1` blocked | PowerShell execution policy | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` |
| `profiles.yml file [ERROR invalid]`, `_dbt_random_suffix` not found | sample profile written by `dbt init` | replace with the profile in section 6.1 |
| `Could not find profile named ...` | profile name differs from `profile:` in `dbt_project.yml` | make them identical |
| YAML parse error / `datalake_formats` ignored | wrong indentation | keys inside the output block must align with `conf` and `location` |
| `iam:PassRole` AccessDenied | user lacks PassRole on the Glue role | add policy from 4.5 with the right role ARN |
| `EntityNotFoundException` | Glue database missing or wrong region | create it (4.3), check region everywhere |
| `Table or view not found: raw_db.orders` | table not created, wrong region/database, Lake Formation, or `defaultCatalog` set to Iceberg | check `aws glue get-tables`, region, Lake Formation grants, remove `defaultCatalog` |
| dbt tables appear under `s3://<BUCKET>/raw/...` | `location` points to `raw/` | set `location` to `/dbt/` |
| Session start timeout | slow provisioning | raise `session_provisioning_timeout_in_seconds` |
| `curl -LfO` "parameter cannot be found" | PowerShell `curl` alias | use `curl.exe` |
| `id` not recognized | Linux command | use `AIRFLOW_UID=50000` |
| `The 'from' keyword is not supported` | Dockerfile text typed into PowerShell | save it as a file named `Dockerfile` |
| `No such file or directory` for the dbt project in the container | container path differs from the mount, or host path wrong | use the right side of the volume mapping; recreate with `docker compose up -d` |
| `dbt debug` in container: `git [ERROR]` | image has no git | add git to the Dockerfile (10.2) and rebuild |
| Container shows `conf: None`, `location: .../raw/` | stale copy of the profile | keep `profiles.yml` in the dbt project and set `DBT_PROFILES_DIR` (section 10) |
| Region errors in Airflow | `AWS_DEFAULT_REGION` differs from profile region | make them equal |
| CI cannot find the profile | relative `DBT_PROFILES_DIR` with `working-directory` | use `${{ github.workspace }}/dbtbyaravind` |
| `Not authorized to perform sts:AssumeRoleWithWebIdentity` | OIDC trust policy `sub` does not match the repo | fix `repo:<GITHUB_USER>/<REPO>:*` |

---

## 14. Cost and cleanup

- Glue sessions bill per DPU-second (minimum 2 DPUs). Keep `workers: 2` and `idle_timeout: 10`.
- Stop forgotten sessions in the Glue console under Interactive Sessions.
- Run `docker compose down` when you finish.
- Delete or deactivate access keys you no longer use (IAM -> user -> Security credentials).
- Temporary CI databases are dropped automatically. Check for leftovers with:

```powershell
aws glue get-databases --query "DatabaseList[].Name"
```

---

## 15. dbt concepts roadmap

Work through these in order using the customers/orders/payments data:

1. Sources and `source()`
2. Models and `ref()`; the DAG
3. Materializations: view, table, incremental, ephemeral
4. Tests: generic (`unique`, `not_null`, `accepted_values`, `relationships`), singular, custom generic
5. Seeds
6. Documentation: `description:` fields, doc blocks, `dbt docs generate` and `dbt docs serve`
7. Jinja and macros
8. Incremental models (`merge` strategy on Iceberg)
9. Snapshots (SCD type 2)
10. Variables, hooks, tags and selectors (`--select tag:daily`, `+model`, `state:modified`)
11. Packages (`dbt_utils`) and `dbt deps`
12. Source freshness
13. Model contracts, constraints and unit tests
14. Exposures
15. Orchestration with Airflow, CI/CD with GitHub Actions

---------------------------
created develop 
