# FinFlow – Enterprise Financial Data Engineering Pipeline

FinFlow is an end-to-end data engineering project that processes synthetic banking data using ETL/ELT workflows. It ingests customer, account, transaction, and payment data from CSV files, validates data quality, transforms records, and loads curated data into a database with reconciliation and audit tracking.

The project runs locally with SQLite (zero setup) or against a MySQL 8.0 database using the official `mysql-connector-python` driver.

## Key Features

- **ETL/ELT Pipeline:** Extracts, validates, transforms, and loads financial data.
- **Data Quality Validation:** Identifies missing values, invalid formats, duplicate records, invalid dates, and referential integrity issues.
- **Raw, Staging, and Curated Layers:** Maintains raw input files, validates staging data, and stores processed records in curated tables.
- **Incremental Data Loading:** Supports incremental batches, watermarks, duplicate-file detection, and upsert operations.
- **Data Reconciliation:** Checks data consistency and rolls back failed database transactions.
- **Audit Logging:** Records load status, data quality results, rejected records, and pipeline execution details.
- **Automated Reporting:** Generates load summaries, rejected-record reports, reconciliation results, and data quality reports.

## Technology Stack

- **Programming:** Python, SQL
- **Data Processing:** Pandas, NumPy
- **Database:** SQLite, MySQL 8.0
- **Database Connectivity:** mysql-connector-python (MySQL), built-in sqlite3 (SQLite) - plain SQL, no ORM
- **Testing & Code Quality:** pytest, pytest-cov, Ruff
- **Version Control & CI:** Git, GitHub Actions

## Project Architecture

The pipeline follows these stages:

1. **Extract:** Reads customer, account, transaction, and payment CSV files and preserves copies in the raw layer.
2. **Validate:** Checks schemas, required fields, formats, dates, amounts, duplicates, foreign keys, and currency consistency.
3. **Transform:** Processes valid records and prepares them for database loading.
4. **Load:** Writes data to staging and merges changes into curated tables.
5. **Reconcile:** Performs consistency checks and rolls back the transaction if required checks fail.
6. **Audit & Report:** Records execution details and generates data quality, rejection, and reconciliation reports.

## Repository Structure

```text
finflow/
├── src/finflow/       # Pipeline modules and CLI
├── data/
│   ├── source/        # Synthetic input batches
│   ├── raw/           # Raw data copies
│   ├── reports/       # Generated reports
│   └── local/         # Local database files
├── sql/
│   ├── mysql/         # MySQL schemas, audit tables and views
│   └── sqlite/        # SQLite schema
├── tests/             # Automated tests
├── scripts/           # Setup and generation scripts
├── .github/workflows/ # CI workflow
├── docker-compose.yml
├── requirements.txt
├── requirements-dev.txt
├── pyproject.toml
└── README.md
```

## Getting Started

### Prerequisites

- Python 3.11 or later
- Git
- Windows PowerShell or a compatible terminal

SQLite is used for local development, so a separate database server is not required for the basic workflow.

### 1. Clone the Repository

```bash
git clone https://github.com/chaitanya-dharne/FINFLOW.git
cd FINFLOW
```

If your GitHub repository uses a different URL, replace the clone URL accordingly.

### 2. Create a Virtual Environment

**Windows PowerShell:**

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

**Install dependencies:**

```bash
python -m pip install --upgrade pip
pip install -r requirements-dev.txt
pip install -e .
```

### 3. Run the Pipeline

Process the initial batch:

```bash
finflow run --source data/source/batch_01
```

Process the incremental batch:

```bash
finflow run --source data/source/batch_02
```

Run the same batch again to exercise duplicate-file detection:

```bash
finflow run --source data/source/batch_02
```

Check recent pipeline loads:

```bash
finflow status
```

The exact results depend on the input data and execution environment.

### 4. Run Tests

```bash
pytest -q
```

For test coverage:

```bash
pytest --cov=finflow
```

Ruff can be used for linting:

```bash
ruff check src tests scripts
```

## Database Setup

### SQLite (default)

SQLite is the zero-setup local option. Nothing to configure - just run the pipeline.

### MySQL 8.0

The pipeline connects with `mysql-connector-python` and plain SQL. To use MySQL:

1. Start MySQL (for example `docker compose up -d`) and run `sql/mysql/01_create_database_and_roles.sql` as an admin.
2. Copy `.env.example` to `.env` and set:

```text
FINFLOW_DB_BACKEND=mysql
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DATABASE=finflow
MYSQL_USER=finflow_etl
MYSQL_PASSWORD=your_password
```

3. Run `finflow init-db`, then `finflow run --source data/source/batch_01`.

## Generated Outputs

Pipeline execution can generate:

- Load summaries and execution reports
- Rejected-record reports with validation details
- Data quality summaries
- Reconciliation results
- JSON execution summaries
- Structured application logs
- Database audit records

Local database files, raw copies, reports, and logs are stored in their configured output locations.

## Testing and Verification

The repository contains tests covering extraction, validation, transformations, pipeline execution, configuration, and SQL definitions.

SQLite is the local development option. MySQL integration tests (`tests/test_mysql_integration.py`) only run when `FINFLOW_TEST_MYSQL_URL` points to an empty MySQL scratch database.

## Future Enhancements

- Add performance benchmarks for larger datasets.
- Extend support for historical data tracking and source deletions.
- Optimize validation for larger datasets.
- Run and record the MySQL integration test in CI.
- Introduce streaming and additional orchestration capabilities.

## License

No license has been specified yet. Add an appropriate open-source license before distributing the project under specific reuse terms.

---

**FinFlow** demonstrates practical data engineering concepts through financial data ingestion, validation, incremental processing, database integration, reconciliation, and audit reporting using Python and SQL.
