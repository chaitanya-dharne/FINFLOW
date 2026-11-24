# Security and governance

## Secrets
- All configuration comes from environment variables (`src/finflow/config.py`). **No credential appears in code, SQL, JSON or tests** (a test scans for hardcoded secrets).
- `.env` is git-ignored; `.env.example` contains names only. Rotate any secret that was ever pasted into a chat, ticket or commit.
- Logs are passed through a redactor (URLs with credentials, `password=`, `token=`, `sig=`...). Database connection info is logged with the password masked.

## Least privilege
| Principal | Scope | Rights |
|---|---|---|
| `finflow_deploy` (MySQL) | `finflow.*` | DDL + DML; used by CI/DBA only |
| `finflow_etl` (MySQL) | `finflow.*` | SELECT/INSERT/UPDATE/DELETE only; run with `FINFLOW_AUTO_INIT_SCHEMA=false` |
| `finflow_reader` (MySQL) | `finflow.*` | SELECT; consider excluding `etl_rejected_records` |
| GitHub Actions | repo | `permissions: contents: read`; CI uses only throwaway service-container credentials |

## Data protection
- Synthetic data only: names are generic, emails use `example.com`. Treat real data as PII/financial data.
- `etl_rejected_records.raw_record` and `rejected_records.csv` mask PII columns (`FINFLOW_MASK_PII_IN_REPORTS=true`). The unmasked source stays in the raw layer; use `source_file` + `source_row_number` to find it.
- Raw files are immutable and checksummed (tamper evidence, replay).
- Network: firewall rules for MySQL; no public access in production.

## Auditability
Every curated row carries `created_load_id`, `updated_load_id`, `source_file`, `row_hash`, `etl_created_at`, `etl_updated_at`;
`etl_load_audit`, `etl_file_registry`, `etl_dq_results`, `etl_reconciliation` and `etl_rejected_records` answer
"what was loaded, from which file, when, and what was rejected and why".
