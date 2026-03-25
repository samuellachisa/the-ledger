# DATA_GENERATION.md

## Seed Data Summary

- Companies: 80
- Documents: 349 files total
  - 189 statement/proposal artifacts (`.txt`)
  - 160 Excel/workbook artifacts (`.csv`, generated as multi-sheet CSV + year summary)
- Seed Events: 1,847
- Seed Applications: 29

## How to Generate (after PostgreSQL is installed)

Run the generator and produce the seed database + documents:

```bash
python datagen/generate_all.py \
  --applicants 80 \
  --db-url postgresql://<user>:<password>@localhost:5432/<db_name> \
  --docs-dir ./documents \
  --output-dir ./data \
  --random-seed 42
```

## Application Distribution

Fill this table after generation (counts + application IDs).

| State | Count | Application IDs |
|-------|-------|-----------------|
| SUBMITTED | 6 | APEX-0001 – APEX-0006 |
| DOCUMENTS_UPLOADED | 5 | APEX-0007 – APEX-0011 |
| DOCUMENTS_PROCESSED | 4 | APEX-0012 – APEX-0015 |
| CREDIT_COMPLETE | 3 | APEX-0016 – APEX-0018 |
| FRAUD_COMPLETE | 2 | APEX-0019 – APEX-0020 |
| APPROVED | 4 | APEX-0021 – APEX-0024 |
| DECLINED | 1 | APEX-0025 |
| DECLINED_AGENT | 2 | APEX-0026 – APEX-0027 |
| DECLINED_COMPLIANCE | 1 | APEX-0028 |
| REFERRED | 1 | APEX-0029 |

## Statement Variants

Income statement variants (expected distribution; confirm after generation):

- `clean`: 40
- `dense` / multi-subtotal: 20
- `missing_ebitda`: 8
- `scanned`: 12

Balance sheets:

- 6 balance sheets with intentional equity rounding discrepancy ($500–$4,500)
  - File naming: `balance_sheet_2024_discrepancy.txt` vs `balance_sheet_2024.txt`

Loan application proposal artifacts:

- 29 proposal files (`application_proposal_{application_id}.txt`) — one per seeded application

## Event Breakdown

Expected total: 1,847.

Breakdown by `events.aggregate_type` after generation (seed simulator in `datagen/`):

| Aggregate Type | Event Count | Key Event Types (top) |
|----------------|-------------|------------------------|
| `AuditLedger` | 923 | `AuditEntryRecorded` |
| `AgentSession` | 790 | `AgentContextLoaded`, `AgentSessionStarted`, `AgentCreditAnalysisObserved`, `AgentDecisionRecorded` |
| `LoanApplication` | 107 | `LoanApplicationSubmitted`, `CreditAnalysisRequested`, `CreditAnalysisCompleted`, `FraudCheckCompleted`, `ComplianceCheckRequested`, `ComplianceFinalizedOnApplication` |
| `ComplianceRecord` | 27 | `ComplianceRecordCreated`, `ComplianceRecordFinalized`, `ComplianceCheckPassed`, `ComplianceCheckFailed` |

## API Cost Simulation

This project stubs LLM calls in Week 5 agents. If you later enable real LLM calls, update this section with:

- Token counts per agent type
- Model versions used
- Estimated cost per agent
- Total cost budget (< $50 total)

## Sanity Checks (suggested)

After running the generator:

- Verify companies count
- Verify events count (should be 1,847)
- Verify documents exist (349 files total)

