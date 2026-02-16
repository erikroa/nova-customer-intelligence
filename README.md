# NovaCRM Customer Intelligence Platform

A production-style dbt analytics project that transforms raw SaaS operational data into actionable customer intelligence - health scores, churn risk assessments, net revenue retention analysis, and support SLA tracking.

Built to demonstrate end-to-end data modeling for **Revenue Operations** and **Customer Success Analytics** in a B2B SaaS context.

---

## Business Context

**NovaCRM** is a fictional B2B SaaS CRM platform serving ~150 accounts across SMB, Mid-Market, and Enterprise segments. Leadership needs answers to critical questions:

- *"Which accounts are about to churn, and why?"*
- *"What's our net revenue retention - are we growing from existing customers?"*
- *"Are we meeting our support SLAs across all segments?"*
- *"How do newer customer cohorts compare to older ones?"*

This project builds the data infrastructure to answer those questions reliably and repeatably.

## Architecture

```
Raw Seeds (CSV)          Staging Layer           Core Layer              Mart Layer
┌──────────────┐     ┌─────────────────┐    ┌─────────────────┐    ┌──────────────────────┐
│ raw_accounts │────▶│ stg_accounts    │───▶│ dim_account      │───▶│ customer_health_scores│
│ raw_subscr.  │────▶│ stg_subscr.     │───▶│ dim_product      │───▶│ churn_risk_view       │
│ raw_invoices │────▶│ stg_invoices    │───▶│ dim_date         │───▶│ nrr_summary           │
│ raw_usage    │────▶│ stg_usage       │───▶│ fct_revenue      │───▶│ revenue_cohort_analysis│
│ raw_tickets  │────▶│ stg_tickets     │───▶│ fct_usage        │───▶│ sla_adherence          │
└──────────────┘     └─────────────────┘    │ fct_tickets      │    └──────────────────────┘
                                            └─────────────────┘
       5 seeds            5 models          6 models + 2 macros         5 models
                        60 tests              49 tests                28 tests + 2 custom
```

**Total: 157 automated data quality checks — all passing.**

## Data Model

### Staging Layer — Clean & Standardize
| Model | Grain | Purpose |
|-------|-------|---------|
| `stg_accounts` | 1 row per account | Trimmed, typed, lowercased CRM data |
| `stg_subscriptions` | 1 row per subscription | Cleaned billing data with `is_active` flag and duration |
| `stg_invoices` | 1 row per invoice | Standardized amounts with `is_paid` flag and invoice month |
| `stg_usage_events` | 1 row per event | Parsed timestamps with event category classification |
| `stg_support_tickets` | 1 row per ticket | Resolution time, SLA targets, and breach flags |

### Core Layer — Business Entities
| Model | Grain | Purpose |
|-------|-------|---------|
| `dim_account` | 1 row per account | Master profile: CRM + subscriptions + support + computed segment and lifecycle stage |
| `dim_product` | 1 row per product-tier | Product catalog with pricing and feature categories |
| `dim_date` | 1 row per day | Calendar spine (2023–2025) for time-series analysis |
| `fct_revenue` | 1 row per account × month | MRR, ARR, and revenue movement classification (new/expansion/contraction) |
| `fct_usage` | 1 row per account × day | Daily usage volume, feature breadth, and engagement metrics |
| `fct_tickets` | 1 row per ticket | Enriched tickets with SLA ratio, resolution speed, and CSAT tier |

### Mart Layer — Analytics Outputs
| Model | What It Answers |
|-------|----------------|
| `customer_health_scores` | "How healthy is each account?" — weighted 0–100 score combining usage, support, revenue, and engagement |
| `churn_risk_view` | "Which accounts need intervention?" — risk flags with usage trends, escalations, and revenue trajectory |
| `nrr_summary` | "What's our net revenue retention?" — monthly MRR waterfall with NRR and GRR percentages |
| `revenue_cohort_analysis` | "How do customer cohorts retain over time?" — account and revenue retention curves by signup month |
| `sla_adherence` | "Are we meeting support SLAs?" — adherence rates by priority and segment with resolution time distributions |

## Key Metrics

| Metric | Definition | Why It Matters |
|--------|-----------|----------------|
| **NRR** (Net Revenue Retention) | (Starting MRR + Expansion − Contraction − Churn) ÷ Starting MRR | >100% = organic growth from existing customers |
| **GRR** (Gross Revenue Retention) | (Starting MRR − Contraction − Churn) ÷ Starting MRR | How fast the revenue bucket is leaking |
| **Health Score** | Weighted composite: Usage (30%) + Support (20%) + Revenue (25%) + Engagement (25%) | Single number telling CS where to focus |
| **SLA Adherence** | % of tickets resolved within SLA target by priority | Are we keeping support promises? |
| **Feature Breadth** | Distinct event categories used ÷ 5 total categories | Are customers using the full product? |

## dbt Features Demonstrated

- **Layered architecture**: raw → staging → core → mart with clear separation of concerns
- **Reusable macros**: `classify_segment()` and `calculate_health_score()` used across models
- **Comprehensive testing**: 157 tests including unique, not_null, accepted_values, relationships, and custom singular tests
- **Documentation**: Full `schema.yml` descriptions for every model and column
- **Revenue modeling**: MRR waterfall, NRR/GRR, cohort retention curves
- **Customer health scoring**: Weighted composite methodology combining four distinct signals

## How to Run

### Prerequisites
- Python 3.9+
- dbt-core with BigQuery adapter (`pip install dbt-bigquery`)
- Google Cloud project with BigQuery enabled
- `gcloud` CLI authenticated (`gcloud auth application-default login`)

### Setup
```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/nova-customer-intelligence.git
cd nova-customer-intelligence

# Configure your profiles.yml (~/.dbt/profiles.yml)
# Set your GCP project ID and dataset name

# Load seed data, build all models, run all tests
dbt build
```

### Expected Output
```
Completed successfully
Done. PASS=157 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=157
```

## Project Structure

```
nova_customer_intelligence/
├── README.md
├── dbt_project.yml
├── seeds/                          # Synthetic CSV data (5 tables)
│   ├── raw_accounts.csv
│   ├── raw_subscriptions.csv
│   ├── raw_invoices.csv
│   ├── raw_usage_events.csv
│   └── raw_support_tickets.csv
├── models/
│   ├── staging/                    # Clean & standardize (5 models)
│   │   ├── _stg_models.yml
│   │   └── stg_*.sql
│   ├── core/                       # Business entities (6 models)
│   │   ├── _core_models.yml
│   │   ├── dim_*.sql
│   │   └── fct_*.sql
│   └── marts/                      # Analytics outputs (5 models)
│       ├── _mart_models.yml
│       └── *.sql
├── macros/                         # Reusable business logic
│   ├── classify_segment.sql
│   └── calculate_health_score.sql
├── tests/                          # Custom data quality tests
│   ├── assert_mrr_non_negative.sql
│   └── assert_health_score_range.sql
└── dashboards/                     # Dashboard screenshots
    └── *.png
```

## Dashboards

> Screenshots of Looker Studio dashboards connected to the BigQuery mart layer.

*(Dashboard screenshots to be added)*

## About

Built by **Erik Roa** as a portfolio project demonstrating Revenue Operations analytics and data modeling capabilities.

- 8+ years of B2B SaaS experience in Customer Success and Revenue Operations
- Google Cloud Data Analytics certified
- Proficient in dbt, SQL, BigQuery, Python, and Looker Studio

This project reflects the kind of analytics infrastructure that CS and RevOps teams rely on daily to reduce churn, drive expansion, and operationalize customer intelligence.

---

*Built with [dbt](https://www.getdbt.com/) and [Google BigQuery](https://cloud.google.com/bigquery).*
