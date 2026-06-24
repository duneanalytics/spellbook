# Spellbook AI Tooling

This directory contains optional guidance for AI-assisted development in Spellbook. The canonical project context is still [`CLAUDE.md`](../CLAUDE.md), [`README.md`](../README.md), and [`docs/`](../docs/); use this file as the index for Cursor-specific rules and skills.

## Structure

```
.cursor/
├── rules/      # always-on Cursor guidance for Spellbook conventions
└── skills/     # task-specific agent skills, invoked by name in Agent
```

## Rules

Cursor applies these rules automatically when available:

| Rule | Purpose |
|------|---------|
| `ci-state-detection.mdc` | Identify CI state and stale/irrelevant failures |
| `dbt-core-conventions.mdc` | Core dbt model, Jinja, and SQL conventions |
| `dbt-yaml-generic-tests.mdc` | Generic test and schema YAML patterns |
| `model-config-checklist.mdc` | Required Spellbook model config checks |
| `schema-and-testing.mdc` | Model documentation and test expectations |
| `seed-conventions.mdc` | Seed file and seed test conventions |

## Skills

Invoke these skills for focused workflows:

| Skill | Use When |
|-------|----------|
| `/run-dbt-commands` | Compile, list, test, or run dbt models in the correct sub-project |
| `/debug-ci` | Diagnose Spellbook CI failures from a PR or workflow run |
| `/dbt-prod-ci-regression` | Compare production output against CI tables for regression checks |
| `/review-spell` | Review a Spellbook spell contribution with the repo checklist |
| `/catalyst-foundational-metadata` | Add EVM foundational metadata for a new chain |
| `/catalyst-gas-and-transfers` | Add gas fees and token transfer models for a new chain |
| `/catalyst-dex-integration` | Add a DEX project or chain integration to `dex.trades` |

## Reference Docs

- [`docs/general/best_practices.md`](../docs/general/best_practices.md) - development workflow, performance, incremental model guidance
- [`docs/models/model_overview.md`](../docs/models/model_overview.md) - model layout and conventions
- [`docs/tests/test_overview.md`](../docs/tests/test_overview.md) - required test patterns
- [`docs/ci_test/ci_test_overview.md`](../docs/ci_test/ci_test_overview.md) - CI behavior and troubleshooting
- [`docs/general/faq_and_common_issues.md`](../docs/general/faq_and_common_issues.md) - common contributor issues

## Dune MCP

Use Dune MCP for query validation when local compilation is not enough:

- `query_sql` - execute raw SQL
- `run_query_by_id` - run a saved Dune query by ID with parameters, for example `query_parameters: '{"chain":"<chain>"}'`

For AMP metadata support, run saved query `6637901` with `query_parameters: '{"chain":"<chain>","sim_api_key":"<key>"}'`. In this repo, read `SIM_METADATA_API_KEY` from `.env` for `sim_api_key` when available.

## Environment

Required environment variables in `.env` at the project root:

```
DUNE_API_KEY=your_api_key
SIM_METADATA_API_KEY=your_sim_key
```
