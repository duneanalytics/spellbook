---
name: run-dbt-commands
description: Run dbt CLI commands (compile, ls, test, run, etc.) in the spellbook repo. Use when the user asks to compile, list, test, or run dbt models, or when you need to validate SQL by compiling a model.
---

# Run dbt Commands

## Prerequisites

dbt commands require project dependencies synced with uv. Before running any dbt command:

1. **Sync the project environment** from the **repo root**:

```bash
uv sync --locked
```

2. **Navigate to the correct sub-project directory** before running dbt commands:

```bash
cd dbt_subprojects/<project_name>/
```

## Determining the Correct Sub-Project

The repo has six sub-projects under `dbt_subprojects/`:

| Sub-project | Contents |
|-------------|----------|
| `daily_spellbook` | Default for most spells (daily refresh) |
| `hourly_spellbook` | High-frequency spells (Dune team managed) |
| `dex` | DEX trading data (`dex.trades`, aggregators) |
| `nft` | NFT models |
| `solana` | Solana-specific models |
| `tokens` | Token metadata, transfers, balances |

**How to pick**: Look at where the model file lives. A model at `dbt_subprojects/solana/models/...` requires `cd dbt_subprojects/solana/`. If the user specifies a model name without a path, search for the `.sql` file to determine its sub-project.

## Running Commands

All dbt commands must be run from inside a sub-project directory (`dbt_subprojects/<project_name>/`).

### Using `uv run` from the Shell tool

The Shell tool starts a fresh shell. Use `uv run` to execute dbt commands in the project's locked environment:

```bash
uv run dbt compile -s model_name --project-dir dbt_subprojects/<project_name>/
```

Or navigate first, then use `uv run`:

```bash
cd dbt_subprojects/<project_name>/ && uv run dbt compile -s model_name
```

**Important**: Always use `uv run` when executing via the Shell tool. Do NOT use bare `dbt` commands unless you've manually activated `.venv`.

### Common commands

```bash
uv run dbt deps                        # install dbt package dependencies (run once per sub-project)
uv run dbt compile                     # compile all models to target/
uv run dbt compile -s model_name       # compile a single model
uv run dbt ls -s model_name            # list/check model selection
uv run dbt test -s model_name          # run tests for a model
```

### First time in a sub-project

Run `uv run dbt deps` once before `dbt compile` or other commands to pull dbt package dependencies. Subsequent runs in the same sub-project don't need `dbt deps` again unless `packages.yml` changed.

### Compiled output

After `dbt compile`, the rendered SQL is at:

```
dbt_subprojects/<project_name>/target/compiled/<project_name>/models/.../model_name.sql
```

## Workflow Example

```bash
# From repo root — compile a specific model in daily_spellbook
cd dbt_subprojects/daily_spellbook/ && uv run dbt compile -s stablecoins_balances
```

Or using --project-dir without changing directory:

```bash
uv run dbt compile -s stablecoins_balances --project-dir dbt_subprojects/daily_spellbook/
```

## Troubleshooting

- **"dbt: command not found"** — Run `uv sync --locked` and use `uv run dbt ...`.
- **Permission errors on log files** — Request `required_permissions: ["all"]` on the Shell tool call so dbt can write to its logs directory.
- **Compilation errors about missing refs** — You may be in the wrong sub-project. Check where the model file lives.
- **"Could not find profile"** — Make sure you're targeting a sub-project directory, not the repo root.
