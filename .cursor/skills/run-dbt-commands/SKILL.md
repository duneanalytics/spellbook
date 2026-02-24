---
name: run-dbt-commands
description: Run dbt CLI commands (compile, ls, test, run, etc.) in the spellbook repo. Use when the user asks to compile, list, test, or run dbt models, or when you need to validate SQL by compiling a model.
---

# Run dbt Commands

## Prerequisites

dbt commands require an active pipenv shell. Before running any dbt command:

1. **Check for an active pipenv shell** — look at the terminals folder for a terminal with `pipenv shell` running.
2. **If no pipenv shell exists**, run these from the **repo root**:

```bash
pipenv install   # only needed on first setup or after Pipfile changes
pipenv shell
```

3. **Navigate to the correct sub-project directory** before running dbt commands:

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

All dbt commands must be run from inside a sub-project directory (`dbt_subprojects/<project_name>/`), inside an active pipenv shell.

### Common commands

```bash
dbt deps                        # install dbt package dependencies (run once per sub-project)
dbt compile                     # compile all models to target/
dbt compile -s model_name       # compile a single model
dbt ls -s model_name            # list/check model selection
dbt test -s model_name          # run tests for a model
```

### First time in a sub-project

Run `dbt deps` once before `dbt compile` or other commands to pull dbt package dependencies. Subsequent runs in the same sub-project don't need `dbt deps` again unless `packages.yml` changed.

### Compiled output

After `dbt compile`, the rendered SQL is at:

```
dbt_subprojects/<project_name>/target/compiled/<project_name>/models/.../model_name.sql
```

## Workflow Example

```bash
# 1. From repo root, enter pipenv (if not already active)
pipenv shell

# 2. Navigate to the sub-project
cd dbt_subprojects/daily_spellbook/

# 3. Install deps (first time only)
dbt deps

# 4. Compile a specific model
dbt compile -s stablecoins_balances
```

## Troubleshooting

- **"dbt: command not found"** — You're not inside the pipenv shell. Run `pipenv shell` from repo root.
- **Compilation errors about missing refs** — You may be in the wrong sub-project. Check where the model file lives.
- **"Could not find profile"** — Make sure you're inside a sub-project directory, not the repo root.
