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

All dbt commands must be run from inside a sub-project directory (`dbt_subprojects/<project_name>/`).

### Using `pipenv run` from the Shell tool

The Shell tool starts a fresh shell without the user's active `pipenv shell` session. Use `pipenv run` to execute dbt commands without needing to enter the shell first:

```bash
pipenv run dbt compile -s model_name --project-dir dbt_subprojects/<project_name>/
```

Or navigate first, then use `pipenv run`:

```bash
cd dbt_subprojects/<project_name>/ && pipenv run dbt compile -s model_name
```

**Important**: Always use `pipenv run` when executing via the Shell tool. Do NOT use bare `dbt` commands — they will fail with "dbt: command not found" since the pipenv shell is not active in the tool's session. Also request `required_permissions: ["all"]` since dbt needs to write log files.

### Common commands

```bash
pipenv run dbt deps                        # install dbt package dependencies (run once per sub-project)
pipenv run dbt compile                     # compile all models to target/
pipenv run dbt compile -s model_name       # compile a single model
pipenv run dbt ls -s model_name            # list/check model selection
pipenv run dbt test -s model_name          # run tests for a model
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
# From repo root — compile a specific model in daily_spellbook
cd dbt_subprojects/daily_spellbook/ && pipenv run dbt compile -s stablecoins_balances
```

Or using --project-dir without changing directory:

```bash
pipenv run dbt compile -s stablecoins_balances --project-dir dbt_subprojects/daily_spellbook/
```

## Troubleshooting

- **"dbt: command not found"** — Use `pipenv run dbt ...` instead of bare `dbt`. The Shell tool does not share the user's `pipenv shell` session.
- **Permission errors on log files** — Request `required_permissions: ["all"]` on the Shell tool call so dbt can write to its logs directory.
- **Compilation errors about missing refs** — You may be in the wrong sub-project. Check where the model file lives.
- **"Could not find profile"** — Make sure you're targeting a sub-project directory, not the repo root.
