# shared catalyst steps

## prep vars
- retrieve chain metadata: `.cursor/scripts/dune_query.py --query "select * from dune.blockchains where name = '<chain>'"`
  - extract: `chain_id`, `name` (display name), `token_address` (native token)
- retrieve first_block_time: `.cursor/scripts/dune_query.py --query "select min(time) from <chain>.blocks where number <> 0"`

## git workflow

### 1. verify main is up to date
- fetch latest, pull if behind, exit if diverged

### 2. create branch
- name: `<issue_id>-<chain>-<task_suffix>`
- create off `main`, checkout, warn if exists
- don't commit/push anything

## final checks
- run `pipenv shell` + `dbt compile` in relevant subprojects
- fix any issues found
