---
globs: ["dbt_subprojects/**"]
---
# catalyst conventions

## execution order
- `run` prefix = blocking prerequisite, execute before proceeding
- numbered items = execute sequentially

## code patterns
- use existing chain patterns as reference (e.g., `kaia`, `mezo`)
- ordering: mimic existing; if unclear, append alphabetically
- swap chain name in: file paths, model names, schema entries, `blockchain` values

## contributors
- new files: set git username only
- existing files: append git username

## sql style
- when updating, mimic the style of the existing code
- when creating new code, lowercase keywords
- trailing commas
- use 1 space for in-line padding
- do not include a semicolon at the end of the query
- indent joins and use inner join instead of join
- single-line joins when short
- positional group by
- no blank lines around unions
- if there are multiple ctes, separate them with 1 empty line
