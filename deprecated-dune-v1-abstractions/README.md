Enjoy working with Dune? We're [hiring developers](https://careers.duneanalytics.com) remotely!

# Abstractions

This repository tracks user-created abstractions to the Dune Analytics data platform. Contributions in the form of issues and pull requests are very much welcome here.

## Guidelines and conventions
- Folders within the {ethereum/, xdai/} folder maps to schema names (project names) in the relevant Dune dataset. Make sure to get the right schema.
- Each file should only contain one table, view, materialized view or function declaration.
- Files should have names matching their declared object. I.e. if a file declares `CREATE VIEW x.view_y`, the file should be `ethereum/x/view_y.sql`
- Each file should be run in a transaction. I.e. either have one statement or be wrapped in `BEGIN;` and `COMMIT;`


## Objects

### Views
View names should be prefixed by `view_`. If the view is defined as `CREATE VIEW view_ctokens AS`, then the file should be called `view_ctokens.sql`. 

### Materialized Views
Materialized view names should be prefxied by `view_`, same as normal views.
Additionally, a materialized view must specify at what interval it should be refreshed. This is done by adding the following block to the end of the declaration:
```sql
INSERT INTO cron.job(schedule, command)
VALUES ('* 1 * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY x.view_y$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
```
Note that the preferred way to refresh a materialized view is using the `CONCURRENTLY` keyword, and that this mandates the existence of a `UNIQUE` index on the materialized view. See more info [here](https://www.postgresql.org/docs/12/sql-refreshmaterializedview.html).

### Tables
Tables are declared without any prefix in the name. If the table `x.y` needs to be periodically updated, the convention is to create a companion function `x.insert_y(from timestamptz, to timestamptz=now())`. It is then customary to do
```sql
INSERT INTO cron.job (schedule, command)
VALUES ('* 1 * * *', $$SELECT x.insert_y((SELECT max(block_time) - interval '1 days' FROM x.y));$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
```

### Functions
Functions that are not companion functions to tables, should be prefixed by `fn_`.


## Other conventions
- Objects (tables, views, materialized views, functions) should be names in `lowercase_snake_cased`
- Columns should be `lowercase_snake_cased`
- Use `block_time` to indicate the time of an event, not `timestamp`, `ts`, `evt_block_time`
