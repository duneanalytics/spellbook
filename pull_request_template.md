Brief comments on the purpose of your changes:


*For Dune Engine V2*
I've checked that:
General checks:
* [ ] I tested the query on dune.com after compiling the model with dbt compile (compiled queries are written to the target directory)
* [ ] I used "refs" to reference other models in this repo and "sources" to reference raw or decoded tables 
* [ ] if adding a new model, I added a test
* [ ] the filename is unique and ends with .sql
* [ ] each sql file is a select statement and has only one view, table or function defined  
* [ ] column names are `lowercase_snake_cased`
* [ ] if adding a new model, I edited the dbt project YAML file with new directory path for both models and seeds (if applicable)
* [ ] if adding a new model, I edited the alter table macro to display new database object (table or view) in UI explorer
* [ ] if adding a new materialized table, I edited the optimize table macro

Join logic:
* [ ] if joining to base table (i.e. ethereum transactions or traces), I looked to make it an inner join if possible

Incremental logic:
* [ ] I used is_incremental & not is_incremental jinja block filters on both base tables and decoded tables
  * [ ] where block_time >= date_trunc("day", now() - interval '1 week')
* [ ] if joining to base table (i.e. ethereum transactions or traces), I applied join condition where block_time >= date_trunc("day", now() - interval '1 week')
* [ ] if joining to prices view, I applied join condition where minute >= date_trunc("day", now() - interval '1 week')
