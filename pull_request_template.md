Brief comments on the purpose of your changes:


*For Dune Engine V2*
I've checked that:

* [ ] I tested the query on dune.com after compiling the model with dbt compile (compiled queries are written to the target directory)
* [ ] I used "refs" to reference other models in this repo and "sources" to reference raw or decoded tables 
* [ ] if adding a new model, I added a test
* [ ] the filename is unique and ends with .sql
* [ ] each sql file is a select statement and has only one view, table or function defined  
* [ ] column names are `lowercase_snake_cased`
* [ ] if adding a new model, I edited the dbt project YAML file with new directory path for both models and seeds (if applicable)
* [ ] if adding a new model, I edited the alter table macro to display new database object (table or view) in UI explorer
* [ ] if adding a new materialized table, I edited the optimize table macro to be run in the background
* [ ] if joining to base table (i.e. ethereum transactions or traces), I looked to make it an inner join if possible
* [ ] if joining to base table (i.e. ethereum transactions or traces), I applied join condition where block_time >= date_trunc("day", now() - interval '1 week')
* [ ] if joining to prices view, I applied join condition where minute >= date_trunc("day", now() - interval '1 week')

When you are ready for a review, tag duneanalytics/data-experience. We will re-open your forked pull request as an internal pull request. Then your spells will run in dbt and the logs will be avaiable in Github Actions DBT Slim CI. This job will only run the models and tests changed by your PR compared to the production project.
