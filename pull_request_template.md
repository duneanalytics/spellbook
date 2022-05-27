I've checked that:

* [ ] I tested the query on dune.com after compiling the model with dbt compile (compiled queries are written to the target directory)
* [ ] I used "refs" to reference other models in this repo and "sources" to reference raw or decoded tables 
* [ ] the directory tree matches the pattern /sector/blockchain/ e.g. /tokens/ethereum
* [ ] if adding a new model, I added a test
* [ ] the filename is unique and ends with .sql
* [ ] each file has only one view, table or function defined  
* [ ] column names are `lowercase_snake_cased`
