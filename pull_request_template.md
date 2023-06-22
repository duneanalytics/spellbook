# SPELLBOOK FREEZE

From June 22 to June 29, we will be freezing some Spellbook contributions for the migration to DuneSQL. We will not accept contributions to the lineage of the following models:

* dex.trades
* nft.trades
* labels
* token.erc20
* tokens.nft

Run the following command to see the list of affected files:

```
dbt ls --resource-type model --output path --select +dex_trades +labels +nft_trades +tokens_nft +tokens_erc20
```

Don't hesitate to reach out on Discord if you have any questions.

Brief comments on the purpose of your changes:

**For Dune Engine V2**

I've checked that:

### General checks:
* [ ] I tested the query on dune.com after compiling the model with dbt compile (compiled queries are written to the target directory)
* [ ] I used "refs" to reference other models in this repo and "sources" to reference raw or decoded tables 
* [ ] if adding a new model, I added a test
* [ ] the filename is unique and ends with .sql
* [ ] each sql file is a select statement and has only one view, table or function defined  
* [ ] column names are `lowercase_snake_cased`
* [ ] if adding a new model, I edited the dbt project YAML file with new directory path for both models and seeds (if applicable)
* [ ] if wanting to expose a model in the UI (Dune data explorer), I added a post-hook in the JINJA config to add metadata (blockchains, sector/project, name and contributor Dune usernames)

### Pricing checks:
* [ ] `coin_id` represents the ID of the coin on coinpaprika.com
* [ ] all the coins are active on coinpaprika.com (please remove inactive ones)

### Join logic:
* [ ] if joining to base table (i.e. ethereum transactions or traces), I looked to make it an inner join if possible

### Incremental logic:
* [ ] I used is_incremental & not is_incremental jinja block filters on both base tables and decoded tables
  * [ ] where block_time >= date_trunc("day", now() - interval '1 week')
* [ ] if joining to base table (i.e. ethereum transactions or traces), I applied join condition where block_time >= date_trunc("day", now() - interval '1 week')
* [ ] if joining to prices view, I applied join condition where minute >= date_trunc("day", now() - interval '1 week')
