# Get OP Contracts
The `ovm2.get_contracts` table stores all contracts created on Optimism, including those created before the Nov 11th system upgrade and many of the predeploys.
1. Query for all 'create' traces
2. Check if the creator of the contract is a known project deployer (list currently maintained by @MSilb7), or exists as a decoded contract in `optimism.contracts`. Assign the `contract_project` field to the project name if so.
3. Check if there are any contracts created by contracts (i.e. factories) and apply the same matching rules based on the original contract.
4. Check if the contract is a token via transfers or known [erc20](https://github.com/duneanalytics/abstractions/tree/master/optimism2/erc20) and [erc721](https://github.com/MSilb7/abstractions/tree/patch-54/optimism2/erc721) contracts.

There is also `insert_updated_contract_info.sql` which scans all contracts from `ovm2.get_contracts` which are either unmapped or have a mismatched `contract_project` name and tries to pull in updated data.

*Note: Many of these pre-deploy contract lists are still maintained in manual `dune_user_generated` tables. They should be added to this repo for reference once implemented.*
