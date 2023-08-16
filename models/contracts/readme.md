# Runbook to add a new chain
1. Add a new file for the chain in `contracts/chains/contract_creator_project_mapping/`
2. Add this new chain alias to `contracts/chains/contract_creator_project_mapping/contracts_contract_creator_project_mapping_schema.sql`
3. Add the ref to the model to `contracts/contracts_contract_mapping.sql`

4. Add a new file for the chain in `contracts/chains/find_self_destruct_contracts/`
5. Add this new chain alias to `contracts/chains/find_self_destruct_contracts/contracts_find_self_destruct_contracts_schema.sql`
6. Add the ref to the model to `contracts/contracts_optimism_self_destruct_contracts.sql`
