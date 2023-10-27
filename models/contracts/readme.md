# Runbook to add a new chain

This is a long process. Ideally we figure out a solution where we don't need to do one per chain, and instead can unify all chains (please help).

1. Add a new file for the chain in `contracts/chains/contract_creator_project_mapping/`
2. Create dummy legacy model
3. Add this new chain alias to `contracts/chains/contract_creator_project_mapping/contracts_contract_creator_project_mapping_schema.sql`
4. Add the ref to the model to `contracts/contracts_contract_mapping.sql`

5. Add a new file for the chain in `contracts/chains/find_self_destruct_contracts/`
6. Create dummy legacy model
7. Add this new chain alias to `contracts/chains/find_self_destruct_contracts/contracts_find_self_destruct_contracts_schema.sql`
8. Add the ref to the model to `contracts/contracts_optimism_self_destruct_contracts.sql`
