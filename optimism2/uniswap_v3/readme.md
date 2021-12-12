### Uniswap Pools

This abstraction table is intended to create the canonical table `uniswap_v3.view_pools` to be used for all Uniswap pool queries in the OVM 2.0 database.

**This is needed because:**
1. All transactions (including OVM 1.0 pool creation events) were wiped from the on-chain history at Optimism's Nov 11 Regenesis which upgraded to OVM 2.0.
2. Some pool contracts were migrated to new addresses from OVM 1.0 to OVM 2.0

This view enables analysts to query from one table: `uniswap_v3.view_pools` which accurately handles for all of the logic and updates

#### Tables
 - **uniswap_v3.view_pools**: Base table schema for Uniswap pool contracts.
 - **uniswap_v3.insert_ovm1_legacy_pools**: Insert script for pools created in OVM1.0, migrated to their new contract address in OVM2.0.
 - **uniswap_v3.insert_uniswap_v3_poolcreated**: Insert script for pools created in OVM2.0, appended to the running list of pool contracts.
