Airdrop
_____

The airdrop sector has multipe tables available including:
- `airdrop.claims` where all onchain airdrop claim transactions can be found.
- (TO BE ADDED) `airdrop.eligible` contains the list of addresses and the airdrop amount it is eligible for.
- (TO BE ADDED) `airdrop.total_stats` contains a single line per token airdrop containing some overall statistics on the airdrop]


All of tables depend on their blockchain-specific abstractions (ie `airdrop.claims` depends on `airdrop_ethereum.claims`, `airdrop_optimism.claims` & `airdrop_arbitrum.claims`). And those depend on the corresponding protocol specific tables (ie `ens_ethereum.airdrop_claims`).