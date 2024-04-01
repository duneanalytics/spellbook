# OP Schema & Tables

- **op_token:** Tables relating to the OP Token, including initial allocations, metadata, etc. - [Contract Address](https://optimistic.etherscan.io/token/0x4200000000000000000000000000000000000042).
- **token_distributions:** Tables relating to OP Token grants and growth experiment distributions to projects on Optimism. This aims to track project's transfers & handling through to when each token is deployed to users and/or grants - `op_token_distributions_optimism.transfer_mapping`. See [gov.optimism.io](https://gov.optimism.io/) for governance discussion and grant proposals.

*To be Created*
- **op_chains:** The purpose of this schema is to unify Dune's schema with the OP Chain & Superchain model. This includes tables relating to *op-chains*, a subset of chains building on the [OP Stack](https://optimism.mirror.xyz/fLk5UGjZDiXFuvQh6R_HscMQuuY9ABYNF7PI76-qJYs) which contribute back to Optimism; including metadata, unified on-chain activity, etc (i.e. Base, OPCraft). Longer-term, this can include relevant metadata for the [Superchain](https://app.optimism.io/superchain).

## Other Relevant Schemas
- **ovm1:** Tables relating to Optimism before the regenesis upgrade to the [EVM Equivalence](https://medium.com/ethereum-optimism/introducing-evm-equivalence-5c2021deb306) mainnet (Nov 11, 2021). [See Optimism Help Center](https://help.optimism.io/hc/en-us/articles/4414190132251-Where-is-my-pre-11-November-2021-transaction-history-).
- **ovm:** Tables relating to Optimism protocol operations (i.e. cross-domain messaging, bridge token factories). 'OVM' is now an outdated term, but it's what was used in the initial version of these tables. To avoid downstream queries breaking, we'll keep the name here.