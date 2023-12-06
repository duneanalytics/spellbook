# Bridges Spells

### Brief Background on Bridge Protocols
Bridges are used by users who want to send information (often transferring assets) between chains. We've observed a few main bridge categories:
- **Native Bridges:** Bridges that live natively in the chain's design. ex: Optimism Bridge, Arbitrum Bridge, etc
        - Deposits: Assets are sent and locked in the L1 bridge contract, then minted to the user on L2.
        - Withdrawals: Assets are burned on the L2 bridge, then the user claims on L1.
- **Bridge Protocols:** Protocols that facilitate liquidity pools on multiple chains to handle transfers. ex: Hop, Stargate, Wormhole, etc
        - Transfers: Assets are deposited in Chain A's liquidity pool. A stableswap and burn may occur (i.e. ETH/hETH). Assets are transferred to the user on Chain B.
        - Rebalancing: These bridge protocols may rebalance assets between chains in order to handle for user's deposits and withdrawals.
- **CEX / Fiat On/Off-Ramps:** Centralized entities that hold funds on either end of a bridge and facilitate user's directly depositing and withdrawing. ex: Coinbase, Binance, Ramp. (Debatable if we should include these as bridges, so for now we'll plan to do a separate On-Ramp transfers table).

### Tables:
- **bridge.flows:** Transfer events for any native bridges and cross-chain bridges. This table ~tries to avoid double counting bridge events by excluding native bridge transfers if they're also found in a bridge protocol or bridge aggregator (i.e. User deposits to Optimism from Ethereum using Hop, and Hop uses the Optimism standard bridge)

### Protocols:
- **Optimism:** Bolded if Done or In-Progress
        - Native Bridges: **Optimism Bridge**
        - Protocols: **Hop**, Celer, Synapse, Multichain, Teleportr (v1, v2), Across, Stargate, Connext, Orbiter
        - Aggregators: Socket/Bungee, LiFi, | Aggregator User Checks: Rainbow, Zapper, Metamask
