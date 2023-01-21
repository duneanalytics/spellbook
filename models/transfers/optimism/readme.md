# Optimism Token Transfers
Tables to abstract token transfers on Optimism. Eventually this can be extended to all chains.

- **transfers_optimism_eth:** ETH Transfers on Optimism. This includes ETH transfers found via traces as well as the placeholder ETH ERC20 used for bridge deposits/withdrawals (Note: the ERC20 representation will be deprecated in Bedrock)

- **transfers_optimism_tokens:** Aggregate ETH, ERC20, and NFT (ERC721, ERC1155) transfers on Optimism in to one table. This helps make token and balance tracking simpler to write queries for on Optimism.

For NFT transfers, we pull from the **nft.transfers** spell