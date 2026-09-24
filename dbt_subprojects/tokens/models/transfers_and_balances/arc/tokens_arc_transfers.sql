{{config(
     schema = 'tokens_arc',
     alias = 'transfers',
     partition_by = ['block_month'],
     materialized = 'incremental',
     file_format = 'delta',
     incremental_strategy = 'merge',
     merge_skip_unchanged = true,
     unique_key = ['block_date','unique_key'],
     incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
     , post_hook='{{ hide_spells() }}'
     )
}}

-- Standard transfers_enrich call: Arc's price and token metadata now come from the crosschain
-- sources every other chain uses. prices.usd_with_native covers Arc because prices.tokens
-- includes prices_arc_tokens (the ERC-20 entries) and prices_native_tokens (arc ->
-- usdc-usd-coin, the 18-decimal native gas token at dune.blockchains' token_address);
-- tokens.erc20 and prices.trusted_tokens are crosschain and carry Arc since launch. No
-- per-chain prices_arc.minute / tokens_arc.erc20 reads are needed any more.
--
-- Native rows carry dune.blockchains' token_address for Arc rather than a real contract, at 18
-- decimals, matching the EIP-7708 emitter's precision. The 6-decimal ERC-20 USDC interface at
-- 0x3600...0000 is priced separately in prices.tokens, but tokens_arc_base_transfers excludes
-- that address, so the two precisions cannot collide here. Arc's native asset is trusted via
-- prices.trusted_tokens' non-testnet native tokens, so large native transfers keep their
-- amount_usd instead of tripping the outlier guard.

{{ transfers_enrich(
    base_transfers = ref('tokens_arc_base_transfers'),
    transfers_start_date = '2026-05-12',
    blockchain = 'arc'
  )
}}
