{{
    config(
        schema = 'nft_solana'
        
        , alias = 'trades'
        , materialized = 'view'
        ,post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "nft",
                                    \'["ilemi"]\') }}'
    )
}}

{% set solana_marketplaces = [
    ref('magiceden_v2_solana_trades')
    , ref('magiceden_v3_solana_trades')
    , ref('magiceden_mmm_solana_trades')
    , ref('tensorswap_v1_solana_trades')
    , ref('tensorswap_v2_solana_trades')
] %}


{% for marketplace in solana_marketplaces %}
SELECT
        blockchain,
        project,
        version,
        cast(date_trunc('day', block_time) as date) as block_date,
        cast(date_trunc('month', block_time) as date) as block_month,
        block_time,
        trade_type,
        number_of_items,
        trade_category,
        buyer,
        seller,
        amount_raw,
        amount_original,
        amount_usd,
        currency_symbol,
        currency_address,
        account_merkle_tree,
        leaf_id,
        account_mint,
        project_program_id,
        aggregator_name,
        aggregator_address,
        tx_id,
        block_slot,
        tx_signer,
        taker_fee_amount_raw,
        taker_fee_amount,
        taker_fee_amount_usd,
        taker_fee_percentage,
        maker_fee_amount_raw,
        maker_fee_amount,
        maker_fee_amount_usd,
        maker_fee_percentage,
        royalty_fee_amount_raw,
        royalty_fee_amount,
        royalty_fee_amount_usd,
        royalty_fee_percentage,
        amm_fee_amount_raw,
        amm_fee_amount,
        amm_fee_amount_usd,
        amm_fee_percentage,
        instruction,
        outer_instruction_index,
        inner_instruction_index
FROM {{ marketplace }}

{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}