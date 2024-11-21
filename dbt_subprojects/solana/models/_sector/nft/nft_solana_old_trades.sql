{{ config(
    schema = 'nft_solana',
    alias = 'old_trades',
    materialized = 'view'
    )
}}


-- while we refactor more marketplace models, they should be removed here and added to the chain specific base_trades unions.
{% set nft_models = [
ref('magiceden_solana_events')
] %}


-- we have to do some column wrangling here to convert the old schema to the new schema
-- lots of columns will hold null values..
SELECT * FROM  (
{% for nft_model in nft_models %}
    SELECT
        blockchain,
        project,
        version,
        cast(date_trunc('day', block_time) as date) as block_date,
        cast(date_trunc('month', block_time) as date) as block_month,
        block_time,
        case when evt_type = 'Mint' then 'primary' else 'secondary' end as trade_type,
        number_of_items,
        trade_category,
        cast(buyer as varchar) as buyer,
        cast(seller as varchar) as seller,
        amount_raw,
        amount_original,
        amount_usd,
        currency_symbol,
        cast(currency_contract as varchar) as currency_address,
        cast(null as varchar) as account_merkle_tree,
        cast(null as uint256) as leaf_id,
        cast(null as varchar) as account_mint,
        cast(null as varchar) as project_program_id,
        aggregator_name,
        cast(aggregator_address as varchar) as aggregator_address,
        cast(tx_hash as varchar) as tx_id,
        block_number as block_slot,
        cast(null as varchar) as tx_signer,
        cast(null as double) as taker_fee_amount_raw,
        cast(null as double)  as taker_fee_amount,
        cast(null as double)  as taker_fee_amount_usd,
        cast(null as double)  as taker_fee_percentage,
        cast(null as double)  as maker_fee_amount_raw,
        cast(null as double)  as maker_fee_amount,
        cast(null as double)  as maker_fee_amount_usd,
        cast(null as double)  as maker_fee_percentage,
        royalty_fee_amount_raw,
        royalty_fee_amount,
        royalty_fee_amount_usd,
        royalty_fee_percentage,
        cast(null as double)  as amm_fee_amount_raw,
        cast(null as double)  as amm_fee_amount,
        cast(null as double)  as amm_fee_amount_usd,
        cast(null as double)  as amm_fee_percentage,
        cast(null as varchar) as instruction,
        cast(null as integer) as outer_instruction_index,
        cast(null as integer) as inner_instruction_index
    FROM {{ nft_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
