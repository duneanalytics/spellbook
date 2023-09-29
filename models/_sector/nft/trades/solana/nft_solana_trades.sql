{{
    config(
        schema = 'nft_solana'
        , tags = ['dunesql']
        , alias = alias('trades')
        ,materialized = 'incremental'
        ,file_format = 'delta'
        ,incremental_strategy = 'merge'
        ,partition_by = ['project','block_month']
        ,unique_key = ['project','trade_category','outer_instruction_index','inner_instruction_index','account_metadata','tx_id']
        ,post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "nft",
                                    \'["ilemi"]\') }}'
    )
}}

{% set solana_marketplaces = [
    ref('magiceden_solana_trades')
    , ref('tensorswap_solana_trades')
] %}


{% for marketplace in solana_marketplaces %}
SELECT
        blockchain,
        project,
        version,
        cast(date_trunc('day', block_time) as date) as block_date,
        cast(date_trunc('month', block_time) as date) as block_month,
        block_time,
        token_name,
        token_symbol,
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
        account_metadata,
        account_master_edition,
        account_mint,
        verified_creator,
        collection_mint,
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
        instruction,
        outer_instruction_index,
        inner_instruction_index,
        unique_trade_id,
        row_number() over (partition by unique_trade_id order by tx_id) as duplicates_rank
FROM {{ marketplace }}

{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}