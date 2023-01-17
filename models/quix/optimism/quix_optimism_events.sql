 {{
  config(
        alias='events',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "quix",
                                    \'["chuxin"]\') }}')
}}

{% set quix_events = [
ref( 'quix_v1_optimism_events' )
,ref( 'quix_v2_optimism_events' )
,ref( 'quix_v3_optimism_events' )
,ref( 'quix_v4_optimism_events' )
,ref( 'quix_v5_optimism_events' )
,ref( 'quix_seaport_optimism_events' )
] %}

select *
from (
    {% for model in quix_events %}
    select
        blockchain,
        project,
        version,
        block_time,
        token_id,
        collection,
        amount_usd,
        token_standard,
        trade_type,
        number_of_items,
        trade_category,
        evt_type,
        seller,
        buyer,
        amount_original,
        amount_raw,
        currency_symbol,
        currency_contract,
        nft_contract_address,
        project_contract_address,
        aggregator_name,
        aggregator_address,
        tx_hash,
        block_number,
        tx_from,
        tx_to,
        CAST(platform_fee_amount_raw AS DOUBLE) AS platform_fee_amount_raw,
        CAST(platform_fee_amount AS DOUBLE) AS platform_fee_amount,
        CAST(platform_fee_amount_usd AS DOUBLE) AS platform_fee_amount_usd,
        CAST(platform_fee_percentage AS DOUBLE) AS platform_fee_percentage,
        CAST(royalty_fee_amount_raw AS DOUBLE) royalty_fee_amount_raw ,
        CAST(royalty_fee_amount AS DOUBLE) royalty_fee_amount,
        CAST(royalty_fee_amount_usd AS DOUBLE) royalty_fee_amount_usd,
        CAST(royalty_fee_percentage AS DOUBLE) AS royalty_fee_percentage,
        royalty_fee_receive_address,
        royalty_fee_currency_symbol,
        unique_trade_id
    from {{ model }}
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)
