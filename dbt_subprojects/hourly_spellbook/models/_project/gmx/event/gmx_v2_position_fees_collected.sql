{{ config(
        schema='gmx_v2',
        alias = 'position_fees_collected',
        post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c"]\',
                                    "project",
                                    "gmx",
                                    \'["ai_data_master","gmx-io"]\') }}'
        )
}}

{%- set chains = [
    'arbitrum',
    'avalanche_c',
] -%}

{%- for chain in chains -%}
SELECT
    blockchain,
    block_time,
    block_date,
    block_number,
    tx_hash,
    index,
    contract_address,
    tx_from,
    tx_to,  
    event_name,
    msg_sender,
    market,
    collateral_token,
    affiliate,
    trader,
    ui_fee_receiver,
    collateral_token_price_min,
    collateral_token_price_max,
    trade_size_usd,
    funding_fee_amount,
    claimable_long_token_amount,
    claimable_short_token_amount,
    latest_funding_fee_amount_per_size,
    latest_long_token_claimable_funding_amount_per_size,
    latest_short_token_claimable_funding_amount_per_size,
    borrowing_fee_usd,
    borrowing_fee_amount,
    borrowing_fee_receiver_factor,
    borrowing_fee_amount_for_fee_receiver,
    position_fee_factor,
    protocol_fee_amount,
    position_fee_receiver_factor,
    fee_receiver_amount,
    fee_amount_for_pool,
    position_fee_amount_for_pool,
    position_fee_amount,
    total_cost_amount,
    ui_fee_receiver_factor,
    ui_fee_amount,
    referral_total_rebate_factor,
    referral_total_rebate_amount,
    referral_trader_discount_factor,
    referral_adjusted_affiliate_reward_factor,
    referral_affiliate_reward_amount,
    referral_trader_discount_amount,
    pro_trader_discount_factor,
    pro_trader_discount_amount,
    liquidation_fee_amount,
    liquidation_fee_receiver_factor,
    liquidation_fee_amount_for_fee_receiver,
    is_increase,
    order_key,
    position_key,
    referral_code
FROM {{ ref('gmx_v2_' ~ chain ~ '_position_fees_collected') }}
{% if not loop.last %}
UNION ALL
{% endif %}
{%- endfor -%}