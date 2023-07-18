{{ config(
	tags=['legacy'],
	
    schema = 'archipelago_ethereum',
    alias = alias('base_trades', legacy_model=True),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}
{% set project_start_date='2022-6-20' %}

WITH
trade_events as (
    SELECT * FROM {{ source('archipelago_ethereum','ArchipelagoMarket_evt_Trade') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% else %}
    WHERE evt_block_time >= '{{project_start_date}}'
    {% endif %}
),
token_events as (
    SELECT * FROM {{ source('archipelago_ethereum','ArchipelagoMarket_evt_TokenTrade') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% else %}
    WHERE evt_block_time >= '{{project_start_date}}'
    {% endif %}
),
fee_events as (
    SELECT
        evt_block_number
        , tradeId
        , sum(amount) filter (
            where recipient not in ('0xa76456bb6abc50fb38e17c042026bc27a95c3314','0x1fc12c9f68a6b0633ba5897a40a8e61ed9274dc9')
            ) as royalty_amount
        , sum(amount) filter (
            where recipient in ('0xa76456bb6abc50fb38e17c042026bc27a95c3314','0x1fc12c9f68a6b0633ba5897a40a8e61ed9274dc9')
            ) as platform_amount
    FROM {{ source('archipelago_ethereum','ArchipelagoMarket_evt_RoyaltyPayment') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% else %}
    WHERE evt_block_time >= '{{project_start_date}}'
    {% endif %}
    GROUP BY evt_block_number, tradeId
)

SELECT
    date_trunc('day',trade.evt_block_time) as block_date
    ,trade.evt_block_time as block_time
    ,trade.evt_block_number as block_number
    ,trade.evt_tx_hash as tx_hash
    ,trade.contract_address as project_contract_address
    ,CAST(null as varchar(5)) as trade_category
    ,'secondary' as trade_type
    ,trade.buyer
    ,trade.seller
    ,tok.tokenAddress as nft_contract_address
    ,tok.tokenId as nft_token_id
    ,1 as nft_amount
    ,trade.currency as currency_contract
    ,cast(trade.cost as decimal(38)) as price_raw
    ,cast(coalesce(fee.platform_amount,0) as decimal(38)) as platform_fee_amount_raw
    ,cast(coalesce(fee.royalty_amount,0) as decimal(38)) as royalty_fee_amount_raw
    ,CAST(null as varchar(5)) as platform_fee_address
    ,CAST(null as varchar(5)) as royalty_fee_address
    ,trade.evt_index as sub_tx_trade_id
FROM trade_events trade
INNER JOIN token_events tok
ON trade.evt_block_number = tok.evt_block_number
    AND trade.tradeId = tok.tradeId
LEFT JOIN fee_events fee
ON trade.evt_block_number = fee.evt_block_number
    AND trade.tradeId = fee.tradeId

