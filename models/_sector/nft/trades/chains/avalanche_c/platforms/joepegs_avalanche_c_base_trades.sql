{{ config(
    schema='joepegs_avalanche_c',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id']
)}}

{% set project_start_date = '2023-09-01' %}

WITH 

events as (
    SELECT 
        evt_block_time,
        tokenId as nft_token_id, 
        amount as nft_amount,
        'Buy' as trade_category,
        maker as seller,
        taker as buyer,
        price as price_raw,
        currency as currency_contract,
        collection as nft_contract_address,
        evt_tx_hash,
        contract_address,
        evt_block_number,
        evt_index 
    FROM 
    {{ source('joepegs_avalanche_c', 'JoepegExchange_evt_TakerBid') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('evt_block_time')}}
    {% endif %}

    UNION ALL 

    SELECT 
        evt_block_time,
        tokenId as nft_token_id, 
        amount as nft_amount,
        'Sell' as trade_category,
        taker as seller,
        maker as buyer,
        price as price_raw,
        currency as currency_contract,
        collection as nft_contract_address,
        evt_tx_hash,
        contract_address,
        evt_block_number,
        evt_index 
    FROM 
    {{ source('joepegs_avalanche_c', 'JoepegExchange_evt_TakerAsk') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('evt_block_time')}}
    {% endif %}
),

base_trades as (
    SELECT 
        'avalanche_c' as blockchain,
        'joepegs' as project,
        'v1' as project_version,
        ee.evt_block_time as block_time,
        date_trunc('day',ee.evt_block_time) as block_date,
        date_trunc('month',ee.evt_block_time) as block_month,
        ee.nft_token_id,
        'secondary' as trade_type,
        ee.nft_amount,
        ee.trade_category,
        ee.seller,
        ee.buyer,
        ee.price_raw,
        ee.currency_contract,
        ee.nft_contract_address,
        ee.evt_tx_hash as tx_hash,
        ee.contract_address as project_contract_address,
        ee.evt_block_number as block_number,
        CAST(NULL as UINT256) as platform_fee_amount_raw,
        ra.amount as royalty_fee_amount_raw,
        ra.royaltyRecipient as royalty_fee_address,
        CAST(NULL as VARBINARY) as platform_fee_address,
        ee.evt_index as sub_tx_trade_id
    FROM 
    events ee 
    LEFT JOIN 
    {{ source('joepegs_avalanche_c', 'JoepegExchange_evt_RoyaltyPayment') }} ra 
        ON ee.evt_tx_hash = ra.evt_tx_hash 
        AND ee.nft_contract_address = ra.collection
        AND ee.nft_token_id = ra.tokenId
        {% if is_incremental() %}
        AND {{incremental_predicate('ra.evt_block_time')}}
        {% endif %}
)

{{add_nft_tx_data('base_trades','avalanche_c')}}
