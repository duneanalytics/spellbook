{{ config(
    schema='salvor_avalanche_c',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id']
)}}

{% set project_start_date = '2022-10-29' %}

WITH 

events as (
    SELECT 
        evt_block_time,
        tokenId as nft_token_id, 
        UINT256 '1' as nft_amount,
        'Trade' as trade_category,
        buyer,
        bid as price_raw,
        0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7 as currency_contract,
        collection as nft_contract_address,
        evt_tx_hash,
        contract_address,
        evt_block_number,
        evt_index 
    FROM 
    {{ source('salvor_avalanche_c', 'Marketplace_evt_AcceptCollectionOffer') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('evt_block_time')}}
    {% endif %}
),

royalty_payment as (
    SELECT 
        evt_tx_hash,
        collection,
        tokenId,
        SUM(amount) as amount, 
        MIN_BY(royaltyReceiver, evt_index) as royaltyRecipient,
        MIN_BY(_seller, evt_index) as seller,
        COUNT(*) as number_of_payments -- when there are multiple payments, the royalty_fee_address & seller is null 
    FROM 
    {{ source('salvor_avalanche_c', 'PaymentManager_evt_RoyaltyReceived') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('evt_block_time')}}
    {% endif %}
    GROUP BY 1, 2, 3 
), 

base_trades as (
    SELECT 
        'avalanche_c' as blockchain,
        'salvor' as project,
        'v1' as project_version,
        ee.evt_block_time as block_time,
        date_trunc('day',ee.evt_block_time) as block_date,
        date_trunc('month',ee.evt_block_time) as block_month,
        ee.nft_token_id,
        'secondary' as trade_type,
        ee.nft_amount,
        ee.trade_category,
        ra.seller,
        ee.buyer,
        ee.price_raw,
        ee.currency_contract,
        ee.nft_contract_address,
        ee.evt_tx_hash as tx_hash,
        ee.contract_address as project_contract_address,
        ee.evt_block_number as block_number,
        CAST(NULL as UINT256) as platform_fee_amount_raw,
        ra.amount as royalty_fee_amount_raw,
        CASE 
            WHEN number_of_payments = 1 THEN royaltyRecipient
            ELSE CAST(NULL as VARBINARY)
        END as royalty_fee_address,
        CAST(NULL as VARBINARY) as platform_fee_address,
        ee.evt_index as sub_tx_trade_id
    FROM 
    events ee 
    LEFT JOIN 
    royalty_payment ra 
        ON ee.evt_tx_hash = ra.evt_tx_hash 
        AND ee.nft_contract_address = ra.collection
        AND ee.nft_token_id = ra.tokenId
)

{{add_nft_tx_data('base_trades','avalanche_c')}}
