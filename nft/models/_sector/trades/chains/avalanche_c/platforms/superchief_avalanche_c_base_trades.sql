{{ config(
    schema='superchief_avalanche_c',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id']
)}}

WITH 

auction_started_event as (
    SELECT 
        amount as nft_amount,
        evt_block_time,
        'Auction Sale' as trade_category,
        id as auction_id,
        tokenId as nft_token_id, 
        CASE 
            WHEN paymentToken = 0x0000000000000000000000000000000000000000 THEN 0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7
            ELSE paymentToken 
        END as currency_contract,
        collection as nft_contract_address,
        tx."from" as seller 
    FROM 
    {{source('avalanche_c', 'transactions')}} tx 
    INNER JOIN 
    {{ source('superchief_avalanche_c', 'AuctionManager_evt_NewAuction') }} au 
        ON au.evt_block_number = tx.block_number
        AND au.evt_tx_hash = tx.hash
        {% if is_incremental() %}
        AND {{incremental_predicate('tx.block_time')}}
        {% endif %}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('evt_block_time')}}
    {% endif %}
),

base_trades as (
    SELECT 
        'avalanche_c' as blockchain,
        'superchief' as project,
        'v1' as project_version,
        ee.evt_block_time as block_time,
        date_trunc('day',ee.evt_block_time) as block_date,
        date_trunc('month',ee.evt_block_time) as block_month,
        ae.nft_token_id,
        'Secondary' as trade_type,
        ae.nft_amount,
        ae.trade_category,
        ae.seller,
        ee.winner as buyer,
        ee.bidPrice as price_raw,
        ae.currency_contract,
        ae.nft_contract_address,
        ee.evt_tx_hash as tx_hash,
        ee.contract_address as project_contract_address,
        ee.evt_block_number as block_number,
        CAST(NULL as UINT256) as platform_fee_amount_raw,
        CAST(NULL AS UINT256) as royalty_fee_amount_raw,
        CAST(NULL as VARBINARY) royalty_fee_address,
        CAST(NULL as VARBINARY) as platform_fee_address,
        ee.evt_index as sub_tx_trade_id
    FROM 
    {{ source('superchief_avalanche_c', 'AuctionManager_evt_AuctionFinished') }} ee 
    LEFT JOIN 
    auction_started_event ae 
        ON ee.id = ae.auction_id
    {% if is_incremental() %}
    WHERE {{incremental_predicate('ee.evt_block_time')}}
    {% endif %}
)

{{add_nft_tx_data('base_trades','avalanche_c')}}