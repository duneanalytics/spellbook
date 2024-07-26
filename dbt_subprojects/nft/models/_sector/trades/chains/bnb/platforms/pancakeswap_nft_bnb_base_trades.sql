{{ config(
    schema = 'pancekeswap_nft_bnb',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id']
    )
}}

WITH events AS (
    SELECT
        'bnb' AS blockchain,
        'pancakeswap' AS project,
        'v1' AS version,
        evt_block_time AS block_time,
        tokenId AS token_id,
        'erc721' AS token_standard,
        'Single Item Trade' AS trade_type,
        uint256 '1' AS number_of_items,
        'Buy' AS trade_category,
        buyer AS buyer,
        seller AS seller,
        askPrice AS amount_raw,
        double '0.02' AS platform_fee_percentage,
        case when askPrice > uint256 '0' and (netPrice/cast(askPrice as double))<0.98 then (askPrice-netPrice)/cast(askPrice as double) - 0.02 else 0.0 end AS royalty_fee_percentage,
        0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c AS currency_contract,
        CASE when withBNB then 'BNB' else 'WBNB' end AS currency_symbol,
        collection AS nft_contract_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_block_number AS block_number,
        evt_index
    FROM {{source('pancakeswap_v2_bnb','ERC721NFTMarketV1_evt_Trade')}}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)

, base_trades as (
    SELECT
        events.blockchain,
        events.project,
        events.version as project_version,
        events.block_time,
        cast(date_trunc('day', events.block_time) as date) as block_date,
        cast(date_trunc('month', events.block_time) as date) as block_month,
        events.token_id as nft_token_id,
        'secondary' AS trade_type,
        events.number_of_items as nft_amount,
        events.trade_category,
        events.seller,
        events.buyer,
        events.amount_raw as price_raw,
        events.currency_contract,
        events.nft_contract_address,
        events.project_contract_address,
        events.tx_hash,
        events.block_number,
        CAST(amount_raw * platform_fee_percentage AS uint256) AS platform_fee_amount_raw,
        CAST(amount_raw * royalty_fee_percentage AS uint256) AS royalty_fee_amount_raw,
        cast(null as varbinary) AS royalty_fee_address,
        cast(null as varbinary) AS platform_fee_address,
        events.evt_index AS sub_tx_trade_id

    FROM events
)

-- this will be removed once tx_from and tx_to are available in the base event tables
{{ add_nft_tx_data('base_trades', 'bnb') }}
