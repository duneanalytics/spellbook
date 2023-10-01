{{ config(
    schema = 'pancekeswap_nft_bnb',
    alias = alias('events'),
    tags = ['dunesql'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'unique_trade_id']
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
        cast(1 as uint256) AS number_of_items,
        'Buy' AS trade_category,
        buyer AS buyer,
        seller AS seller,
        askPrice AS amount_raw,
        0.02 AS platform_fee_percentage,
        case when askPrice > cast(0 as uint256) and (netPrice/cast(askPrice as double))<0.98 then (askPrice-netPrice)/cast(askPrice as double) - 0.02 else 0.0 end AS royalty_fee_percentage,
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
    SELECT
        events.blockchain,
        events.project,
        events.version,
        events.block_time,
        events.token_id,
        bnb_nft_tokens.name collection,
        events.amount_raw/POWER(10, bnb_bep20_tokens.decimals)*prices.price AS amount_usd,
        events.token_standard,
        CASE WHEN agg.name IS NOT NULL THEN 'Bundle Trade' ELSE 'Single Item Trade' END AS trade_type,
        events.number_of_items,
        events.trade_category,
        'Trade' AS evt_type,
        events.seller,
        events.buyer,
        events.amount_raw/POWER(10, bnb_bep20_tokens.decimals) AS amount_original,
        events.amount_raw,
        COALESCE(events.currency_symbol, bnb_bep20_tokens.symbol) AS currency_symbol,
        events.currency_contract,
        events.nft_contract_address,
        events.project_contract_address,
        agg.name AS aggregator_name,
        agg.contract_address AS aggregator_address,
        events.tx_hash,
        events.block_number,
        bt."from" AS tx_from,
        bt.to AS tx_to,
        CAST(amount_raw * platform_fee_percentage AS uint256) AS platform_fee_amount_raw,
        (amount_raw * platform_fee_percentage)/POWER(10, bnb_bep20_tokens.decimals) AS platform_fee_amount,
        (amount_raw * platform_fee_percentage)/POWER(10, bnb_bep20_tokens.decimals)*prices.price AS platform_fee_amount_usd,
        CAST(100 * platform_fee_percentage AS DOUBLE) AS platform_fee_percentage,
        CAST(amount_raw * royalty_fee_percentage AS uint256) AS royalty_fee_amount_raw,
        (amount_raw * royalty_fee_percentage)/POWER(10, bnb_bep20_tokens.decimals) AS royalty_fee_amount,
        (amount_raw * royalty_fee_percentage)/POWER(10, bnb_bep20_tokens.decimals)*prices.price AS royalty_fee_amount_usd,
        CAST(100*royalty_fee_percentage AS DOUBLE) AS royalty_fee_percentage,
        cast(null as varbinary) AS royalty_fee_receive_address,
        COALESCE(events.currency_symbol, bnb_bep20_tokens.symbol) AS royalty_fee_currency_symbol,
        cast(events.block_number as varchar) || '-' || cast(events.tx_hash as varchar) || '-' || cast(events.evt_index as varchar) AS unique_trade_id

    FROM events
    LEFT JOIN {{ ref('nft_bnb_aggregators') }} agg ON events.buyer=agg.contract_address
    LEFT JOIN {{ ref('tokens_bnb_bep20') }} bnb_bep20_tokens ON bnb_bep20_tokens.contract_address=events.currency_contract
    LEFT JOIN {{ ref('tokens_bnb_nft') }} bnb_nft_tokens ON bnb_nft_tokens.contract_address=events.currency_contract
    LEFT JOIN {{ source('prices', 'usd') }} prices ON prices.minute=date_trunc('minute', events.block_time)
    AND (prices.contract_address=events.currency_contract AND prices.blockchain=events.blockchain)
        {% if is_incremental() %}
        AND prices.minute >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    INNER JOIN {{ source('bnb','transactions') }} bt ON bt.block_number = events.block_number
    AND bt.hash=events.tx_hash
    {% if is_incremental() %}
    AND bt.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
