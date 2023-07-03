{{ config(
    schema = 'pancekeswap_nft_bnb',
    alias = 'events',
    partition_by = ['block_date'],
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
        1 AS number_of_items,
        'Buy' AS trade_category,
        buyer AS buyer,
        seller AS seller,
        askPrice AS amount_raw,
        0.02 AS platform_fee_percentage,
        case when askPrice > 0 and (netPrice/askPrice)<0.98 then (askPrice-netPrice)/askPrice - 0.02 else 0.0 end AS royalty_fee_percentage,
        '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c' AS currency_contract,
        CASE when withBNB then 'BNB' else 'WBNB' end AS currency_symbol,
        collection AS nft_contract_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_block_number AS block_number,
        evt_index
    FROM {{source('pancakeswap_v2_bnb','ERC721NFTMarketV1_evt_Trade')}}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)
    SELECT
        events.blockchain,
        events.project,
        events.version,
        events.block_time,
        date_trunc('day', events.block_time) AS block_date,
        events.token_id,
        bnb_nft_tokens.name collection,
        events.amount_raw/POWER(10, bnb_bep20_tokens.decimals)*prices.price AS amount_usd,
        events.token_standard,
        CASE WHEN agg.name IS NOT NULL THEN 'Bundle Trade' ELSE 'Single Item Trade' END AS trade_type,
        CAST(events.number_of_items AS DECIMAL(38,0)) AS number_of_items,
        events.trade_category,
        'Trade' AS evt_type,
        events.seller,
        events.buyer,
        events.amount_raw/POWER(10, bnb_bep20_tokens.decimals) AS amount_original,
        CAST(events.amount_raw AS DECIMAL(38,0)) AS amount_raw,
        COALESCE(events.currency_symbol, bnb_bep20_tokens.symbol) AS currency_symbol,
        events.currency_contract,
        events.nft_contract_address,
        events.project_contract_address,
        agg.name AS aggregator_name,
        CASE WHEN agg.name IS NOT NULL THEN agg.contract_address END AS aggregator_address,
        events.tx_hash,
        events.block_number,
        bt.from AS tx_from,
        bt.to AS tx_to,
        CAST(amount_raw * platform_fee_percentage AS DOUBLE) AS platform_fee_amount_raw,
        (amount_raw * platform_fee_percentage)/POWER(10, bnb_bep20_tokens.decimals) AS platform_fee_amount,
        (amount_raw * platform_fee_percentage)/POWER(10, bnb_bep20_tokens.decimals)*prices.price AS platform_fee_amount_usd,
        CAST(100 * platform_fee_percentage AS DOUBLE) AS platform_fee_percentage,
        CAST(amount_raw * royalty_fee_percentage AS DOUBLE) AS royalty_fee_amount_raw,
        (amount_raw * royalty_fee_percentage)/POWER(10, bnb_bep20_tokens.decimals) AS royalty_fee_amount,
        (amount_raw * royalty_fee_percentage)/POWER(10, bnb_bep20_tokens.decimals)*prices.price AS royalty_fee_amount_usd,
        CAST(100*royalty_fee_percentage AS DOUBLE) AS royalty_fee_percentage,
        CAST(null AS STRING) AS royalty_fee_receive_address,
        COALESCE(events.currency_symbol, bnb_bep20_tokens.symbol) AS royalty_fee_currency_symbol,
        events.block_number || '-' || events.tx_hash || '-' || events.evt_index AS unique_trade_id

    FROM events
    LEFT JOIN {{ ref('nft_aggregators') }} agg ON events.buyer=agg.contract_address AND agg.blockchain='bnb'
    LEFT JOIN {{ ref('tokens_erc20_legacy') }} bnb_bep20_tokens ON bnb_bep20_tokens.contract_address=events.currency_contract AND bnb_bep20_tokens.blockchain='bnb'
    LEFT JOIN {{ ref('tokens_nft') }} bnb_nft_tokens ON bnb_nft_tokens.contract_address=events.currency_contract AND bnb_nft_tokens.blockchain='bnb'
    LEFT JOIN {{ source('prices', 'usd') }} prices ON prices.minute=date_trunc('minute', events.block_time)
    AND (prices.contract_address=events.currency_contract AND prices.blockchain=events.blockchain)
        {% if is_incremental() %}
        AND prices.minute >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    INNER JOIN {{ source('bnb','transactions') }} bt ON bt.hash=events.tx_hash
    AND bt.block_time=events.block_time
        {% if is_incremental() %}
        AND bt.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
