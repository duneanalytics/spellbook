{{ config(
        alias ='collections',
        partition_by='block_date',
        materialized='incremental',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["gnosis"]\',
                                    "sector",
                                    "nft",
                                    \'["hildobby"]\') }}',
        unique_key = ['unique_trade_id']
)
}}

WITH trades AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SUM(nftt.amount_usd) DESC) AS volume_ranking
    , nftt.nft_contract_address AS contract_address
    , SUM(nftt.amount_usd/pu.price) AS volume_native_currency
    , SUM(nftt.amount_usd) AS volume_usd
    , COUNT(*) AS trade_count
    FROM {{ ref('nft_trades') }} nftt
    INNER JOIN {{ ref('nft_gnosis_wash_trades') }} wt ON wt.unique_trade_id=nftt.unique_trade_id
        AND wt.is_wash_trade = FALSE
        {% if is_incremental() %}
        AND wt.block_time >= date_trunc("day", NOW() - interval '1 week')
        {% endif %}
    LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain = 'gnosis'
        AND pu.contract_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND pu.minute=date_trunc('minute', nftt.block_time)
    GROUP BY nftt.nft_contract_address
    )

, wash_trades AS (
    SELECT nftt.nft_contract_address AS contract_address
    , SUM(nftt.amount_usd/pu.price) AS wash_volume_native_currency
    , SUM(nftt.amount_usd) AS wash_volume_usd
    , COUNT(*) AS wash_trade_count
    FROM nft.trades nftt
    INNER JOIN {{ ref('nft_gnosis_wash_trades') }} wt ON wt.unique_trade_id=nftt.unique_trade_id
        AND wt.is_wash_trade = TRUE
        {% if is_incremental() %}
        AND wt.block_time >= date_trunc("day", NOW() - interval '1 week')
        {% endif %}
    LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain = 'gnosis'
        AND pu.contract_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND pu.minute=date_trunc('minute', nftt.block_time)
    GROUP BY nftt.nft_contract_address
    )

, supply AS (
        SELECT contract_address
        , COUNT(distinct token_id) AS distinct_token_ids
        , SUM(supply) AS current_supply
        , SUM(minted) AS minted_tokens
        FROM (
            SELECT contract_address
            , token_id
            , SUM(amount) AS supply
            , CAST(SUM(amount) AS double) AS minted
            FROM {{ ref('nft_gnosis_transfers') }}
            WHERE `from` = '0x0000000000000000000000000000000000000000'
            GROUP BY contract_address, token_id
            
            UNION ALL
            
            SELECT contract_address
            , token_id
            , -SUM(amount) AS supply
            , CAST(NULL AS double) AS minted
            FROM {{ ref('nft_gnosis_transfers') }}
            WHERE `to` = '0x0000000000000000000000000000000000000000'
            GROUP BY contract_address, token_id
            )
        GROUP BY contract_address
    )

, holders AS (
    SELECT contract_address
    , COUNT(distinct holder) AS holders
    FROM (
        SELECT contract_address
        , holder
        , SUM(COALESCE(quantity, 0)) AS quantity
        FROM (
            SELECT contract_address AS contract_address
            , `to` AS holder
            , COALESCE(SUM(CAST(amount AS double)), 0) AS quantity
            FROM {{ ref('nft_gnosis_transfers') }}
            WHERE `to`!='0x0000000000000000000000000000000000000000'
            GROUP BY contract_address, `to`
            UNION ALL
            SELECT contract_address AS contract_address
            , `from` AS holder
            , -COALESCE(SUM(CAST(amount AS double)), 0) AS quantity
            FROM {{ ref('nft_gnosis_transfers') }}
            WHERE `from`!='0x0000000000000000000000000000000000000000'
            {% if is_incremental() %}
            AND block_time >= date_trunc("day", NOW() - interval '1 week')
            {% endif %}
            GROUP BY contract_address, `from`
            )
        GROUP BY contract_address, holder
        )
    WHERE quantity > 0
    GROUP BY contract_address
    )

, mint AS (
    SELECT contract_address
    , MIN(block_time) AS first_mint
    , MAX(block_time) AS last_mint
    FROM {{ ref('nft_gnosis_transfers') }}
    WHERE `from` = '0x0000000000000000000000000000000000000000'
    GROUP BY contract_address
    )

, burns AS (
    SELECT contract_address
    , SUM(amount) AS burned_tokens
    FROM {{ ref('nft_gnosis_transfers') }}
    WHERE `to` = '0x0000000000000000000000000000000000000000'
    GROUP BY contract_address
    )

SELECT t.volume_ranking
, 'gnosis' AS blockchain
, tok.contract_address
, tok.name
, tok.standard
, tok.symbol
, 'xDAI' AS native_currency_symbol
, COALESCE(t.volume_native_currency, 0) AS volume_native_currency
, COALESCE(t.volume_usd, 0) AS volume_usd
, COALESCE(t.trade_count, 0) AS trade_count
, COALESCE(wt.wash_volume_native_currency, 0) AS wash_volume_native_currency
, COALESCE(wt.wash_volume_usd, 0) AS wash_volume_usd
, COALESCE(wt.wash_trade_count, 0) AS wash_trade_count
, s.current_supply
, s.minted_tokens
, s.distinct_token_ids
, h.holders
, m.first_mint
, m.last_mint
, COALESCE(b.burned_tokens, 0) AS burned_tokens
FROM tokens_gnosis.nft tok
LEFT JOIN trades t ON t.contract_address=tok.contract_address
LEFT JOIN wash_trades wt ON wt.contract_address=tok.contract_address
LEFT JOIN supply s ON s.contract_address=tok.contract_address
LEFT JOIN holders h ON h.contract_address=tok.contract_address
LEFT JOIN mint m ON m.contract_address=tok.contract_address
LEFT JOIN burns b ON b.contract_address=tok.contract_address
ORDER BY t.volume_ranking NULLS LAST