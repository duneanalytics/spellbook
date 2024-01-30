{{  config(
    
        schema = 'zeroex_celo',
        alias = 'api_fills',
        materialized='incremental',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'tx_hash', 'evt_index'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(\'["celo"]\',
                                "project",
                                "zeroex",
                                \'["rantum"]\') }}'
        
    )
}}

{% set zeroex_v3_start_date = '2019-12-01' %}
{% set zeroex_v4_start_date = '2021-01-06' %}

-- Test Query here: https://dune.com/queries/2755622  / https://dune.com/queries/2755822 

WITH zeroex_tx AS (
    SELECT tx_hash,
           max(affiliate_address) as affiliate_address
    FROM (
       SELECT tr.tx_hash,
                CASE
                            WHEN bytearray_position(INPUT, 0x869584cd ) <> 0 THEN SUBSTRING(INPUT
                                                                                   FROM (bytearray_position(INPUT, 0x869584cd) + 16)
                                                                                   FOR 20)
                            WHEN bytearray_position(INPUT, 0xfbc019a7) <> 0 THEN SUBSTRING(INPUT
                                                                                   FROM (bytearray_position(INPUT, 0xfbc019a7 ) + 16)
                                                                                   FOR 20)
                        END AS affiliate_address
        FROM {{ source('celo', 'traces') }} tr
        WHERE tr.to IN (
                -- exchange contract
                0x61935cbdd02287b511119ddb11aeb42f1593b7ef, 
                -- forwarder addresses
                0x6958f5e95332d93d21af0d7b9ca85b8212fee0a5,
                0x4aa817c6f383c8e8ae77301d18ce48efb16fd2be,
                0x4ef40d1bf0983899892946830abf99eca2dbc5ce, 
                -- exchange proxy
                0xdef1c0ded9bec7f1a1670819833240f027b25eff
                )
                AND (
                    bytearray_position(INPUT, 0x869584cd ) <> 0
                    OR bytearray_position(INPUT, 0xfbc019a7 ) <> 0
                )
                
                {% if is_incremental() %}
                AND block_time >= date_trunc('day', now() - interval '7' day) 
                {% endif %}
                {% if not is_incremental() %}
                AND block_time >= cast('{{zeroex_v3_start_date}}' as date)
                {% endif %}
    ) temp
    group by tx_hash

),

ERC20BridgeTransfer AS (
    SELECT 
            logs.tx_hash,
            logs.block_number                      AS block_number,
            INDEX                                   AS evt_index,
            logs.contract_address,
            block_time                              AS block_time,
            bytearray_substring(DATA, 142, 20) AS maker,
            bytearray_substring(DATA, 172, 20) AS taker,
            bytearray_substring(DATA, 14, 20) AS taker_token,
            bytearray_substring(DATA, 45, 20) AS maker_token,
            bytearray_to_uint256((bytearray_substring(DATA, 77, 20)) ) AS taker_token_amount_raw,
            bytearray_to_uint256(bytearray_substring(DATA, 110, 20)) AS maker_token_amount_raw,
            'ERC20BridgeTransfer'                   AS type,
            zeroex_tx.affiliate_address             AS affiliate_address,
            TRUE                                    AS swap_flag,
            FALSE                                   AS matcha_limit_order_flag
    FROM {{ source('celo', 'logs') }} logs
    INNER JOIN zeroex_tx ON zeroex_tx.tx_hash = logs.tx_hash
    WHERE topic0 = 0x349fc08071558d8e3aa92dec9396e4e9f2dfecd6bb9065759d1932e7da43b8a9
    
    {% if is_incremental() %}
    AND block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    {% if not is_incremental() %}
    AND block_time >= cast('{{zeroex_v3_start_date}}' as date)
    {% endif %}

), 
BridgeFill AS (
    SELECT 
            logs.tx_hash,
            logs.block_number                      AS block_number,
            INDEX                                           AS evt_index,
            logs.contract_address,
            block_time                                      AS block_time,
            bytearray_substring(DATA, 13, 20) AS maker,
            0xdef1c0ded9bec7f1a1670819833240f027b25eff AS taker,
            bytearray_substring(DATA, 45, 20) AS taker_token,
            bytearray_substring(DATA, 77, 20) AS maker_token,
            bytearray_to_uint256((bytearray_substring(DATA, 109, 20))) AS taker_token_amount_raw,
            bytearray_to_uint256((bytearray_substring(DATA, 141, 20))) AS maker_token_amount_raw,
            'BridgeFill'                                    AS type,
            zeroex_tx.affiliate_address                     AS affiliate_address,
            TRUE                                            AS swap_flag,
            FALSE                                           AS matcha_limit_order_flag
    FROM {{ source('celo', 'logs') }} logs
    INNER JOIN zeroex_tx ON zeroex_tx.tx_hash = logs.tx_hash
    WHERE topic0 = 0xff3bc5e46464411f331d1b093e1587d2d1aa667f5618f98a95afc4132709d3a9
        AND contract_address = 0xdb6f1920a889355780af7570773609bd8cb1f498

        {% if is_incremental() %}
        AND block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        {% if not is_incremental() %}
        AND block_time >= cast('{{zeroex_v4_start_date}}' as date)
        {% endif %}
), 
NewBridgeFill AS (
    SELECT 
            logs.tx_hash as tx_hash,
            logs.block_number                      AS block_number,
            INDEX                                           AS evt_index,
            logs.contract_address,
            block_time                                      AS block_time,
            bytearray_substring(DATA, 13, 20) AS maker,
            0xdef1c0ded9bec7f1a1670819833240f027b25eff AS taker,
            bytearray_substring(DATA, 45, 20) AS taker_token,
            bytearray_substring(DATA, 77, 20) AS maker_token,
            bytearray_to_uint256(bytearray_substring(DATA, 109, 20)) AS taker_token_amount_raw,
            bytearray_to_uint256(bytearray_substring(DATA, 141, 20)) AS maker_token_amount_raw,
            'BridgeFill'                                 AS type,
            zeroex_tx.affiliate_address                     AS affiliate_address,
            TRUE                                            AS swap_flag,
            FALSE                                           AS matcha_limit_order_flag
    FROM {{ source('celo' ,'logs') }} logs
    INNER JOIN zeroex_tx ON zeroex_tx.tx_hash = logs.tx_hash
    WHERE topic0 = 0xe59e71a14fe90157eedc866c4f8c767d3943d6b6b2e8cd64dddcc92ab4c55af8
        AND contract_address = 0xdb6f1920a889355780af7570773609bd8cb1f498

        {% if is_incremental() %}
        AND block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        {% if not is_incremental() %}
        AND block_time >= cast('{{zeroex_v4_start_date}}' as date)
        {% endif %}
),

all_tx AS (
    
    SELECT *
    FROM ERC20BridgeTransfer
    UNION ALL SELECT *
    FROM BridgeFill
    UNION ALL 
    SELECT *
    FROM NewBridgeFill 
    
    
)

SELECT 
        all_tx.tx_hash,
        all_tx.block_number,
        all_tx.evt_index,
        all_tx.contract_address,
        all_tx.block_time,
        cast(date_trunc('day', all_tx.block_time) AS date) AS block_date,
        cast(date_trunc('month', all_tx.block_time) AS date) AS block_month,
        maker,
        CASE
             WHEN taker = 0xdef1c0ded9bec7f1a1670819833240f027b25eff THEN tx."from"
            ELSE taker
        END AS taker, -- fix the user masked by ProxyContract issue
         taker_token,
        ts.symbol AS taker_symbol,
        maker_token,
        ms.symbol AS maker_symbol,
        CASE WHEN lower(ts.symbol) > lower(ms.symbol) THEN concat(ms.symbol, '-', ts.symbol) ELSE concat(ts.symbol, '-', ms.symbol) END AS token_pair,
        taker_token_amount_raw / pow(10, tp.decimals) AS taker_token_amount,
        taker_token_amount_raw,
        maker_token_amount_raw / pow(10, mp.decimals) AS maker_token_amount,
        maker_token_amount_raw,
        all_tx.type,
        max(affiliate_address) over (partition by all_tx.tx_hash) as affiliate_address,
        swap_flag,
        matcha_limit_order_flag,
        CASE WHEN maker_token IN (0x04068da6c83afcfa0e13ba15a6696662335d5b75,0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83,0x74b23882a30290451a17c44f4f05243b6b58c76d,0x049d68029688eabf473097a2fc38ef61633a3c7a,0x82f0b8b456c1a451378467398982d4834b6829c1, 0x321162cd933e2be498cd2267a90534a804051b11)
             THEN (all_tx.maker_token_amount_raw / pow(10, mp.decimals)) * mp.price
             WHEN taker_token IN (0x04068da6c83afcfa0e13ba15a6696662335d5b75,0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83,0x74b23882a30290451a17c44f4f05243b6b58c76d,0x049d68029688eabf473097a2fc38ef61633a3c7a,0x82f0b8b456c1a451378467398982d4834b6829c1, 0x321162cd933e2be498cd2267a90534a804051b11)     
             THEN (all_tx.taker_token_amount_raw / pow(10, tp.decimals)) * tp.price
             ELSE COALESCE((all_tx.maker_token_amount_raw / pow(10, mp.decimals)) * mp.price, (all_tx.taker_token_amount_raw / pow(10, tp.decimals)) * tp.price)
             END AS volume_usd,
        tx."from" AS tx_from,
        tx.to AS tx_to,
        'celo' AS blockchain
FROM all_tx
INNER JOIN {{ source('celo', 'transactions')}} tx ON all_tx.tx_hash = tx.hash

{% if is_incremental() %}
AND tx.block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}
{% if not is_incremental() %}
AND tx.block_time >= cast('{{zeroex_v3_start_date}}' as date)
{% endif %}

LEFT JOIN {{ source('prices', 'usd') }} tp ON date_trunc('minute', all_tx.block_time) = tp.minute
AND CASE
         WHEN all_tx.taker_token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
        ELSE all_tx.taker_token
    END = tp.contract_address
AND tp.blockchain = 'celo'

{% if is_incremental() %}
AND tp.minute >= date_trunc('day', now() - interval '7' day)
{% endif %}
{% if not is_incremental() %}
AND tp.minute >= cast('{{zeroex_v3_start_date}}' as date)
{% endif %}

LEFT JOIN {{ source('prices', 'usd') }} mp ON DATE_TRUNC('minute', all_tx.block_time) = mp.minute
AND CASE
        WHEN all_tx.maker_token =  0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
        ELSE all_tx.maker_token
    END = mp.contract_address
AND mp.blockchain = 'celo'

{% if is_incremental() %}
AND mp.minute >= date_trunc('day', now() - interval '7' day)
{% endif %}
{% if not is_incremental() %}
AND mp.minute >= cast('{{zeroex_v3_start_date}}' as date)
{% endif %}

LEFT OUTER JOIN {{ source('tokens', 'erc20')  }} ts ON ts.contract_address = taker_token and ts.blockchain = 'celo'
LEFT OUTER JOIN {{ source('tokens', 'erc20')  }} ms ON ms.contract_address = maker_token and ms.blockchain = 'celo'