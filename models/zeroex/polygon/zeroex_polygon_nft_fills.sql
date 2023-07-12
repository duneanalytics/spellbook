{{  config(
        alias='nft_fills',
        materialized='incremental',
        partition_by = ['block_date'],
        unique_key = ['block_date', 'tx_hash', 'evt_index'],
        on_schema_change='sync_all_columns',     
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(\'["polygon"]\',
                                "project",
                                "zeroex",
                                \'["rantum","bakabhai993", "danning.sui"]\') }}'
    )
}}

{% set zeroex_v4_nft_start_date = '2022-03-01' %}

--sample query on dune v2: https://dune.com/queries/1844568 
WITH tbl_cte_transaction AS
(
    SELECT  evt_block_time
         , evt_tx_hash
         , evt_index
         , maker
         , taker
         , matcher
         , erc721Token      AS nft_address
         , erc721TokenId    AS nft_id
         , 'erc721'         AS label
         , '1'             AS nft_cnt   
         , CASE
                WHEN erc20Token in ('0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', '0x0000000000000000000000000000000000001010') 
                THEN '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
                ELSE erc20Token
            END             AS price_label
         , erc20Token       AS token
         , erc20TokenAmount AS token_amount_raw
    FROM {{ source ('zeroex_polygon', 'ExchangeProxy_evt_ERC721OrderFilled') }}
    WHERE 1 = 1 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
        {% if not is_incremental() %}
        AND evt_block_time >= '{{zeroex_v4_nft_start_date}}'
        {% endif %}

    UNION ALL

    SELECT  evt_block_time
            , evt_tx_hash
            , evt_index
            , maker
            , taker
            , matcher
            , erc1155Token      AS nft_address
            , erc1155TokenId    AS nft_id
            , 'erc1155'         AS label
            , erc1155FillAmount  as nft_cnt
            , CASE
                WHEN erc20Token in ('0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', '0x0000000000000000000000000000000000001010') 
                THEN '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'
                ELSE erc20Token
                END             AS price_label
            , erc20Token        AS token
            , erc20FillAmount   AS token_amount_raw
    FROM {{ source ('zeroex_polygon', 'ExchangeProxy_evt_ERC1155OrderFilled') }}
    WHERE 1 = 1 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
        {% if not is_incremental() %}
        AND evt_block_time >= '{{zeroex_v4_nft_start_date}}'
        {% endif %}
)
, tbl_usd AS
(
    SELECT  contract_address
           ,minute
           ,price
           ,decimals
           ,symbol
    FROM
    (
        SELECT  *
               ,row_number() OVER(PARTITION BY contract_address,minute ORDER BY minute DESC) AS ranker
        FROM {{ source('prices', 'usd') }} p
        WHERE 1=1
            AND blockchain = 'polygon'
            AND p.contract_address IN ( SELECT DISTINCT price_label FROM tbl_cte_transaction) 
            {% if is_incremental() %}
            AND minute > now() - interval '100 days'
            {% endif %}
            {% if not is_incremental() %}
            AND minute >= '{{zeroex_v4_nft_start_date}}' 
            {% endif %}
    ) a
    WHERE ranker = 1 
) 
, tbl_master AS
(
SELECT a.evt_block_time                                      AS block_time
     , try_cast(date_trunc('day', a.evt_block_time) AS date) AS block_date
     , a.evt_index
     , a.evt_tx_hash                                         AS tx_hash
     , a.maker                                              
     , a.taker
     , a.matcher
     , a.nft_address
     , a.nft_id
     , a.nft_cnt as number_of_items
     , a.label
     , a.price_label
     , a.token
     , a.token_amount_raw
     , CASE
            WHEN token = '0x0000000000000000000000000000000000001010'
            THEN 'MATIC'
            ELSE b.symbol
      END                                                    AS symbol
     , b.price * (a.token_amount_raw / pow(10, b.decimals))  AS price_usd
     , (a.token_amount_raw / pow(10, b.decimals))            AS token_amount
     , c.name                                                AS project_name
FROM tbl_cte_transaction AS a
LEFT JOIN tbl_usd AS b
    ON date_trunc('minute', a.evt_block_time) = b.minute
    AND a.price_label = b.contract_address
LEFT JOIN {{ ref('tokens_nft_legacy') }} AS c
    ON nft_address = c.contract_address
    AND c.blockchain = 'polygon'
)
SELECT  *
FROM tbl_master