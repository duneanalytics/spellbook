{{ config(
    schema = 'opensea_v3_ethereum',
    tags = ['dunesql'],
    alias = alias('base_trades'),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

{% set opensea_v3_start_date = '2022-06-12' %}

WITH base_data AS (
     SELECT evt_block_time AS block_time
    , evt_block_number AS block_number
    , orderHash AS order_hash
    , offerer
    , recipient
    , evt_tx_hash AS tx_hash
    , contract_address AS project_contract_address
    , consideration
    , offer
    , CASE WHEN recipient = 0x0000000000000000000000000000000000000000 THEN 'Private Sale'
        WHEN try(json_extract_scalar(consideration[1], '$.itemType') IN ('2' , '3')) --erc721 or erc1155
        THEN 'Offer Accepted'
        ELSE 'Buy'
        END AS trade_category
    FROM {{ source('seaport_ethereum', 'Seaport_evt_OrderFulfilled') }}
    WHERE zone IN (
        0xf397619df7bfd4d1657ea9bdd9df7ff888731a11
       ,0x9b814233894cd227f561b78cc65891aa55c62ad2
       ,0x004c00500000ad104d7dbd00e3ae0a5c00560c00
       ,0x000000e7ec00e7b300774b00001314b8610022b8
       ,0x110b2b128a9ed1be5ef3232d8e4e41640df5c2cd
       )
    {% if is_incremental() %}
    AND evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    AND evt_block_time >= TIMESTAMP '{{opensea_v3_start_date}}'
    {% endif %}
    )

, considerations_and_offers AS (
    SELECT block_time
    , block_number
    , 'consideration' AS trace_side
    , order_hash
    , tx_hash
    , CASE json_extract_scalar(consideration_item, '$.itemType')
        WHEN '0' THEN 'ETH'
        WHEN '1' THEN 'erc20'
        WHEN '2' THEN 'erc721'
        WHEN '3' THEN 'erc1155'
        END AS token_standard
    , consideration_index AS trace_index
    , project_contract_address
    , trade_category
    , from_hex(json_extract_scalar(consideration_item, '$.token')) AS token_address
    , json_extract_scalar(consideration_item, '$.amount') AS amount
    , json_extract_scalar(consideration_item, '$.identifier') AS identifier
    , from_hex(json_extract_scalar(consideration_item, '$.recipient')) AS recipient
    , offerer
    FROM (
        SELECT block_time
        , block_number
        , order_hash
        , tx_hash
        , offerer
        , project_contract_address
        , trade_category
        , consideration_index
        , consideration_item
        FROM base_data
        CROSS JOIN UNNEST(consideration) WITH ordinality AS t (consideration_item, consideration_index)
        )
    
    UNION ALL
    
    SELECT block_time
    , block_number
    , 'offer' AS trace_side
    , order_hash
    , tx_hash
    , CASE json_extract_scalar(offer_item, '$.itemType')
        WHEN '0' THEN 'ETH'
        WHEN '1' THEN 'erc20'
        WHEN '2' THEN 'erc721'
        WHEN '3' THEN 'erc1155'
        END AS token_standard
    , offer_index AS trace_index
    , project_contract_address
    , trade_category
    , from_hex(json_extract_scalar(offer_item, '$.token')) AS token_address
    , json_extract_scalar(offer_item, '$.amount') AS amount
    , json_extract_scalar(offer_item, '$.identifier') AS identifier
    , recipient
    , offerer
    FROM (
        SELECT block_time
        , block_number
        , order_hash
        , tx_hash
        , recipient
        , offerer
        , project_contract_address
        , trade_category
        , offer_index
        , offer_item
        FROM base_data
        CROSS JOIN UNNEST(offer) WITH ordinality AS t (offer_item, offer_index)
        )
    )

, identified_traces AS (
    SELECT block_time
    , block_number
    , trace_side
    , order_hash
    , tx_hash
    , token_standard
    , token_address
    , CAST(amount AS double) AS amount
    , identifier
    , recipient
    , offerer
    , trace_index
    , project_contract_address
    , trade_category
    , CASE WHEN token_standard IN ('erc721', 'erc1155') THEN 'nft'
        WHEN recipient IN (
            0x8de9c5a032463c561423387a9648c5c7bcc5bc90 -- OpenSea Fees
            , 0x34ba0f2379bf9b81d09f7259892e26a8b0885095 -- OpenSea Fees 2
            , 0x0000a26b00c1f0df003000390027140000faa719 -- OpenSea Fees 3
            ) THEN 'os_fees'
        ELSE 'payment_or_royalties'
        END AS trace_type
    FROM considerations_and_offers
    )

, nft AS (
    SELECT nft.block_time
    , nft.block_number
    , nft.order_hash
    , nft.tx_hash
    , nft.project_contract_address
    , nft.nft_amount
    , nft.token_standard
    , nft.nft_contract_address
    , nft.trade_category
    , nft.nft_token_id
    , MIN_BY(nftt."from", nftt.evt_index) AS seller
    , MAX_BY(nftt.to, nftt.evt_index) AS buyer
    FROM (
        SELECT block_time
        , block_number
        , order_hash
        , tx_hash
        , project_contract_address
        , SUM(COALESCE(amount, 1)) AS nft_amount
        , MIN_BY(token_standard, trace_index) AS token_standard
        , MIN_BY(token_address, trace_index) AS nft_contract_address
        , CAST(MIN_BY(identifier, trace_index) AS UINT256) AS nft_token_id
        , MIN_BY(trade_category, trace_index) AS trade_category
        FROM identified_traces
        WHERE trace_type = 'nft'
        GROUP BY 1, 2, 3, 4, 5
        ) nft
    LEFT JOIN {{ ref('nft_ethereum_transfers') }} nftt ON nft.block_number=nftt.block_number
        AND nft.nft_contract_address=nftt.contract_address
        AND nft.nft_token_id=nftt.token_id
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    )

, fungible AS (
    SELECT ft.block_number
    , ft.order_hash
    , CAST(ABS(SUM(COALESCE(ft.price_raw, 0))) AS UINT256) AS price_raw
    , CASE WHEN MIN_BY(ft.currency_contract, ft.trace_index)=0x0000000000000000000000000000000000000000 THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 ELSE MIN_BY(ft.currency_contract, ft.trace_index) END AS currency_contract
    FROM (
        SELECT block_number
        , order_hash
        , token_address AS currency_contract
        , trace_index
        , SUM(amount) AS price_raw
        FROM identified_traces
        WHERE trace_type IN ('payment_or_royalties', 'os_fees')
        AND trace_side = 'offer'
        GROUP BY 1, 2, 3, 4
        
        UNION ALL
        
        SELECT block_number
        , order_hash
        , token_address AS currency_contract
        , trace_index
        , -SUM(amount) AS price_raw
        FROM identified_traces
        WHERE trace_type IN ('payment_or_royalties', 'os_fees')
        AND trace_side = 'consideration'
        GROUP BY 1, 2, 3, 4
        ) ft
    GROUP BY 1, 2
    )

, royalty_fees AS (
    SELECT block_number
    , order_hash
    , MAX_BY(recipient, amount) AS royalty_fee_address
    , SUM(amount) AS royalty_fee_amount_raw
    FROM (
        SELECT t.block_number
        , t.order_hash
        , recipient
        , SUM(t.amount) AS amount
        FROM identified_traces t
        INNER JOIN fungible ON fungible.block_number=t.block_number
            AND fungible.order_hash=t.order_hash
            AND fungible.price_raw >= CAST(10*t.amount AS UINT256)
        INNER JOIN nft ON nft.block_number=t.block_number
            AND nft.order_hash=t.order_hash
            AND t.recipient NOT IN (nft.seller, nft.buyer)
        WHERE t.trace_type = 'payment_or_royalties'
        GROUP BY 1, 2, 3
        )
    GROUP BY 1, 2
    )

, os_fees AS (
    SELECT block_number
    , order_hash
    , MAX_BY(recipient, amount) AS platform_fee_address
    , SUM(amount) AS platform_fee_amount_raw
    FROM (
        SELECT block_number
        , order_hash
        , recipient
        , SUM(amount) AS amount
        FROM identified_traces
        WHERE trace_type = 'os_fees'
        GROUP BY 1, 2, 3
        )
    GROUP BY 1, 2
    )

SELECT date_trunc('day', nft.block_time) AS block_date
, 'ethereum' AS blockchain
, 'opensea' AS project
, 'v3' AS version
, nft.block_number
, nft.tx_hash
, ROW_NUMBER() OVER (PARTITION BY nft.block_number, nft.tx_hash ORDER BY nft.order_hash DESC) AS sub_tx_trade_id
, nft.trade_category
, 'secondary' AS trade_type
, nft.buyer
, nft.seller
, nft.nft_contract_address
, CAST(nft.nft_token_id AS UINT256) AS nft_token_id
, CAST(nft.nft_amount AS UINT256) AS nft_amount
, CAST(fungible.price_raw AS UINT256) AS price_raw
, fungible.currency_contract
, nft.project_contract_address
, CAST(COALESCE(os_fees.platform_fee_amount_raw, 0) AS UINT256) AS platform_fee_amount_raw
, os_fees.platform_fee_address
, CAST(COALESCE(royalty_fees.royalty_fee_amount_raw, 0) AS UINT256) AS royalty_fee_amount_raw
, royalty_fees.royalty_fee_address
FROM nft
INNER JOIN fungible ON nft.block_number=fungible.block_number
    AND nft.order_hash=fungible.order_hash
LEFT JOIN royalty_fees ON nft.block_number=royalty_fees.block_number
    AND nft.order_hash=royalty_fees.order_hash
LEFT JOIN os_fees ON nft.block_number=os_fees.block_number
    AND nft.order_hash=os_fees.order_hash