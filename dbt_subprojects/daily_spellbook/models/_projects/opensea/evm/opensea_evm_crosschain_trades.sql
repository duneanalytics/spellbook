{{
    config(
        schema = 'opensea',
        alias = 'evm_crosschain_token_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['sourceChain', 'block_time', 'tokenSymbolOnSource', 'amountUsd', 'amount', 'bridgor', 'tokenAddressOnSource', 'hash'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    )
}}

WITH erc20_bridging_0xa9059cbb_source AS (
    SELECT
        etr.blockchain AS sourceChain
        , etr.block_time
        ,  pfd.symbol AS tokenSymbolOnSource
        ,  (VARBINARY_TO_UINT256(SUBSTR(etr."data", 37, 32))/POW(10, pfd.decimals)) * pfd.price AS amountUsd
        , VARBINARY_TO_UINT256(SUBSTR(etr."data", 37, 32)) AS amount
        , etr."from" AS bridgor
        , etr."to" AS tokenAddressOnSource
        , etr.hash  
    FROM {{ source('evms', 'transactions') }} etr
    LEFT JOIN prices.day pfd 
        ON etr.blockchain = pfd.blockchain 
        AND etr."to" = pfd.contract_address 
        AND date_trunc('day', etr.block_time) = pfd."timestamp"
    WHERE SUBSTR(etr."data", VARBINARY_LENGTH(etr."DATA") - 3 , 4) = 0x865d8597 --opensea's tag
        AND SUBSTR(etr."data", 1, 4) = 0xa9059cbb
        AND etr.block_time > TRY_CAST('2025-03-12 23:42' AS TIMESTAMP)
            AND etr.blockchain IN (
                'ethereum'
                , 'abstract'
                , 'apechain'
                , 'arbitrum'
                , 'avalanche_c'
                , 'b3'
                , 'base'
                , 'berachain'
                , 'blast'
                , 'flow'
                , 'optimism'
                , 'polygon'
                , 'unichain'
                , 'ronin'
                , 'sei'
                , 'shape'
                , 'zora'
            )
        {% if is_incremental() %}
            AND {{ incremental_predicate('etr.block_time') }}
        {% endif %}
) --0xa9059cbb, --'f70da97812cb96acdf810712aa562db8dfa3dbef' is relaysolver -- then 'amount' comes --then 'inputdata' --then 'openseatag' --datalength 104 --to = token

, native_tokens AS (
    SELECT
        blockchain
        , symbol
        , priceSourceChain
        , priceSourceAddress
        , decimals
    FROM (VALUES
    ('ethereum', 'ETH', 'ethereum', 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 18)
    ,('abstract', 'ETH', 'ethereum', 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 18)
    ,('apechain', 'APE', 'apechain', 0x48b62137edfa95a428d35c09e44256a739f6b557, 18)
    ,('arbitrum', 'ETH', 'ethereum', 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 18)
    ,('avalanche_c', 'AVAX', 'avalanche_c', 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7, 18)
    ,('b3', 'ETH', 'ethereum', 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 18)
    ,('base', 'ETH', 'ethereum', 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 18)
    ,('berachain', 'BERA', 'berachain', 0x6969696969696969696969696969696969696969, 18)
    ,('blast', 'ETH', 'ethereum', 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 18)
    ,('flow', 'FLOW', 'flow', 0xd3bF53DAC106A0290B0483EcBC89d40FcC961f3e, 18)
    ,('hyperevm', 'HYPE', 'hyperevm', 0x5555555555555555555555555555555555555555, 18)
    ,('optimism', 'ETH', 'ethereum', 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 18)
    ,('polygon', 'MATIC', 'polygon', 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270, 18)
    ,('unichain', 'ETH', 'ethereum', 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 18)
    ,('ronin', 'RON', 'ronin', 0xe514d9deb7966c8be0ca922de8a064264ea6bcd4, 18)
    ,('sei', 'SEI', 'sei', 0xe30fedd158a2e3b13e9badaeabafc5516e95e8c7, 18)
    ,('shape', 'ETH', 'ethereum', 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 18)
    ,('zora', 'ETH', 'ethereum', 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 18)
    ) AS t(blockchain, symbol, priceSourceChain, priceSourceAddress, decimals)
)

, native_bridging_source AS (
    SELECT
        etr.blockchain AS sourceChain
        , etr.block_time
        , tna.symbol AS tokenSymbolOnSource
        , (etr."value"/POW(10, pfd.decimals)) * pfd.price AS amountUsd
        , etr."value" AS amount
        , etr."from" AS bridgor
        , tna.priceSourceAddress AS tokenAddressOnSource
        , etr.hash
    FROM {{ source('evms', 'transactions') }} etr
    LEFT JOIN native_tokens tna 
        ON etr.blockchain = tna.blockchain 
    LEFT JOIN prices.day pfd 
        ON tna.priceSourceChain = pfd.blockchain 
        AND tna.priceSourceAddress = pfd.contract_address 
        AND DATE_TRUNC('DAY', etr.block_time) = pfd."timestamp"
    WHERE SUBSTR(etr."data", VARBINARY_LENGTH(etr."DATA") - 3 , 4) = 0x865d8597 --opensea's tag
        AND etr."to" =  0xa5f565650890fba1824ee0f21ebbbf660a179934 --Reservoir: Relay Receiver
        AND etr.block_time > TRY_CAST('2025-03-12 23:42' AS TIMESTAMP)
        AND etr.blockchain IN (
            'ethereum'
            , 'abstract'
            , 'apechain'
            , 'arbitrum'
            , 'avalanche_c'
            , 'b3'
            , 'base'
            , 'berachain'
            , 'blast'
            , 'flow'
            , 'optimism'
            , 'polygon'
            , 'unichain'
            , 'ronin'
            , 'sei'
            , 'shape'
            , 'zora'
        )
        {% if is_incremental() %}
            AND {{ incremental_predicate('etr.block_time') }}
        {% endif %}
)--datalength = 36, sending native token --data just holds inputdata and tag check amount to = 0xa5f565650890fba1824ee0f21ebbbf660a179934 --Reservoir: Relay Receiver


, internalTxnBridging_native_0x30875056_source AS (
    SELECT 
        etr.blockchain AS sourceChain
        , etr.block_time
        , tna.symbol AS tokenSymbolOnSource
        , (eiv."value"/POW(10, pfd.decimals)) * pfd.price AS amountUsd
        , eiv."value" AS amount
        , etr."from" AS bridgor
        , tna.priceSourceAddress AS tokenAddressOnSource
        , etr.hash
    FROM {{ source('evms', 'transactions') }} etr
    INNER JOIN {{ source('evms', 'traces') }} eiv 
        ON etr.blockchain = eiv.blockchain 
        AND etr.hash = eiv.tx_hash 
        AND eiv."to" = 0xf70da97812CB96acDF810712Aa562db8dfA3dbEF --internalTxnTo 
        AND eiv."from" = 0xF5042e6ffaC5a625D4E7848e0b01373D8eB9e222 
        AND eiv.block_time > TRY_CAST('2025-03-12 23:42' AS TIMESTAMP) -- for efficiency
    LEFT JOIN native_tokens tna 
        ON etr.blockchain = tna.blockchain 
    LEFT JOIN prices.day pfd 
        ON tna.priceSourceChain = pfd.blockchain 
        AND tna.priceSourceAddress = pfd.contract_address 
        AND DATE_TRUNC('DAY', etr.block_time) = pfd."timestamp"
    WHERE SUBSTR(etr."data", 1, 4) = 0x30875056 
        AND SUBSTR(etr."data", VARBINARY_LENGTH(etr."data") -3 , 4 ) = 0x865d8597 --opensea's tag
        AND etr."to" = 0xbbbfd134e9b44bfb5123898ba36b01de7ab93d98 -- check internal txns for eth amount
        AND etr.block_time > TRY_CAST('2025-03-12 23:42' AS TIMESTAMP)
        AND etr.blockchain IN (
            'ethereum'
            , 'abstract'
            , 'apechain'
            , 'arbitrum'
            , 'avalanche_c'
            , 'b3'
            , 'base'
            , 'berachain'
            , 'blast'
            , 'flow'
            , 'optimism'
            , 'polygon'
            , 'unichain'
            , 'ronin'
            , 'sei'
            , 'shape'
            , 'zora'
        )
        {% if is_incremental() %}
            AND {{ incremental_predicate('etr.block_time') }}
            AND {{ incremental_predicate('eiv.block_time') }}
        {% endif %}
)

, internalTxnBridging_erc20_0x30875056_source AS (
    SELECT 
        etr.blockchain AS sourceChain
        , etr.block_time
        , pfd.symbol AS tokenSymbolOnSource
        , (VARBINARY_TO_UINT256(eiv."data")/POW(10, pfd.decimals)) * pfd.price AS amountUsd
        , VARBINARY_TO_UINT256(eiv."data") AS amount
        , etr."from" AS bridgor
        , eiv.contract_address AS tokenAddressOnSource --wrappedVersion's address for native tokens
        , etr.hash
    FROM {{ source('evms', 'transactions') }} etr
    INNER JOIN {{ source('evms', 'logs') }} eiv 
        ON etr.blockchain = eiv.blockchain 
        AND etr.hash = eiv.tx_hash 
        AND eiv."topic2" = 0x000000000000000000000000f70da97812cb96acdf810712aa562db8dfa3dbef --internalTxnTo 
        AND eiv."topic1" = 0x000000000000000000000000f5042e6ffac5a625d4e7848e0b01373d8eb9e222 
        AND eiv.block_time > TRY_CAST('2025-03-12 23:42' AS TIMESTAMP) -- for efficiency
        AND eiv."topic0" = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
    LEFT JOIN prices.day pfd 
        ON etr.blockchain = pfd.blockchain 
        AND eiv.contract_address = pfd.contract_address 
        AND DATE_TRUNC('DAY', etr.block_time) = pfd."timestamp"
    WHERE substr(etr."data", 1, 4) = 0x30875056 
        AND substr(etr."data", VARBINARY_LENGTH(etr."data") -3 , 4 ) = 0x865d8597 --opensea's tag
        AND etr."to" = 0xbbbfd134e9b44bfb5123898ba36b01de7ab93d98 -- check internal txns for eth amount
        AND etr.block_time > TRY_CAST('2025-03-12 23:42' AS TIMESTAMP)
        AND etr.blockchain IN (
            'ethereum'
            , 'abstract'
            , 'apechain'
            , 'arbitrum'
            , 'avalanche_c'
            , 'b3'
            , 'base'
            , 'berachain'
            , 'blast'
            , 'flow'
            , 'optimism'
            , 'polygon'
            , 'unichain'
            , 'ronin'
            , 'sei'
            , 'shape'
            , 'zora'
        )
        {% if is_incremental() %}
            AND {{ incremental_predicate('etr.block_time') }}
            AND {{ incremental_predicate('eiv.block_time') }}
        {% endif %}
)

, sources AS (
    SELECT * FROM internalTxnBridging_erc20_0x30875056_source
    UNION ALL
    SELECT * FROM internalTxnBridging_native_0x30875056_source
    UNION ALL
    SELECT * FROM native_bridging_source
    UNION ALL
    SELECT * FROM erc20_bridging_0xa9059cbb_source
)

SELECT * FROM sources