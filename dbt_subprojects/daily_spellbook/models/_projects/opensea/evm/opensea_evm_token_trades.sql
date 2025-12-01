{{
    config(
        schema = 'opensea',
        alias = 'evm_token_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'generatedIndex', 'blockchain'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    )
}}

WITH opensea_evm_hashs AS (
    SELECT 
        blockchain
        , hash
    FROM {{ source('evms', 'transactions') }}
    WHERE SUBSTR("data", varbinary_length("data") - 3, 4) = 0x865d8597
        AND block_time > TRY_CAST('2025-03-12 23:42' AS TIMESTAMP)
        AND blockchain IN (
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
            AND {{ incremental_predicate('block_time') }}
        {% endif %}
)

SELECT 
    det.blockchain
    , det.project
    , det."version"
    , det.project_contract_address
    , det.tx_from
    , det.block_time
    , det.token_bought_symbol
    , det.token_bought_address
    , det.token_sold_symbol
    , det.token_sold_address 
    , det.token_sold_amount
    , det.amount_usd
    , det.tx_hash
    , det.block_number * POW(10,5) + evt_index AS generatedIndex
FROM dex.trades det
INNER JOIN opensea_evm_hashs oeh 
    ON det.tx_hash = oeh.hash 
    AND det.blockchain = oeh.blockchain
WHERE det.block_time  > TRY_CAST('2025-03-12 23:42' AS TIMESTAMP)
    {% if is_incremental() %}
        AND {{ incremental_predicate('det.block_time') }}
    {% endif %}
