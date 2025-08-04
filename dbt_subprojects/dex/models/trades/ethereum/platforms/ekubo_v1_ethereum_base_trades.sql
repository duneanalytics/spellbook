-- #############################################################################################################################
-- ##                                          Methodology & Design Choices                                                   ##
-- #############################################################################################################################
-- Ekubo is a high-efficiency Automated Market Maker (AMM) that utilizes a concentrated liquidity model.
-- Primarily built using cairo for the starknet community. The have also included Ethereum in March 2025

-- Links : 
-- 1. Website : https://ekubo.org/ 
-- 2. Docs: https://docs.ekubo.org/ 
-- 3. Contract address: https://docs.ekubo.org/integration-guides/reference/contract-addresses 
-- 4. Github: https://github.com/EkuboProtocol
-- 5. Twitter: https://x.com/EkuboProtocol 

-- This model deviates from standard DEX abstractions by sourcing primary trade data from `traces` instead of `events`.
-- The Ekubo contract's `swap` function does not emit token amounts, making traces the only reliable source for this critical data.
--
-- To construct a complete trade record, we join trace data with event logs. The `INNER JOIN` ensures we only include trades that have both a successful call trace
-- and a corresponding event log. This acts as a safeguard, as swaps that appear successful in traces but lack an
-- event log may represent failed or reverted state changes within the transaction.
--
-- We also explicitly filter out trades where both `output_delta0` and `output_delta1` are zero. These represent successful
-- calls that resulted in no actual token movement. This explains why the final trade count is slightly lower than the total
-- number of successful swap calls (e.g., 20 transaction calls over 5 months as of July 2025), without affecting the total volume.
-- Example of a zero-amount swap transaction: 0x87f217eba253b23288deebfc434f92c5018d9b90a906e8627589b3172578b29e
--
-- #############################################################################################################################


{{
    config(
        schema = 'ekubo_v1_ethereum',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

-- The project was deployed on 2025-03-14 07:41 PM. https://etherscan.io/tx/0x15cc649ad5dea0943e4d0ea81df42e0dcb11e46766ebd6aeeffd6e91aecaf9c9
{% set project_deployed_on = '2025-03-14 19:00' %}
{% set contract_addresses = [
    '0xe0e0e08a6a4b9dc7bd67bcb7aade5cf48157d444'
] %}


with trace_trades as 
(
    SELECT
        call_block_time as block_time
        , call_block_number AS block_number
        , CASE 
            WHEN isToken1 = TRUE THEN ABS(output_delta0)
            WHEN isToken1 = FALSE THEN ABS(output_delta1)
        END AS token_bought_amount_raw
        , CASE
            WHEN isToken1 = TRUE THEN ABS(output_delta1)
            WHEN isToken1 = FALSE THEN ABS(output_delta0)
        END AS token_sold_amount_raw
        , CASE
            WHEN isToken1 = TRUE THEN cast(JSON_EXTRACT(poolKey, '$.token0') as varchar)
            WHEN isToken1 = FALSE THEN cast(JSON_EXTRACT(poolKey, '$.token1') as varchar)
        END AS token_bought_address
        , CASE
            WHEN isToken1 = TRUE THEN cast(JSON_EXTRACT(poolKey, '$.token1') as varchar)
            WHEN isToken1 = FALSE THEN cast(JSON_EXTRACT(poolKey, '$.token0') as varchar)
        END AS token_sold_address
        , contract_address AS project_contract_address
        , call_tx_hash AS tx_hash
        , CAST(call_tx_from AS VARCHAR) AS taker 
        , CAST(null AS VARBINARY) AS maker
        -- , cast(JSON_EXTRACT(poolKey, '$.config') AS varchar) as config  This is the pool config. So if you are looking to calculate the colume of pool, please use this.
        , ROW_NUMBER() OVER (PARTITION BY call_tx_hash ORDER BY call_trace_address ASC) AS swap_number -- Row number is needed to match each trace call with a matching event in logs. Very helpful in multi step swaps

    FROM {{ source('ekubo_ethereum', 'ekubo_core_call_swap_611415377') }}
    WHERE 1=1
        AND call_success
        {% if is_incremental() %}
        and {{ incremental_predicate('call_block_time') }}
        {% endif %}
        AND (ABS(output_delta1) > 0 or ABS(output_delta0) > 0) -- Removes trades where no tokens are bought or sold.
)
, evt_trades as 
(
    SELECT
        block_time
        , tx_hash
        , index as evt_index
        , ROW_NUMBER() OVER (PARTITION BY tx_hash ORDER BY index ASC) AS swap_number
    FROM {{ source('ethereum', 'logs') }}
    WHERE 1=1
        AND block_time >= timestamp '{{ project_deployed_on }}'
        AND cast(contract_address as varchar) in 
        (
            {% for contract_address in contract_addresses %}
                '{{ contract_address }}'
            {% endfor %}
        )
        {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
        {% endif %}
        AND topic0 is null 
)
SELECT 

    'ethereum' AS blockchain
    , 'ekubo' AS project
    , '1' AS version
    , cast(date_trunc( 'month', tt.block_time) as date) AS block_month
    , cast(date_trunc( 'day', tt.block_time) as date) AS block_date
    , tt.block_time AS block_time
    , tt.block_number AS block_number
    , cast(tt.token_bought_amount_raw as uint256) AS token_bought_amount_raw
    , cast(tt.token_sold_amount_raw as uint256) AS token_sold_amount_raw
    , from_hex(token_bought_address) AS token_bought_address
    , from_hex(token_sold_address) AS token_sold_address
    , from_hex(tt.taker) AS taker
    , from_hex(tt.maker) AS maker
    , project_contract_address
    , tt.tx_hash
    , et.evt_index AS evt_index 

FROM trace_trades tt
INNER JOIN evt_trades et 
ON (
    tt.tx_hash=et.tx_hash 
    AND tt.block_time=et.block_time 
    AND tt.swap_number=et.swap_number
   )
 

