{{ config(
    schema = 'tokens_ethereum',
    alias = 'base_transfers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_date','unique_key'],
    )
}}

{{transfers_base(
    blockchain='ethereum',
    traces = source('ethereum','traces'),
    transactions = source('ethereum','transactions'),
    erc20_transfers = source('erc20_ethereum','evt_Transfer')
)}}

UNION ALL

SELECT *
FROM
(
    {{transfers_base_wrapped_token(
        blockchain='ethereum',
        transactions = source('ethereum','transactions'),
        wrapped_token_deposit = source('zeroex_ethereum', 'weth9_evt_deposit'),
        wrapped_token_withdrawal = source('zeroex_ethereum', 'weth9_evt_withdrawal'),
    )
    }}
)

UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(['w.block_number', "'withdrawal'", 'w.withdrawal_index']) }} AS unique_key
    , 'ethereum' AS blockchain
    , cast(date_trunc('month', w.block_time) as date) AS block_month
    , cast(date_trunc('day', w.block_time) as date) AS block_date
    , w.block_time
    , w.block_number
    , cast(NULL as varbinary) AS tx_hash
    , cast(NULL as integer) AS evt_index
    , cast(NULL as array(bigint)) AS trace_address
    , 'native' AS token_standard
    , cast(NULL as varbinary) AS tx_from
    , cast(NULL as varbinary) AS tx_to
    , cast(NULL as integer) AS tx_index
    , 0x0000000000000000000000000000000000000000 AS "from"
    , w.address AS to
    , (select token_address from {{ source('dune','blockchains') }} where name = 'ethereum') AS contract_address
    , (cast(w.amount as uint256) * uint256 '1000000000') AS amount_raw
FROM (
    SELECT
        block_time
        , block_number
        , "index" AS withdrawal_index
        , address
        , amount
    FROM {{ source('ethereum', 'withdrawals') }}
) w
WHERE w.address IS NOT NULL
    AND w.amount > 0
{% if is_incremental() %}
    AND {{ incremental_predicate('w.block_time') }}
{% endif %}

UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(['b.number', "'miner_reward'"]) }} AS unique_key
    , 'ethereum' AS blockchain
    , cast(date_trunc('month', b.time) as date) AS block_month
    , b.date AS block_date
    , b.time AS block_time
    , b.number AS block_number
    , cast(NULL as varbinary) AS tx_hash
    , cast(NULL as integer) AS evt_index
    , cast(NULL as array(bigint)) AS trace_address
    , 'native' AS token_standard
    , cast(NULL as varbinary) AS tx_from
    , cast(NULL as varbinary) AS tx_to
    , cast(NULL as integer) AS tx_index
    , 0x0000000000000000000000000000000000000000 AS "from"
    , b.miner AS to
    , (select token_address from {{ source('dune','blockchains') }} where name = 'ethereum') AS contract_address
    , r.amount_raw
FROM {{ source('ethereum', 'blocks') }} b
INNER JOIN (
    SELECT
        block_date
        , block_number
        , max(value) AS amount_raw -- use max reward trace per block for miner payout; sum(value) would include uncle rewards and overstate miner amount
    FROM {{ source('ethereum', 'traces') }}
    WHERE type = 'reward'
        AND value > uint256 '0'
        AND block_number <= 15537393
        {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
        {% endif %}
    GROUP BY 1, 2
) r
    ON r.block_date = b.date
    AND r.block_number = b.number
WHERE b.number <= 15537393
{% if is_incremental() %}
    AND {{ incremental_predicate('b.time') }}
{% endif %}
