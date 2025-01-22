{%- macro yield_yak_reinvests(
        blockchain = null
    )
-%}

{%- set namespace_blockchain = 'yield_yak_' + blockchain -%}

WITH

{% if is_incremental() -%}
existing_contracts AS (
    SELECT
        t.contract_address
        , MAX(t.block_number) AS latest_block_number
        , MAX_BY(t.ratio, (t.block_number, t.tx_index, t.evt_index)) AS latest_ratio
        , MAX_BY(t.block_time, (t.block_number, t.tx_index, t.evt_index)) AS latest_block_time
        , MAX_BY(t.recent_reinvest_info, (t.block_number, t.tx_index, t.evt_index)) AS latest_recent_reinvest_info
    FROM
    {{ this }} t
    GROUP BY t.contract_address
),
{% endif -%}

combined AS (
    {%- for strategy in yield_yak_strategies(blockchain) %}
        SELECT
            s.contract_address
            , s.evt_tx_hash AS tx_hash
            , s.evt_index
            , s.evt_block_time AS block_time
            , CAST(date_trunc('day', s.evt_block_time) AS date) AS block_date
            , s.evt_block_number AS block_number
            , s.newTotalDeposits AS new_total_deposits
            , s.newTotalSupply AS new_total_supply
            , CASE WHEN s.newTotalSupply = 0 THEN 1 ELSE (1.0 * s.newTotalDeposits) / (1.0 * s.newTotalSupply) END AS ratio
            {%- if is_incremental() %}
            , c.latest_ratio
            , c.latest_block_time
            , c.latest_recent_reinvest_info
            {%- endif %}
        FROM {{ source(namespace_blockchain, strategy + '_evt_Reinvest') }} s
        {%- if is_incremental() %}
        LEFT JOIN
        existing_contracts c
            ON c.contract_address = s.contract_address
        WHERE
            ({{ incremental_predicate('s.evt_block_time') }}
            AND s.evt_block_number > c.latest_block_number)
            OR c.contract_address IS NULL -- This line allows for new contract_addresses being appended that were not already included in previous runs but also allows their entire historical data to be loaded
        {%- endif %}
        {% if not loop.last -%}
        UNION ALL
        {%- endif -%}
    {%- endfor %}
),

add_tx_index AS (
    SELECT
        c.*
        , t.index AS tx_index
        , t."from" AS reinvest_by_address
    FROM combined c
    INNER JOIN
    {{ source(blockchain, 'transactions') }} t
        ON t.hash = c.tx_hash
        AND t.block_number = c.block_number
        AND t.block_date = c.block_date
),

add_ratio_growth AS (
    SELECT
        *
        , ratio / LAG(ratio{% if is_incremental() %}, 1, latest_ratio{% endif %}) OVER (PARTITION BY contract_address ORDER BY block_number, tx_index, evt_index) AS ratio_growth
        , DATE_DIFF('millisecond', LAG(block_time{% if is_incremental() %}, 1, latest_block_time{% endif %}) OVER (PARTITION BY contract_address ORDER BY block_number, tx_index, evt_index), block_time) AS time_between_reinvests
    FROM add_tx_index
),

add_reinvests_array AS (
    SELECT
        *
        {%- if is_incremental() %}
        , concat(COALESCE(latest_recent_reinvest_info, ARRAY[]), ARRAY_AGG(json_object('ratio_growth': ratio_growth, 'time_between_reinvests': time_between_reinvests)) OVER (PARTITION BY contract_address ORDER BY block_number, tx_index, evt_index ROWS BETWEEN 25 PRECEDING AND CURRENT ROW)) AS recent_reinvest_info
        {%- else %}
        , ARRAY_AGG(json_object('ratio_growth': ratio_growth, 'time_between_reinvests': time_between_reinvests)) OVER (PARTITION BY contract_address ORDER BY block_number, tx_index, evt_index ROWS BETWEEN 25 PRECEDING AND CURRENT ROW) AS recent_reinvest_info
        {%- endif %}
    FROM add_ratio_growth
),

slice_reinvests_to_size AS (
    SELECT
        *
        -- This line ensures the recent reinvest info is at most of length 25
        , slice(recent_reinvest_info, greatest(1, cardinality(recent_reinvest_info) - 25 + 1), 25) AS sliced_recent_reinvest_info
        , filter(slice(recent_reinvest_info, greatest(1, cardinality(recent_reinvest_info) - 25 + 1), 25), x -> json_extract_scalar(x, '$.ratio_growth') IS NOT NULL) AS filtered_recent_reinvest_info
    FROM add_reinvests_array
)

SELECT
    '{{ blockchain }}' AS blockchain
    , contract_address
    , tx_hash
    , evt_index
    , tx_index
    , block_time
    , block_date
    , block_number
    , reinvest_by_address
    , new_total_deposits
    , new_total_supply
    -- The below gets the APY from an array of recent reinvest-related data.
    -- The calculation uses the growth in the ratio between the totalDeposits and totalSupply for the strategy (at inception these are 1:1).
    -- It uses the most recent 25 reinvests and weights them as powers of 2, so (sum of 2^i * value / sum of 2^i). This means a recent reinvest
    -- counts twice as strongly as the reinvest before that when it comes to calculating the APY.
    -- We've done it like this because custom user functions cannot be declared and when defining it as an in-line function we got an error message
    -- as part of the PR saying that inline functions were not allowed in views.
    , CASE WHEN cardinality(filtered_recent_reinvest_info) > 0 THEN
        CAST(
            reduce(
                filtered_recent_reinvest_info,
                json_object('n': 1, 'weighted_ratio_growth': 0, 'weighted_time_between_reinvests': 0, 'scaling_factor': 0),
                (s, x) -> json_object(
                    'n': CAST(json_extract_scalar(s, '$.n') AS integer) + 1,
                    'weighted_ratio_growth': CAST(json_extract_scalar(s, '$.weighted_ratio_growth') AS double) + POWER(2, CAST(json_extract_scalar(s, '$.n') AS double)) * CAST(json_extract_scalar(x, '$.ratio_growth') AS double),
                    'weighted_time_between_reinvests': CAST(json_extract_scalar(s, '$.weighted_time_between_reinvests') AS double) + POWER(2, CAST(json_extract_scalar(s, '$.n') AS double)) * CAST(json_extract_scalar(x, '$.time_between_reinvests') AS double),
                    'scaling_factor': CAST(json_extract_scalar(s, '$.scaling_factor') AS double) + POWER(2, CAST(json_extract_scalar(s, '$.n') AS double))
                ),
                s -> least(
                    POWER(
                        CAST(json_extract_scalar(s, '$.weighted_ratio_growth') AS double) / CAST(json_extract_scalar(s, '$.scaling_factor') AS double),
                        31536000000 / (CAST(json_extract_scalar(s, '$.weighted_time_between_reinvests') AS double) / CAST(json_extract_scalar(s, '$.scaling_factor') AS double))
                    ) - 1,
                    999999999 -- Sort of setting the max as 1 billion % by doing this
                )
            ) AS decimal(18, 6)
        )
        ELSE NULL END AS apy
    , ratio
    -- We need this for future incremental model additions
    , sliced_recent_reinvest_info AS recent_reinvest_info
FROM slice_reinvests_to_size

{%- endmacro -%}