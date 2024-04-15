{%- macro yield_yak_deposits_withdraws(
        blockchain = null,
        event_name = 'Deposit'
    )
-%}

{%- set namespace_blockchain = 'yield_yak_' + blockchain -%}

WITH

{% if is_incremental() -%}
existing_contracts AS (
    SELECT
        t.contract_address
        , MAX(t.block_number) AS latest_block_number
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
            , t.index AS tx_index
            , s.evt_block_time AS block_time
            , s.evt_block_number AS block_number
            , s.account AS user_address
            , s.amount AS {{ event_name.lower() }}_amount
        FROM {{ source(namespace_blockchain, strategy + '_evt_' + event_name) }} s
        LEFT JOIN
        {{ source(blockchain, 'transactions') }} t
            ON t.hash = s.evt_tx_hash
        {% if is_incremental() -%}
        LEFT JOIN
        existing_contracts c
            ON c.contract_address = s.contract_address
        WHERE
            ({{ incremental_predicate('s.evt_block_time') }}
            AND s.evt_block_number > c.latest_block_number)
            OR c.contract_address IS NULL -- This line allows for new contract_addresses being appended that were not already included in previous runs but also allows their entire historical data to be loaded
        {%- endif %}
        {%- if not loop.last -%}
        UNION ALL
        {%- endif -%}
    {%- endfor %}
)

SELECT
    '{{ blockchain }}' AS blockchain
    , *
FROM combined

{%- endmacro -%}