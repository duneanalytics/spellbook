{{ config(
        schema='evms',
        alias = 'transactions',
        materialized = 'view',
        post_hook='{{ expose_spells(evms_structured_blockchains_list() | tojson, "sector", "evms", \'[]\') }}'
        )
}}

-- include non-L2s in models, since we want to control for L1 Gas Used
{% set structured_blockchains = evms_structured_blockchains_list() %}

SELECT *
FROM (
        {% for blockchain in structured_blockchains %}
        SELECT
        '{{ blockchain }}' AS blockchain
        , access_list
        , block_hash
        , data
        , "from"
        , hash
        , to
        , block_number
        , block_time
        , gas_limit
        , CAST(gas_price AS double) AS gas_price
        , gas_used
        , index
        , max_fee_per_gas
        , max_priority_fee_per_gas
        , nonce
        , priority_fee_per_gas
        , success
        , "type"
        , CAST(value AS double) AS value
        {% if blockchain in ('ethereum', 'arbitrum', 'base', 'optimism', 'polygon', 'zkevm', 'bnb', 'gnosis', 'scroll', 'zora', 'mantle', 'berachain', 'unichain', 'worldchain', 'ink', 'nova', 'opbnb') %}
        , authorization_list
        {% else %}
        , CAST(NULL AS array(row(chainid bigint, address varbinary, nonce bigint, r varchar, s varchar, yparity varchar))) AS authorization_list
        {% endif %}
        
        --Logic for L2s
                {% if blockchain in all_op_chains() + ('scroll','mantle','blast') %}
                , l1_tx_origin
                , l1_fee_scalar
                , l1_block_number
                , l1_fee
                , l1_gas_price
                , l1_gas_used
                , l1_timestamp
                , CAST(NULL AS DECIMAL(38,0)) AS effective_gas_price

                {% elif blockchain == 'arbitrum' %}
                , cast(NULL as varbinary) AS l1_tx_origin
                , CAST(NULL AS double) AS l1_fee_scalar
                , CAST(NULL AS DECIMAL(38,0)) AS l1_block_number
                , CAST(NULL AS DECIMAL(38,0)) AS l1_fee
                , CAST(NULL AS DECIMAL(38,0)) AS l1_gas_price
                , gas_used_for_l1 AS l1_gas_used
                , cast(NULL as bigint) AS l1_timestamp
                , effective_gas_price

                {% else %}
                , cast(NULL as varbinary) AS l1_tx_origin
                , CAST(NULL AS double) AS l1_fee_scalar
                , CAST(NULL AS DECIMAL(38,0)) AS l1_block_number
                , CAST(NULL AS DECIMAL(38,0)) AS l1_fee
                , CAST(NULL AS DECIMAL(38,0)) AS l1_gas_price
                , CAST(NULL AS DECIMAL(38,0)) AS l1_gas_used
                , cast(NULL as bigint) AS l1_timestamp
                , CAST(NULL AS DECIMAL(38,0)) AS effective_gas_price
                {% endif %}
        FROM {{ source(blockchain, 'transactions') }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}

        );
