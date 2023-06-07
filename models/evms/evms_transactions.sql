{{ config(
        alias ='transactions',
        materialized = 'incremental',
        file_format = 'delta',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}'
        )
}}

{% set transactions_models = [
     ('ethereum', source('ethereum', 'transactions'))
     , ('polygon', source('polygon', 'transactions'))
     , ('bnb', source('bnb', 'transactions'))
     , ('avalanche_c', source('avalanche_c', 'transactions'))
     , ('gnosis', source('gnosis', 'transactions'))
     , ('fantom', source('fantom', 'transactions'))
     , ('arbitrum', source('arbitrum', 'transactions'))
] %}

SELECT *
FROM (
        {% for transactions_model in transactions_models %}
        SELECT
        '{{ transactions_model[0] }}' AS blockchain
        , access_list
        , block_hash
        , data
        , `from`
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
        , `type`
        , CAST(value AS double) AS value
        , NULL AS l1_tx_origin
        , CASE WHEN '{{ transactions_model[0] }}' = 'optimsm' THEN l1_fee_scalar
                ELSE CAST(NULL AS double)
                END AS l1_fee_scalar
        , CAST(NULL AS DECIMAL(38,0)) AS l1_block_number
        , CAST(NULL AS DECIMAL(38,0)) AS l1_fee
        , CAST(NULL AS DECIMAL(38,0)) AS l1_gas_price
        , CAST(NULL AS DECIMAL(38,0)) AS l1_gas_used
        , NULL AS l1_timestamp
        , CAST(NULL AS DECIMAL(38,0)) AS effective_gas_price
        FROM {{ transactions_model[1] }}
        {% if not loop.last %}
        {% if is_incremental() %}
        WHERE block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        UNION ALL
        {% endif %}
        {% endfor %}

        UNION ALL

        SELECT 'optimism' AS blockchain
        , access_list
        , block_hash
        , data
        , `from`
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
        , `type`
        , CAST(value AS double) AS value
        ,l1_tx_origin
        , l1_fee_scalar
        , l1_block_number
        , l1_fee
        , l1_gas_price
        , l1_gas_used
        , l1_timestamp
        , CAST(NULL AS DECIMAL(38,0)) AS effective_gas_price
        FROM {{ source('optimism', 'transactions') }}
        {% if is_incremental() %}
        WHERE block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}

        UNION ALL

        SELECT 'arbitrum' AS blockchain
        , access_list
        , block_hash
        , data
        , `from`
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
        , `type`
        , CAST(value AS double) AS value
        , NULL AS l1_tx_origin
        , CAST(NULL AS double) AS l1_fee_scalar
        , CAST(NULL AS DECIMAL(38,0)) AS l1_fee_scalar
        , CAST(NULL AS DECIMAL(38,0)) AS l1_fee
        , CAST(NULL AS DECIMAL(38,0)) AS l1_gas_price
        , gas_used_for_l1 AS l1_gas_used
        , NULL AS l1_timestamp
        , effective_gas_price
        FROM {{ source('arbitrum', 'transactions') }}
        {% if is_incremental() %}
        WHERE block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        );


UNION ALL

SELECT 'ethereum' AS blockchain
, access_list
, block_hash
, data
, `from`
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
, `type`
, CAST(value AS double) AS value
, NULL AS l1_tx_origin
, CAST(NULL AS double) AS l1_fee_scalar
, CAST(NULL AS DECIMAL(38,0)) AS l1_block_number
, CAST(NULL AS DECIMAL(38,0)) AS l1_fee
, CAST(NULL AS DECIMAL(38,0)) AS l1_gas_price
, CAST(NULL AS DECIMAL(38,0)) AS l1_gas_used
, NULL AS l1_timestamp
, CAST(NULL AS DECIMAL(38,0)) AS effective_gas_price
FROM {{source('ethereum', 'transactions')}}

UNION ALL

SELECT 'polygon' AS blockchain
, access_list
, block_hash
, data
, `from`
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
, `type`
, CAST(value AS double) AS value
, NULL AS l1_tx_origin
, CAST(NULL AS double) AS l1_fee_scalar
, CAST(NULL AS DECIMAL(38,0)) AS l1_fee_scalar
, CAST(NULL AS DECIMAL(38,0)) AS l1_fee
, CAST(NULL AS DECIMAL(38,0)) AS l1_gas_price
, CAST(NULL AS DECIMAL(38,0)) AS l1_gas_used
, NULL AS l1_timestamp
, CAST(NULL AS DECIMAL(38,0)) AS effective_gas_price
FROM {{ source('polygon', 'transactions') }}

UNION ALL


SELECT 'bnb' AS blockchain
, access_list
, block_hash
, data
, `from`
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
, `type`
, CAST(value AS double) AS value
, NULL AS l1_tx_origin
, CAST(NULL AS double) AS l1_fee_scalar
, CAST(NULL AS DECIMAL(38,0)) AS l1_fee_scalar
, CAST(NULL AS DECIMAL(38,0)) AS l1_fee
, CAST(NULL AS DECIMAL(38,0)) AS l1_gas_price
, CAST(NULL AS DECIMAL(38,0)) AS l1_gas_used
, NULL AS l1_timestamp
, CAST(NULL AS DECIMAL(38,0)) AS effective_gas_price
FROM {{ source('bnb', 'transactions') }}

UNION ALL

SELECT 'avalanche_c' AS blockchain
, access_list
, block_hash
, data
, `from`
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
, `type`
, CAST(value AS double) AS value
, NULL AS l1_tx_origin
, CAST(NULL AS double) AS l1_fee_scalar
, CAST(NULL AS DECIMAL(38,0)) AS l1_fee_scalar
, CAST(NULL AS DECIMAL(38,0)) AS l1_fee
, CAST(NULL AS DECIMAL(38,0)) AS l1_gas_price
, CAST(NULL AS DECIMAL(38,0)) AS l1_gas_used
, NULL AS l1_timestamp
, CAST(NULL AS DECIMAL(38,0)) AS effective_gas_price
FROM {{ source('avalanche_c', 'transactions') }}

UNION ALL

SELECT 'gnosis' AS blockchain
, access_list
, block_hash
, data
, `from`
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
, `type`
, CAST(value AS double) AS value
, NULL AS l1_tx_origin
, CAST(NULL AS double) AS l1_fee_scalar
, CAST(NULL AS DECIMAL(38,0)) AS l1_fee_scalar
, CAST(NULL AS DECIMAL(38,0)) AS l1_fee
, CAST(NULL AS DECIMAL(38,0)) AS l1_gas_price
, CAST(NULL AS DECIMAL(38,0)) AS l1_gas_used
, NULL AS l1_timestamp
, CAST(NULL AS DECIMAL(38,0)) AS effective_gas_price
FROM {{ source('gnosis', 'transactions') }}

UNION ALL

SELECT 'fantom' AS blockchain
, access_list
, block_hash
, data
, `from`
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
, `type`
, CAST(value AS double) AS value
, NULL AS l1_tx_origin
, CAST(NULL AS double) AS l1_fee_scalar
, CAST(NULL AS DECIMAL(38,0)) AS l1_fee_scalar
, CAST(NULL AS DECIMAL(38,0)) AS l1_fee
, CAST(NULL AS DECIMAL(38,0)) AS l1_gas_price
, CAST(NULL AS DECIMAL(38,0)) AS l1_gas_used
, NULL AS l1_timestamp
, CAST(NULL AS DECIMAL(38,0)) AS effective_gas_price
FROM {{ source('fantom', 'transactions') }}

UNION ALL

SELECT 'arbitrum' AS blockchain
, access_list
, block_hash
, data
, `from`
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
, `type`
, CAST(value AS double) AS value
, NULL AS l1_tx_origin
, CAST(NULL AS double) AS l1_fee_scalar
, CAST(NULL AS DECIMAL(38,0)) AS l1_fee_scalar
, CAST(NULL AS DECIMAL(38,0)) AS l1_fee
, CAST(NULL AS DECIMAL(38,0)) AS l1_gas_price
, gas_used_for_l1 AS l1_gas_used
, NULL AS l1_timestamp
, effective_gas_price
FROM {{ source('arbitrum', 'transactions') }}