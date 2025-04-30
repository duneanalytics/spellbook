{{ config(
        schema='evms',
        alias = 'transactions',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'[
                                        "abstract"
                                        , "apechain"
                                        , "arbitrum"
                                        , "avalanche_c"
                                        , "b3"
                                        , "base"
                                        , "berachain"
                                        , "blast"
                                        , "bnb"
                                        , "bob"
                                        , "boba"
                                        , "celo"
                                        , "corn"
                                        , "degen"
                                        , "ethereum"
                                        , "fantom"
                                        , "flare"
                                        , "gnosis"
                                        , "ink"
                                        , "kaia"
                                        , "lens"
                                        , "linea"
                                        , "mantle"
                                        , "nova"
                                        , "opbnb"
                                        , "optimism"
                                        , "polygon"
                                        , "ronin"
                                        , "scroll"
                                        , "sei"
                                        , "shape"
                                        , "sonic"
                                        , "sophon"
                                        , "unichain"
                                        , "viction"
                                        , "worldchain"
                                        , "zkevm"
                                        , "zksync"
                                        , "zora"
                                        ]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby", "synthquest", "allelosi"]\') }}'
        )
}}

-- include non-L2s in models, since we want to control for L1 Gas Used
{% set transactions_models = [
     ('abstract', source('abstract', 'transactions'))
     , ('apechain', source('apechain', 'transactions'))
     , ('arbitrum', source('arbitrum', 'transactions'))
     , ('avalanche_c', source('avalanche_c', 'transactions'))
     , ('b3', source('b3', 'transactions'))
     , ('base', source('base', 'transactions'))
     , ('berachain', source('berachain', 'transactions'))
     , ('blast', source('blast', 'transactions'))
     , ('bnb', source('bnb', 'transactions'))
     , ('bob', source('bob', 'transactions'))
     , ('boba', source('boba', 'transactions'))
     , ('celo', source('celo', 'transactions'))
     , ('corn', source('corn', 'transactions'))
     , ('degen', source('degen', 'transactions'))
     , ('ethereum', source('ethereum', 'transactions'))
     , ('fantom', source('fantom', 'transactions'))
     , ('flare', source('flare', 'transactions'))
     , ('gnosis', source('gnosis', 'transactions'))
     , ('ink', source('ink', 'transactions'))
     , ('kaia', source('kaia', 'transactions'))
     , ('lens', source('lens', 'transactions'))
     , ('linea', source('linea', 'transactions'))
     , ('mantle', source('mantle', 'transactions'))
     , ('nova', source('nova', 'transactions'))
     , ('opbnb', source('opbnb', 'transactions'))
     , ('optimism', source('optimism', 'transactions'))
     , ('polygon', source('polygon', 'transactions'))
     , ('ronin', source('ronin', 'transactions'))
     , ('scroll', source('scroll', 'transactions'))
     , ('sei', source('sei', 'transactions'))
     , ('shape', source('shape', 'transactions'))
     , ('sonic', source('sonic', 'transactions'))
     , ('sophon', source('sophon', 'transactions'))
     , ('unichain', source('unichain', 'transactions'))
     , ('viction', source('viction', 'transactions'))
     , ('worldchain', source('worldchain', 'transactions'))
     , ('zkevm', source('zkevm', 'transactions'))
     , ('zksync', source('zksync', 'transactions'))
     , ('zora', source('zora', 'transactions'))
] %}

{% set unstructured_transactions_models = [
     ('mode', source('mode', 'transactions'))
] %}

SELECT *
FROM (
        {% for transactions_model in transactions_models %}
        SELECT
        '{{ transactions_model[0] }}' AS blockchain
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
        --Logic for L2s
                {% if transactions_model[0] in all_op_chains() + ('scroll','mantle','blast') %}
                , l1_tx_origin
                , l1_fee_scalar
                , l1_block_number
                , l1_fee
                , l1_gas_price
                , l1_gas_used
                , l1_timestamp
                , CAST(NULL AS DECIMAL(38,0)) AS effective_gas_price

                {% elif transactions_model[0] == 'arbitrum' %}
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
        FROM {{ transactions_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}

        UNION ALL

        {% for unstructured_transactions_models in unstructured_transactions_models %}
        {% if unstructured_transactions_models[0] == 'mode' %}
        SELECT
        '{{ unstructured_transactions_models[0] }}' AS blockchain
        , CAST(NULL AS array(row(address varbinary, storagekeys array(varbinary)))) AS access_list
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
        , l1_tx_origin
        , l1_fee_scalar
        , l1_block_number
        , l1_fee
        , l1_gas_price
        , l1_gas_used
        , l1_timestamp
        , CAST(NULL AS DECIMAL(38,0)) AS effective_gas_price
        {% endif %}
        FROM {{ unstructured_transactions_models[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}

        );
