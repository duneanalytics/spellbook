{{ config(

        schema = 'cex',
        alias ='deposit_addresses',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.creation_block_time')],
        unique_key = ['address'],
        merge_update_columns = ['cex_name', 'blockchains', 'first_used_blockchain', 'creation_block_time', 'creation_block_number', 'funded_by_same_cex', 'first_funded_by', 'is_smart_contract'],
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "polygon", "base", "celo", "scroll", "zora"]\',
                                    "sector",
                                    "cex",
                                    \'["hildobby"]\') }}'
)
}}

{% set cex_models = [
    ref('cex_ethereum_deposit_addresses')
    , ref('cex_bnb_deposit_addresses')
    , ref('cex_avalanche_c_deposit_addresses')
    , ref('cex_gnosis_deposit_addresses')
    , ref('cex_optimism_deposit_addresses')
    , ref('cex_arbitrum_deposit_addresses')
    , ref('cex_polygon_deposit_addresses')
    , ref('cex_base_deposit_addresses')
    , ref('cex_celo_deposit_addresses')
    , ref('cex_scroll_deposit_addresses')
    , ref('cex_zora_deposit_addresses')
] %}


{% if not is_incremental() %}

SELECT address
, MAX(cex_name) AS cex_name
, array_agg(blockchain) AS blockchains
, MIN_BY(blockchain, creation_block_time) AS first_used_blockchain
, MIN(creation_block_time) AS creation_block_time
, MIN(creation_block_number) AS creation_block_number
, MIN_BY(funded_by_same_cex, creation_block_time) AS funded_by_same_cex
, MIN_BY(first_funded_by, creation_block_time) AS first_funded_by
, MIN_BY(is_smart_contract, creation_block_time) AS is_smart_contract
FROM (
    {% for cex_model in cex_models %}
    SELECT blockchain
    , address
    , cex_name
    , creation_block_time
    , creation_block_number
    , funded_by_same_cex
    , first_funded_by
    , is_smart_contract 
    FROM {{ cex_model }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('creation_block_time')}}
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
    )
GROUP BY address
HAVING COUNT(DISTINCT cex_name) = 1


{% else %}

WITH new AS (
    SELECT address
    , MAX(cex_name) AS cex_name
    , array_agg(blockchain) AS blockchains
    , MIN_BY(blockchain, creation_block_time) AS first_used_blockchain
    , MIN(creation_block_time) AS creation_block_time
    , MIN(creation_block_number) AS creation_block_number
    , MIN_BY(funded_by_same_cex, creation_block_time) AS funded_by_same_cex
    , MIN_BY(first_funded_by, creation_block_time) AS first_funded_by
    , MIN_BY(is_smart_contract, creation_block_time) AS is_smart_contract
    FROM (
        {% for cex_model in cex_models %}
        SELECT cm.blockchain
        , cm.address
        , cm.cex_name
        , cm.creation_block_time
        , cm.creation_block_number
        , cm.funded_by_same_cex
        , cm.first_funded_by
        , cm.is_smart_contract 
        FROM {{ cex_model }} cm
        LEFT JOIN {{this}} t ON cm.address=t.address
            AND contains(t.blockchains, cm.blockchain) = FALSE
        {% if is_incremental() %}
        WHERE {{incremental_predicate('cm.creation_block_time')}}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )
    GROUP BY address
    HAVING COUNT(DISTINCT cex_name) = 1
    )

SELECT n.address
, n.cex_name AS cex_name
, array_union(t.blockchains, n.blockchains) AS blockchains
, COALESCE(t.first_used_blockchain, n.first_used_blockchain) AS first_used_blockchain
, COALESCE(t.creation_block_time, n.creation_block_time) AS creation_block_time
, COALESCE(t.creation_block_number, n.creation_block_number) AS creation_block_number
, COALESCE(t.funded_by_same_cex, n.funded_by_same_cex) AS funded_by_same_cex
, COALESCE(t.first_funded_by, n.first_funded_by) AS first_funded_by
, COALESCE(t.is_smart_contract, n.is_smart_contract) AS is_smart_contract
FROM new n
LEFT JOIN {{this}} t ON t.address=n.address AND t.cex_name=n.cex_name

{% endif %}