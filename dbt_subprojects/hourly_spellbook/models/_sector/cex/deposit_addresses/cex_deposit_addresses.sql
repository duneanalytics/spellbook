{{ config(

        schema = 'cex',
        alias ='deposit_addresses',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['address'],
        merge_update_columns = ['cex_name', 'blockchains', 'first_used_blockchain', 'creation_block_number', 'funded_by_same_cex', 'first_funded_by', 'is_smart_contract'],
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

SELECT MIN_BY(blockchain, funded_block_time) AS blockchain
, address
, MIN(cex_name) AS cex_name
, MIN_BY(token_standard, funded_block_time) AS token_standard
, MIN(consolidation_block_time) AS consolidation_block_time
, MIN(consolidation_block_number) AS consolidation_block_number
, MIN(funded_block_time) AS funded_block_time
, MIN(funded_block_number) AS funded_block_number
, MIN_BY(first_funded_by, funded_block_time) AS first_funded_by
, MIN_BY(self_executed, funded_block_time) AS self_executed
, MIN_BY(tx_hash, funded_block_time) AS tx_hash
FROM (
    {% for cex_model in cex_models %}
    SELECT blockchain
    , address
    , cex_name
    , token_standard
    , consolidation_block_time
    , consolidation_block_number
    , funded_block_time
    , funded_block_number
    , first_funded_by
    , self_executed
    , tx_hash
    FROM {{ cex_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
    )
GROUP BY address
HAVING COUNT(DISTINCT cex_name) = 1


{% else %}



SELECT MIN_BY(blockchain, funded_block_time) AS blockchain
, address
, MIN(cex_name) AS cex_name
, MIN_BY(token_standard, funded_block_time) AS token_standard
, MIN(consolidation_block_time) AS consolidation_block_time
, MIN(consolidation_block_number) AS consolidation_block_number
, MIN(funded_block_time) AS funded_block_time
, MIN(funded_block_number) AS funded_block_number
, MIN_BY(first_funded_by, funded_block_time) AS first_funded_by
, MIN_BY(self_executed, funded_block_time) AS self_executed
, MIN_BY(tx_hash, funded_block_time) AS tx_hash
FROM (
    {% for cex_model in cex_models %}
    SELECT cm.blockchain
    , cm.address
    , cm.cex_name
    , cm.token_standard
    , cm.consolidation_block_time
    , cm.consolidation_block_number
    , cm.funded_block_time
    , cm.funded_block_number
    , cm.first_funded_by
    , cm.self_executed
    , cm.tx_hash
    FROM {{ cex_model }} cm
    LEFT JOIN {{this}} t ON cm.address=t.address
        AND cm.cex_name IS NULL
    {% if is_incremental() %}
    WHERE {{incremental_predicate('cm.consolidation_block_number')}}
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
    )
GROUP BY address
HAVING COUNT(DISTINCT cex_name) = 1

{% endif %}