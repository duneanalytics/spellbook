{{ config(

        schema = 'cex',
        alias ='deposit_addresses',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['address'],
        merge_update_columns = ['blockchain', 'cex_name', 'first_deposit_token_standard', 'first_deposit_token_address', 'deposit_first_block_time', 'consolidation_first_block_time', 'deposit_count', 'consolidation_count', 'amount_deposited', 'consolidation_unique_key', 'deposit_unique_key'],
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


SELECT MIN_BY(blockchain, deposit_first_block_time) AS blockchain
, address
, MIN(cex_name) AS cex_name
, MIN_BY(first_deposit_token_standard, deposit_first_block_time) AS first_deposit_token_standard
, MIN_BY(first_deposit_token_address, deposit_first_block_time) AS first_deposit_token_address
, MIN(deposit_first_block_time) AS deposit_first_block_time
, MIN(consolidation_first_block_time) AS consolidation_first_block_time
, SUM(deposit_count) AS deposit_count
, SUM(consolidation_count) AS consolidation_count
, MIN(amount_deposited) AS amount_deposited
, MIN_BY(consolidation_unique_key, consolidation_first_block_time) AS consolidation_unique_key
, MIN_BY(deposit_unique_key, deposit_first_block_time) AS deposit_unique_key
FROM (
    {% for cex_model in cex_models %}
    SELECT blockchain
    , address
    , cex_name
    , first_deposit_token_standard
    , first_deposit_token_address
    , deposit_first_block_time
    , consolidation_first_block_time
    , deposit_count
    , consolidation_count
    , amount_deposited
    , consolidation_unique_key
    , deposit_unique_key
    FROM {{ cex_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
    )
GROUP BY address
HAVING COUNT(DISTINCT cex_name) = 1


{% else %}


SELECT MIN_BY(blockchain, deposit_first_block_time) AS blockchain
, address
, MIN(cex_name) AS cex_name
, MIN_BY(first_deposit_token_standard, deposit_first_block_time) AS first_deposit_token_standard
, MIN_BY(first_deposit_token_address, deposit_first_block_time) AS first_deposit_token_address
, MIN(deposit_first_block_time) AS deposit_first_block_time
, MIN(consolidation_first_block_time) AS consolidation_first_block_time
, SUM(deposit_count) AS deposit_count
, SUM(consolidation_count) AS consolidation_count
, MIN(amount_deposited) AS amount_deposited
, MIN_BY(consolidation_unique_key, consolidation_first_block_time) AS consolidation_unique_key
, MIN_BY(deposit_unique_key, deposit_first_block_time) AS deposit_unique_key
FROM (
    {% for cex_model in cex_models %}
    SELECT cm.blockchain
    , cm.address
    , cm.cex_name
    , cm.first_deposit_token_standard
    , cm.first_deposit_token_address
    , cm.deposit_first_block_time
    , cm.consolidation_first_block_time
    , cm.deposit_count
    , cm.consolidation_count
    , cm.amount_deposited
    , cm.consolidation_unique_key
    , cm.deposit_unique_key
    FROM {{ cex_model }} cm
    LEFT JOIN {{this}} t ON cm.address=t.address
        AND cm.cex_name IS NULL
    {% if is_incremental() %}
    WHERE {{incremental_predicate('cm.deposit_first_block_time')}}
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
    )
GROUP BY address
HAVING COUNT(DISTINCT cex_name) = 1


{% endif %}