{{ config(

        schema = 'cex',
        alias ='deposit_addresses',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['address'],
        merge_update_columns = ['blockchain', 'cex_name', 'token_standard', 'token_address', 'amount_consolidated', 'consolidation_first_block_time', 'consolidation_last_block_time', 'consolidation_count', 'amount_deposited', 'deposit_first_block_time', 'deposit_last_block_time', 'deposit_count'],
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


SELECT MIN_BY(blockchain, consolidation_first_block_time) AS blockchain
, address
, MIN(cex_name) AS cex_name
, MIN_BY(token_standard, consolidation_first_block_time) AS token_standard
, MIN_BY(token_address, consolidation_first_block_time) AS token_address
, MIN(amount_consolidated) AS amount_consolidated
, MIN(consolidation_first_block_time) AS consolidation_first_block_time
, MAX(consolidation_last_block_time) AS consolidation_last_block_time
, SUM(consolidation_count) AS consolidation_count
, MIN(amount_deposited) AS amount_deposited
, MIN(deposit_first_block_time) AS deposit_first_block_time
, MAX(deposit_last_block_time) AS deposit_last_block_time
, SUM(deposit_count) AS deposit_count
FROM (
    {% for cex_model in cex_models %}
    SELECT blockchain
    , address
    , cex_name
    , token_standard
    , token_address
    , amount_consolidated
    , consolidation_first_block_time
    , consolidation_last_block_time
    , consolidation_count
    , amount_deposited
    , deposit_first_block_time
    , deposit_last_block_time
    , deposit_count
    FROM {{ cex_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
    )
GROUP BY address
HAVING COUNT(DISTINCT cex_name) = 1


{% else %}


SELECT MIN_BY(blockchain, consolidation_first_block_time) AS blockchain
, address
, MIN(cex_name) AS cex_name
, MIN_BY(token_standard, consolidation_first_block_time) AS token_standard
, MIN_BY(token_address, consolidation_first_block_time) AS token_address
, MIN(amount_consolidated) AS amount_consolidated
, MIN(consolidation_first_block_time) AS consolidation_first_block_time
, MAX(consolidation_last_block_time) AS consolidation_last_block_time
, SUM(consolidation_count) AS consolidation_count
, MIN(amount_deposited) AS amount_deposited
, MIN(deposit_first_block_time) AS deposit_first_block_time
, MAX(deposit_last_block_time) AS deposit_last_block_time
, SUM(deposit_count) AS deposit_count
FROM (
    {% for cex_model in cex_models %}
    SELECT cm.blockchain
    , cm.address
    , cm.cex_name
    , cm.token_standard
    , cm.token_address
    , cm.amount_consolidated
    , cm.consolidation_first_block_time
    , cm.consolidation_last_block_time
    , cm.consolidation_count
    , cm.amount_deposited
    , cm.deposit_first_block_time
    , cm.deposit_last_block_time
    , cm.deposit_count
    FROM {{ cex_model }} cm
    LEFT JOIN {{this}} t ON cm.address=t.address
        AND cm.cex_name IS NULL
    {% if is_incremental() %}
    WHERE {{incremental_predicate('cm.consolidation_first_block_time')}}
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
    )
GROUP BY address
HAVING COUNT(DISTINCT cex_name) = 1


{% endif %}