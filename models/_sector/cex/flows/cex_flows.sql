{{ config(
        
        schema='cex',
        alias = 'flows',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'flow_type', 'unique_key'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        post_hook='{{ expose_spells(blockchains = \'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "polygon", "base", "celo", "zora", "zksync", "scroll", "fantom", "linea", "zkevm"]\',
                                spell_type = "sector",
                                spell_name = "cex",
                                contributors = \'["hildobby"]\') }}'
        )
}}

{% set cex_models = [
     (ref('cex_arbitrum_flows'))
     , (ref('cex_avalanche_c_flows'))
     , (ref('cex_bnb_flows'))
     , (ref('cex_ethereum_flows'))
     , (ref('cex_fantom_flows'))
     , (ref('cex_gnosis_flows'))
     , (ref('cex_optimism_flows'))
     , (ref('cex_polygon_flows'))
     , (ref('cex_base_flows'))
     , (ref('cex_celo_flows'))
     , (ref('cex_zora_flows'))
     , (ref('cex_zksync_flows'))
     , (ref('cex_scroll_flows'))
     , (ref('cex_linea_flows'))
     , (ref('cex_zkevm_flows'))
] %}

SELECT *
FROM (
        {% for cex_model in cex_models %}
        SELECT blockchain
        , block_month
        , block_time
        , block_number
        , cex_name
        , distinct_name
        , token_address
        , token_symbol
        , token_standard
        , flow_type
        , amount
        , amount_raw
        , amount_usd
        , "from"
        , to
        , tx_from
        , tx_to
        , tx_index
        , tx_hash
        , evt_index
        , unique_key
        FROM {{ cex_model }}
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('block_time') }}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )