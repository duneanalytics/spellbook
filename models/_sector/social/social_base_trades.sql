{{ config(
    schema = 'social'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain','tx_hash','evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set chain_specific_models = [
    ref('social_base_base_trades')
    , ref('social_avalanche_c_base_trades')
    , ref('social_arbitrum_base_trades')
    , ref('social_bnb_base_trades')
] %}


with base_union as (
    SELECT *
    FROM (
        {% for chain_specific_model in chain_specific_models %}
        SELECT 
            blockchain
            , block_time
            , block_number
            , project
            , trader
            , subject
            , tx_from
            , tx_to
            , trade_side
            , amount_original
            , share_amount
            , subject_fee_amount
            , protocol_fee_amount
            , currency_contract
            , currency_symbol
            , supply
            , tx_hash
            , evt_index
            , contract_address
            , row_number() over (partition by tx_hash, evt_index order by tx_hash) as duplicates_rank
        FROM {{ chain_specific_model }}
        {% if is_incremental() %}
        WHERE
            {{ incremental_predicate('block_time') }}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
    WHERE
        duplicates_rank = 1
)
select
    *
from
    base_union
