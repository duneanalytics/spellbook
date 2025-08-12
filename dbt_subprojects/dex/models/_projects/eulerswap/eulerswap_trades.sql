{{ config(
    schema = 'eulerswap'
    , alias = 'trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'block_month']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set base_models = [
    ref('eulerswap_ethereum_trades')
    , ref('eulerswap_bnb_trades')
    , ref('eulerswap_unichain_trades')
] %}

select * from  (
    {% for base_model in base_models %}
    SELECT 
        blockchain
        , project
        , version
        , block_month
        , block_date
        , block_time
        , block_number
        , token_bought_symbol
        , token_sold_symbol
        , token_pair
        , token_bought_amount
        , token_sold_amount
        , token_bought_amount_raw
        , token_sold_amount_raw
        , amount_usd
        , token_bought_address
        , token_sold_address
        , taker
        , maker
        , project_contract_address
        , tx_hash
        , tx_from
        , tx_to
        , case 
            when source = 'uni-v4' then evt_index - 1
            else evt_index 
        end as evt_index
        , pool_creation_time
        , fee 
        , protocolFee 
        , instance 
        , eulerAccount 
        , factory_address 
        , sender 
        , source 
    FROM
    {{ base_model }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)