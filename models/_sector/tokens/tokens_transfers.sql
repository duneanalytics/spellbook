{{config(
    schema = 'tokens',
    alias = 'transfers',
    partition_by = ['token_standard', 'block_date', 'blockchain'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['unique_key', 'blockchain'],
    post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "bnb", "celo", "ethereum", "fantom", "gnosis", "optimism", "polygon", "zksync", "zora"]\',
                                "sector",
                                "tokens",
                                \'["aalan3", "jeff-dude"]\') }}'
)
}}

{% 
    set blockchains = [
        'arbitrum'
        ,'avalanche_c'
        ,'base'
        ,'bnb'
        ,'celo'
        ,'ethereum'
        ,'fantom'
        ,'gnosis'
        ,'optimism'
        ,'polygon'
        ,'zksync'
        ,'zora'
    ]
%}

with base_union as (
    SELECT *
    FROM
    (
        {% for blockchain in blockchains %}
        SELECT
            unique_key
            , blockchain
            , block_date
            , block_time
            , block_number
            , tx_hash
            , tx_index
            , evt_index
            , trace_address
            , token_standard
            , tx_from
            , tx_to
            , "from"
            , "to"
            , contract_address
            , symbol
            , amount_raw
            , amount
            , usd_price
            , usd_amount
        FROM
            {{ ref('tokens_' + blockchain + '_transfers') }}
        {% if is_incremental() %}
        WHERE
            {{ incremental_predicate('block_time') }}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
)
select
    *
from
    base_union
