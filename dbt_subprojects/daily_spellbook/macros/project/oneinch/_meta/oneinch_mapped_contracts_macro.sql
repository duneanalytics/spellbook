-- this macro helps to optimize the etl in case of adding new contract to a certain chain so it triggers pipeline only for this chain
{% macro oneinch_mapped_contracts_macro(blockchain) %}



{% set 
    config = [
    ('0xdeb460658269d99c7aef30c52736df55ad109f42', 'false', 'Swaps.io'             , 'Swaps.io'                 , ['optimism'])
    , ('0xc0a70e04bd48d3717bfbce1d62786a3dd1d86162', 'false', 'Swaps.io'             , 'Swaps.io'                 , ['base'])
    , ('0x9f02a311e5fd06084c224a30e363c8cdb027d68f', 'false', 'Swaps.io'             , 'Swaps.io'                 , ['arbitrum'])
    ]
%}



with 
    
contracts as (
    select distinct
        '{{blockchain}}' as blockchain
        , address
        , project
        , user
        , contains(array[
              '1inch'
            , 'BabySwap'
            , 'BoggedFinance'
            , 'Dzap'
            , 'Firebird'
            , 'Kyber'
            , 'Odos'
            , 'OpenOcean'
            , 'Paraswap'
            , 'SlingshotFinance'
            , 'TransitSwap'
            , 'ZeroEx'
            , 'LiFi'
        ], project) as multi
        , contains(array[
              'Across'
            , 'CrossCurve'
            , 'Stargate'
            , 'Orbiter'
            , 'LiFi'
            , 'Swaps.io'
        ], project) or position('bridge' in lower(concat(project, tag))) > 0 as cross_chain
        , tag
    from (values
        {% for row in config if blockchain in row[4] %}
            {% if not loop.first %}, {% endif %}({{ row[0] }}, {{ row[1] }}, '{{ row[2] }}', '{{ row[3] }}')
        {% endfor %}
    ) as c(address, user, project, tag)
)

, creations as (
    select
        '{{blockchain}}' as blockchain
        , address
        , min(block_time) as first_created_at
        , max(block_time) as last_created_at
        , max("from") as last_creator
        , max(tx_hash) as last_creation_tx_hash
    from {{ source(blockchain, 'creation_traces') }}
    group by 1, 2
)

-- output --

select
    blockchain
    , address
    , project
    , tag
    , map_from_entries(array[
            ('user', user)
            , ('multi', multi)
            , ('recreated', first_created_at <> last_created_at)
            , ('cross_chain', cross_chain) -- a project/contract that implements a cross-chain swap protocol
    ]) as flags
    , first_created_at
    , last_created_at
    , last_creator
    , last_creation_tx_hash
from contracts
join creations using(blockchain, address)
order by project, blockchain, last_created_at, tag, address

{% endmacro %}