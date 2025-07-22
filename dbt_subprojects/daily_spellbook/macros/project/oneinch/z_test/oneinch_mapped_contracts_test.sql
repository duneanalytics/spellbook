-- this macro helps to optimize the etl in case of adding new contract to a certain chain so it triggers pipeline only for this chain
{% macro oneinch_mapped_contracts_macro_test(blockchain, partition_number) %}



{% set 
    config = [
          ('0x6352a56caadc4f1e25cd6c75970fa768a3304e64', 'true', 'OpenOcean'      , '1'      , 'ExchangeV2'              , ['ethereum','bnb','polygon','avalanche_c','optimism','fantom','base','arbitrum','gnosis','linea'])
        
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
        ], project) or position('bridge' in lower(concat(project, tag))) > 0 as cross_chain
        , tag
        , partition_number
    from (values
        {% for row in config if blockchain in row[5] %}
            {% if not loop.first %}, {% endif %}({{ row[0] }}, {{ row[1] }}, '{{ row[2] }}', '{{ row[3] }}', '{{ row[4] }}')
        {% endfor %}
    ) as c(address, user, project, partition_number, tag)
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
    , partition_number
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