-- this macro helps to optimize the etl in case of adding new contract to a certain chain so it triggers pipeline only for this chain
{% macro oneinch_mapped_contracts_macro_test(blockchain, partition_number) %}



{% set 
    config = [
        ('0x5c7BCd6E7De5423a257D81B442095A1a6ced35C5', 'true', 'Across'      , '1'         , 'SpokePool'               , ['ethereum'])
        , ('0xe35e9842fceaCA96570B734083f4a58e8F7C5f2A', 'true', 'Across'    , '1'           , 'SpokePool'               , ['arbitrum'])
        , ('0x7E63A5f1a8F0B4d0934B2f2327DAED3F6bb2ee75', 'true', 'Across'    , '1'           , 'SpokePool'               , ['linea'])
        , ('0x6f26Bf09B1C792e3228e5467807a900A503c0281', 'true', 'Across'    , '1'           , 'SpokePool'               , ['optimism'])
        , ('0x9295ee1d8C5b022Be115A2AD3c30C72E34e7F096', 'true', 'Across'    , '1'           , 'SpokePool'               , ['polygon'])
        , ('0x09aea4b2242abC8bb4BB78D537A67a245A7bEC64', 'true', 'Across'    , '1'           , 'SpokePool'               , ['base'])
        , ('0xE0B015E54d54fc84a6cB9B666099c46adE9335FF', 'true', 'Across'    , '1'           , 'SpokePool'               , ['zksync'])
        , ('0x3bad7ad0728f9917d1bf08af5782dcbd516cdd96', 'true', 'Across'    , '1'           , 'SpokePool'               , ['scroll'])
        
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