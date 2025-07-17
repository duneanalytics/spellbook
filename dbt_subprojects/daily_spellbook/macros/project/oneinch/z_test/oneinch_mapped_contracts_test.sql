-- this macro helps to optimize the etl in case of adding new contract to a certain chain so it triggers pipeline only for this chain
{% macro oneinch_mapped_contracts_macro_test(blockchain, partition_number) %}



{% set 
    config = [
          ('0xc586bef4a0992c495cf22e1aeee4e446cecdee0e', 'true', '1inch'     , '1'            , 'OneSplit'                , ['ethereum'])
        , ('0xe4c577bdec9ce0f6c54f2f82aed5b1913b71ae2f', 'true', '1inch'     , '1'            , 'ExchangeV1'              , ['ethereum'])
        , ('0x0000000006adbd7c01bc0738cdbfc3932600ad63', 'true', '1inch'     , '1'            , 'ExchangeV2'              , ['ethereum'])
        , ('0x0000000053d411becdb4a82d8603edc6d8b8b3bc', 'true', '1inch'     , '1'            , 'ExchangeV3'              , ['ethereum'])
        , ('0x000005edbbc1f258302add96b5e20d3442e5dd89', 'true', '1inch'     , '1'            , 'ExchangeV4'              , ['ethereum'])
        , ('0x0000000f8ef4be2b7aed6724e893c1b674b9682d', 'true', '1inch'     , '1'            , 'ExchangeV5'              , ['ethereum'])
        , ('0x111112549cfedf7822eb11fbd8fd485d8a10f93f', 'true', '1inch'     , '1'            , 'ExchangeV6'              , ['ethereum'])
        , ('0x111111254b08ceeee8ad6ca827de9952d2a46781', 'true', '1inch'     , '1'            , 'ExchangeV7'              , ['ethereum'])
        , ('0x3ef51736315f52d568d6d2cf289419b9cfffe782', 'true', '1inch'     , '1'            , 'LimitOrderProtocolV1'    , ['ethereum'])
        , ('0xe3456f4ee65e745a44ec3bcb83d0f2529d1b84eb', 'true', '1inch'     , '1'            , 'LimitOrderProtocolV1'    , ['bnb'])
        , ('0xb707d89d29c189421163515c59e42147371d6857', 'true', '1inch'     , '1'            , 'LimitOrderProtocolV1'    , ['polygon','optimism'])
        , ('0xe295ad71242373c37c5fda7b57f26f9ea1088afe', 'true', '1inch'     , '1'            , 'LimitOrderProtocolV1'    , ['arbitrum'])
        , ('0x119c71d3bbac22029622cbaec24854d3d32d2828', 'true', '1inch'     , '1'            , 'LimitOrderProtocolV2'    , ['ethereum'])
        , ('0x1e38eff998df9d3669e32f4ff400031385bf6362', 'true', '1inch'     , '1'            , 'LimitOrderProtocolV2'    , ['bnb'])
        , ('0x94bc2a1c732bcad7343b25af48385fe76e08734f', 'true', '1inch'     , '1'            , 'LimitOrderProtocolV2'    , ['polygon'])
        , ('0x54431918cec22932fcf97e54769f4e00f646690f', 'true', '1inch'     , '1'            , 'LimitOrderProtocolV2'    , ['gnosis'])
        , ('0x7f069df72b7a39bce9806e3afaf579e54d8cf2b9', 'true', '1inch'     , '1'            , 'LimitOrderProtocolV2'    , ['arbitrum'])
        , ('0x0f85a912448279111694f4ba4f85dc641c54b594', 'true', '1inch'     , '1'            , 'LimitOrderProtocolV2'    , ['avalanche_c'])
        , ('0x11431a89893025d2a48dca4eddc396f8c8117187', 'true', '1inch'     , '1'            , 'LimitOrderProtocolV2'    , ['optimism'])
        , ('0x11dee30e710b8d4a8630392781cc3c0046365d4c', 'true', '1inch'     , '1'            , 'LimitOrderProtocolV2'    , ['fantom'])
        , ('0x11111254369792b2ca5d084ab5eea397ca8fa48b', 'true', '1inch'     , '1'            , 'AggregationRouterV1'     , ['ethereum'])
        , ('0x111111125434b319222cdbf8c261674adb56f3ae', 'true', '1inch'     , '1'            , 'AggregationRouterV2'     , ['ethereum'])
        , ('0x111111254bf8547e7183e4bbfc36199f3cedf4a1', 'true', '1inch'     , '1'            , 'AggregationRouterV2'     , ['bnb'])
        , ('0x11111112542d85b3ef69ae05771c2dccff4faa26', 'true', '1inch'     , '1'            , 'AggregationRouterV3'     , ['ethereum','bnb','polygon','arbitrum','optimism'])
        , ('0x1111111254fb6c44bac0bed2854e76f90643097d', 'true', '1inch'     , '1'            , 'AggregationRouterV4'     , ['ethereum','bnb','polygon','arbitrum','avalanche_c','gnosis','fantom'])
        , ('0x1111111254760f7ab3f16433eea9304126dcd199', 'true', '1inch'     , '1'            , 'AggregationRouterV4'     , ['optimism'])
        , ('0x1111111254eeb25477b68fb85ed929f73a960582', 'true', '1inch'     , '1'            , 'AggregationRouterV5'     , ['ethereum','bnb','polygon','arbitrum','avalanche_c','gnosis','optimism','fantom','base'])
        , ('0x6e2b76966cbd9cf4cc2fa0d76d24d5241e0abc2f', 'true', '1inch'     , '1'            , 'AggregationRouterV5'     , ['zksync'])
        , ('0x111111125421ca6dc452d289314280a0f8842a65', 'true', '1inch'     , '1'            , 'AggregationRouterV6'     , ['ethereum','bnb','polygon','arbitrum','avalanche_c','gnosis','optimism','fantom','base','linea','sonic','unichain'])
        , ('0x6fd4383cb451173d5f9304f041c7bcbf27d561ff', 'true', '1inch'     , '1'            , 'AggregationRouterV6'     , ['zksync'])
        , ('0xa88800cd213da5ae406ce248380802bd53b47647', 'true', '1inch'     , '1'            , 'SettlementV1'            , ['ethereum'])
        , ('0x1d0ae300eec4093cee4367c00b228d10a5c7ac63', 'true', '1inch'     , '1'            , 'SettlementV1'            , ['bnb'])
        , ('0x1e8ae092651e7b14e4d0f93611267c5be19b8b9f', 'true', '1inch'     , '1'            , 'SettlementV1'            , ['polygon'])
        , ('0x4bc3e539aaa5b18a82f6cd88dc9ab0e113c63377', 'true', '1inch'     , '1'            , 'SettlementV1'            , ['arbitrum'])
        , ('0xd89adc20c400b6c45086a7f6ab2dca19745b89c2', 'true', '1inch'     , '1'            , 'SettlementV1'            , ['optimism'])
        , ('0x7731f8df999a9441ae10519617c24568dc82f697', 'true', '1inch'     , '1'            , 'SettlementV1'            , ['avalanche_c'])
        , ('0xcbdb7490968d4dbf183c60fc899c2e9fbd445308', 'true', '1inch'     , '1'            , 'SettlementV1'            , ['gnosis'])
        , ('0xa218543cc21ee9388fa1e509f950fd127ca82155', 'true', '1inch'     , '1'            , 'SettlementV1'            , ['fantom'])
        , ('0x7f069df72b7a39bce9806e3afaf579e54d8cf2b9', 'true', '1inch'     , '1'            , 'SettlementV1'            , ['base'])
        , ('0x11de482747d1b39e599f120d526af512dd1a9326', 'true', '1inch'     , '1'            , 'SettlementV1'            , ['zksync'])
        , ('0xfb2809a5314473e1165f6b58018e20ed8f07b840', 'true', '1inch'     , '1'            , 'SettlementV2'            , ['ethereum','bnb','polygon','arbitrum','avalanche_c','gnosis','optimism','fantom','base','linea'])
        , ('0xabd4e5fb590aa132749bbf2a04ea57efbaac399e', 'true', '1inch'     , '1'            , 'SettlementV3'            , ['ethereum','bnb','polygon','arbitrum','avalanche_c','gnosis','optimism','fantom','base','linea','sonic','unichain'])
       , ('0x5c7BCd6E7De5423a257D81B442095A1a6ced35C5', 'true', 'Across'      , '1'         , 'SpokePool'               , ['ethereum'])
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