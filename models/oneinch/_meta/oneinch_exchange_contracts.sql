{{
    config(
        schema = 'oneinch',
        alias = 'exchange_contracts',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['contract_address', 'blockchain'],
        
    )
}}



with 
    
contracts as (
    select
        project
        , address
        , contract_name
        , blockchain
    from (values
          ('1inch'                , 0xe4c577bdec9ce0f6c54f2f82aed5b1913b71ae2f, 'ExchangeV1'                  , array['ethereum'])
        , ('1inch'                , 0x0000000006adbd7c01bc0738cdbfc3932600ad63, 'ExchangeV2'                  , array['ethereum'])
        , ('1inch'                , 0x0000000053d411becdb4a82d8603edc6d8b8b3bc, 'ExchangeV3'                  , array['ethereum'])
        , ('1inch'                , 0x000005edbbc1f258302add96b5e20d3442e5dd89, 'ExchangeV4'                  , array['ethereum'])
        , ('1inch'                , 0x0000000f8ef4be2b7aed6724e893c1b674b9682d, 'ExchangeV5'                  , array['ethereum'])
        , ('1inch'                , 0x111112549cfedf7822eb11fbd8fd485d8a10f93f, 'ExchangeV6'                  , array['ethereum'])
        , ('1inch'                , 0x111111254b08ceeee8ad6ca827de9952d2a46781, 'ExchangeV7'                  , array['ethereum'])
        , ('1inch'                , 0x3ef51736315f52d568d6d2cf289419b9cfffe782, 'LimitOrderProtocolV1'        , array['ethereum'])
        , ('1inch'                , 0xe3456f4ee65e745a44ec3bcb83d0f2529d1b84eb, 'LimitOrderProtocolV1'        , array['bnb'])
        , ('1inch'                , 0xb707d89d29c189421163515c59e42147371d6857, 'LimitOrderProtocolV1'        , array['polygon', 'optimism'])
        , ('1inch'                , 0xe295ad71242373c37c5fda7b57f26f9ea1088afe, 'LimitOrderProtocolV1'        , array['arbitrum'])
        , ('1inch'                , 0x119c71d3bbac22029622cbaec24854d3d32d2828, 'LimitOrderProtocolV2'        , array['ethereum'])
        , ('1inch'                , 0x1e38eff998df9d3669e32f4ff400031385bf6362, 'LimitOrderProtocolV2'        , array['bnb'])
        , ('1inch'                , 0x94bc2a1c732bcad7343b25af48385fe76e08734f, 'LimitOrderProtocolV2'        , array['polygon'])
        , ('1inch'                , 0x54431918cec22932fcf97e54769f4e00f646690f, 'LimitOrderProtocolV2'        , array['gnosis'])
        , ('1inch'                , 0x7f069df72b7a39bce9806e3afaf579e54d8cf2b9, 'LimitOrderProtocolV2'        , array['arbitrum'])
        , ('1inch'                , 0x0f85a912448279111694f4ba4f85dc641c54b594, 'LimitOrderProtocolV2'        , array['avalanche_c'])
        , ('1inch'                , 0x11431a89893025d2a48dca4eddc396f8c8117187, 'LimitOrderProtocolV2'        , array['optimism'])
        , ('1inch'                , 0x11dee30e710b8d4a8630392781cc3c0046365d4c, 'LimitOrderProtocolV2'        , array['fantom'])
        , ('1inch'                , 0x11111254369792b2ca5d084ab5eea397ca8fa48b, 'AggregationRouterV1'         , array['ethereum'])
        , ('1inch'                , 0x111111125434b319222cdbf8c261674adb56f3ae, 'AggregationRouterV2'         , array['ethereum'])
        , ('1inch'                , 0x111111254bf8547e7183e4bbfc36199f3cedf4a1, 'AggregationRouterV2'         , array['bnb'])
        , ('1inch'                , 0x11111112542d85b3ef69ae05771c2dccff4faa26, 'AggregationRouterV3'         , array['ethereum', 'bnb', 'polygon', 'optimism', 'arbitrum'])
        , ('1inch'                , 0x1111111254fb6c44bac0bed2854e76f90643097d, 'AggregationRouterV4'         , array['ethereum', 'bnb', 'polygon', 'arbitrum', 'avalanche_c', 'gnosis', 'fantom'])
        , ('1inch'                , 0x1111111254760f7ab3f16433eea9304126dcd199, 'AggregationRouterV4'         , array['optimism'])
        , ('1inch'                , 0x1111111254eeb25477b68fb85ed929f73a960582, 'AggregationRouterV5'         , array['ethereum', 'bnb', 'polygon', 'optimism', 'arbitrum', 'avalanche_c', 'gnosis', 'fantom', 'base'])
        , ('1inch'                , 0x6e2b76966cbd9cf4cc2fa0d76d24d5241e0abc2f, 'AggregationRouterV5'         , array['zksync'])
        , ('1inch'                , 0x111111125421ca6dc452d289314280a0f8842a65, 'AggregationRouterV6'         , array['ethereum', 'bnb', 'polygon', 'optimism', 'arbitrum', 'avalanche_c', 'gnosis', 'fantom', 'base'])
        , ('1inch'                , 0x6fd4383cb451173d5f9304f041c7bcbf27d561ff, 'AggregationRouterV6'         , array['zksync'])
        , ('MetaMask'             , 0x881d40237659c251811cec9c364ef91dc08d300c, 'MetaSwap'                    , array['ethereum'])
        , ('CoW Swap'             , 0x9008d19f58aabd9ed0d60971565aa8510560ab41, 'GPv2Settlement'              , array['ethereum', 'gnosis'])
        , ('MetaMask'             , 0x1a1ec25dc08e98e5e93f1104b5e5cdd298707d31, 'MetaSwap'                    , array['bnb', 'polygon', 'avalanche_c'])
        , ('MetaMask'             , 0x9dda6ef3d919c9bc8885d5560999a3640431e8e6, 'MetaSwap'                    , array['arbitrum', 'optimism'])
        , ('Odos'                 , 0x69dd38645f7457be13571a847ffd905f9acbaf6d, 'OdosRouter'                  , array['optimism'])
        , ('Odos'                 , 0x76f4eed9fe41262669d0250b2a97db79712ad855, 'OdosRouter'                  , array['ethereum'])
        , ('Odos'                 , 0xa32ee1c40594249eb3183c10792bcf573d4da47c, 'OdosRouter'                  , array['polygon'])
        , ('Odos'                 , 0xdd94018f54e565dbfc939f7c44a16e163faab331, 'OdosRouter'                  , array['arbitrum'])
        , ('Odos'                 , 0xfe7ce93ac0f78826cd81d506b07fe9f459c00214, 'OdosRouter'                  , array['avalanche_c'])
        , ('OpenOcean'            , 0x6352a56caadc4f1e25cd6c75970fa768a3304e64, 'ExchangeV2'                  , array['bnb', 'avalanche_c', 'ethereum', 'optimism', 'fantom'])
        , ('PancakeSwap'          , 0x05ff2b0db69458a0750badebc4f9e13add608c7f, 'PancakeRouter'               , array['bnb'])
        , ('PancakeSwap'          , 0x10ed43c718714eb63d5aa57b78b54704e256024e, 'PancakeSwapV2'               , array['bnb'])
        , ('PancakeSwap'          , 0x13f4ea83d0bd40e75c8222255bc855a974568dd4, 'SmartRouter'                 , array['ethereum', 'bnb'])
        , ('PancakeSwap'          , 0x2f22e47ca7c5e07f77785f616ceee80c5e84127c, 'SwapSmartRouter'             , array['bnb'])
        , ('PancakeSwap'          , 0xd4c4a7c55c9f7b3c48bafb6e8643ba79f42418df, 'ZapV1'                       , array['bnb'])
        , ('Paraswap'             , 0xdef171fe48cf0115b1d80b88dc8eab59176fee57, 'AugustusSwapperV5'           , array['ethereum', 'bnb', 'fantom', 'optimism', 'arbitrum', 'polygon', 'avalanche_c'])
        , ('Paraswap'             , 0x90249ed4d69d70e709ffcd8bee2c5a566f65dade, 'AugustusSwapperV4'           , array['polygon'])
        , ('Slingshot Finance'    , 0x00c0184c0b5d42fba6b7ca914b31239b419ab80b, 'Swap'                        , array['optimism'])
        , ('Slingshot Finance'    , 0x07e56b727e0eacfa53823977599905024c2de4f0, 'Swap'                        , array['polygon'])
        , ('Slingshot Finance'    , 0x224b239b8bb896f125bd77eb334e302a318d9e33, 'Swap'                        , array['bnb'])
        , ('Slingshot Finance'    , 0x5543550d65813c1fa76242227cbba0a28a297771, 'Swap'                        , array['arbitrum'])
        , ('Slingshot Finance'    , 0xe8c97bf6d084880de38aec1a56d97ed9fdfa0c9b, 'Swap'                        , array['arbitrum'])
        , ('Slingshot Finance'    , 0xf2e4209afa4c3c9eaa3fb8e12eed25d8f328171c, 'TradingContract'             , array['polygon'])
        , ('Trader Joe'           , 0x079c68167f85cb06ed550149cce250e06dc3c52d, 'SwapLogic'                   , array['avalanche_c'])
        , ('Trader Joe'           , 0x60ae616a2155ee3d9a68541ba4544862310933d4, 'JoeRouter02'                 , array['avalanche_c'])
        , ('Trader Joe'           , 0xb4315e873dbcf96ffd0acd8ea43f689d8c20fb30, 'LBRouter'                    , array['bnb', 'arbitrum', 'avalanche_c'])
        , ('Trader Joe'           , 0xed8cbd9f0ce3c6986b22002f03c6475ceb7a6256, 'JoePair'                     , array['avalanche_c'])
        , ('Uniswap'              , 0xf164fc0ec4e93095b804a4795bbe1e041497b92a, 'Router01'                    , array['ethereum'])
        , ('Uniswap'              , 0x7a250d5630b4cf539739df2c5dacb4c659f2488d, 'Router02'                    , array['ethereum'])
        , ('Uniswap'              , 0xe592427a0aece92de3edee1f18e0157c05861564, 'SwapRouter'                  , array['ethereum', 'polygon', 'arbitrum', 'optimism'])
        , ('Uniswap'              , 0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45, 'SwapRouter02'                , array['ethereum', 'polygon', 'arbitrum', 'optimism'])
        , ('Uniswap'              , 0x0000000052be00ba3a005edbe83a0fb9aadb964c, 'UniversalRouter'             , array['ethereum'])
        , ('Uniswap'              , 0xef1c6e67703c7bd7107eed8303fbe6ec2554bf6b, 'UniversalRouter'             , array['ethereum'])
        , ('Uniswap'              , 0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad, 'UniversalRouter'             , array['ethereum', 'bnb', 'polygon', 'arbitrum', 'optimism'])
        , ('Uniswap'              , 0x5dc88340e1c5c6366864ee415d6034cadd1a9897, 'UniversalRouter'             , array['bnb'])
        , ('Uniswap'              , 0x4c60051384bd2d3c01bfc845cf5f4b44bcbe9de5, 'UniversalRouter'             , array['polygon', 'arbitrum'])
        , ('Uniswap'              , 0x643770e279d5d0733f21d6dc03a8efbabf3255b4, 'UniversalRouter'             , array['polygon'])
        , ('Uniswap'              , 0xb555edf5dcf85f42ceef1f3630a52a108e55a654, 'UniversalRouter'             , array['optimism'])
        , ('Uniswap'              , 0xec8b0f7ffe3ae75d7ffab09429e3675bb63503e4, 'UniversalRouter'             , array['bnb', 'arbitrum', 'optimism', 'base'])
        , ('Uniswap'              , 0xb971ef87ede563556b2ed4b1c0b0019111dd85d2, 'UniversalRouter'             , array['bnb'])
        , ('Uniswap'              , 0x2626664c2603336e57b271c5c0b26f421741e481, 'UniversalRouter'             , array['base'])
        , ('Uniswap'              , 0x198ef79f1f515f02dfe9e3115ed9fc07183f02fc, 'UniversalRouter'             , array['base'])
        , ('Uniswap'              , 0x6000da47483062a0d734ba3dc7576ce6a0b645c4, 'UniswapX'                    , array['ethereum'])
        , ('Velodrome'            , 0x9c12939390052919af3155f41bf4160fd3666a6f, 'Router'                      , array['optimism'])
        , ('Velodrome'            , 0xa132dab612db5cb9fc9ac426a0cc215a3423f9c9, 'Router'                      , array['optimism'])
        , ('ZeroEx'               , 0x3f93c3d9304a70c9104642ab8cd37b1e2a7c203a, 'Exchange'                    , array['bnb'])
        , ('ZeroEx'               , 0xdef189deaef76e379df891899eb5a00a94cbc250, 'ExchangeProxy'               , array['fantom'])
        , ('ZeroEx'               , 0xdef1abe32c034e558cdd535791643c58a13acc10, 'ExchangeProxy'               , array['optimism'])
        , ('ZeroEx'               , 0xdef1c0ded9bec7f1a1670819833240f027b25eff, 'ExchangeProxy'               , array['ethereum', 'bnb', 'polygon', 'arbitrum'])
        , ('ZeroEx'               , 0xe66b31678d6c16e9ebf358268a790b763c133750, 'ExchangeProxy'               , array['ethereum'])
    ) as c(project, address, contract_name, blockchains), unnest(blockchains) as blockchains(blockchain)
)

, evms_creation_traces as (
    {% for blockchain in all_evm_chains() %}
        select '{{blockchain}}' as blockchain, address, block_time, "from", tx_hash from {{ source(blockchain, 'traces') }}
        {% if is_incremental() %}
            where {{ incremental_predicate('block_time') }}
        {% endif %}
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
)

, evms_contracts as (
    {% for blockchain in all_evm_chains() %}
        select
            '{{blockchain}}' as blockchain
            , address
            , abi
            , dynamic
            , base
            , factory
            , detection_source
            , namespace
            , created_at
            , name
        from {{ source(blockchain, 'contracts') }}
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
)

, descriptions as ( -- light table, no needs to be incremental
    select
        blockchain
        , address
        , max_by(abi, created_at) as abi
        , max_by(map_from_entries(array[
            ('dynamic', cast(dynamic as varchar))
            , ('base', cast(base as varchar))
            , ('factory', cast(factory as varchar))
            , ('detection_source', detection_source)
        ]), created_at) as params
        , array_agg(namespace) as namespaces
        , array_agg(name) as names
    from evms_contracts
    join contracts using(blockchain, address)
    group by 1, 2
)

, creations as (
    select
        project
        , address
        , contract_name
        , blockchain
        , abi
        , params
        , namespaces
        , names
        , max(block_time) as last_created_at
        , max(evms_creation_traces."from") as last_creator
        , max(tx_hash) as last_creation_tx_hash
    from evms_creation_traces
    join contracts using(blockchain, address)
    left join descriptions using(blockchain, address)
    group by 1, 2, 3, 4, 5, 6, 7, 8
)


select
      project
    , address as contract_address
    , substr(address, length(address) - 1) as contract_id
    , contract_name
    , blockchain
    , last_created_at
    , last_creator
    , last_creation_tx_hash
    , abi
    , params
    , namespaces
    , names
from creations
