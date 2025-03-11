{{ config(
        schema = 'uniswap_v3_forks_old_chains',
        alias = 'decoded_factory_events',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'block_month', 'block_number', 'tx_index', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
        )
}}

{%
    set blockchains = uniswap_old_blockchains_list()
%}

with factory_events as (
    {% for blockchain in blockchains %}      
        select 
            '{{blockchain}}' as blockchain,
            * 
        from (
            {{uniswap_v3_factory_event_decoding(
                logs = source(blockchain, 'logs')
            )}}
        )   
        {% if not loop.last or (not is_incremental() and 'optimism' in blockchains) %}
            union all
        {% endif %}
    {% endfor %}
    
    {% if not is_incremental() and 'optimism' in blockchains %}
        -- Old addresses for optimism
        select 
            'optimism' as blockchain,
            token0
            , token1
            , oldaddress as pool
            , 0x1F98431c8aD98523631AE4a59f267346ea31F984 as contract_address
            , 0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118 as factory_topic0
            , 'original uniswap v3 factory event' as factory_info
            , null as block_time
            , null as block_date
            , null as block_month
            , null as block_number
            , null as tx_hash
            , null as tx_from
            , null as tx_to
            , null as tx_index
            , null as evt_index
        from {{ ref('uniswap_optimism_ovm1_pool_mapping') }}
        
        union all
        
        -- New addresses for optimism
        select
            'optimism' as blockchain,
            token0
            , token1
            , newaddress as pool
            , 0x1F98431c8aD98523631AE4a59f267346ea31F984 as contract_address
            , 0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118 as factory_topic0
            , 'original uniswap v3 factory event' as factory_info
            , null as block_time
            , null as block_date
            , null as block_month
            , null as block_number
            , null as tx_hash
            , null as tx_from
            , null as tx_to
            , null as tx_index
            , null as evt_index
        from {{ ref('uniswap_optimism_ovm1_pool_mapping') }}
    {% endif %}
)

select * from factory_events
