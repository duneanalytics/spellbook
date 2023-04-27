{{ config(
        schema='collectionswap_ethereum',
        alias = 'pools'
        )
}}

-- ethereum
WITH pool_info as (
    select
     poolAddress as pool_address
    ,collection as nft_contract_address
    ,token_address
    ,evt_tx_hash as create_tx_hash
    ,evt_block_time as create_block_time
    from {{ source('collectionswap_ethereum','CollectionPoolFactory_evt_NewPool') }} e
    inner join (
        select
        output_pool
        ,get_json_object(params, '$.token') AS token_address
        from {{ source('collectionswap_ethereum','CollectionPoolFactory_call_createPoolERC20') }}
        where call_success
        union all
        select
        output_pool
        ,'0x0000000000000000000000000000000000000000' AS token_address
        from {{ source('collectionswap_ethereum','CollectionPoolFactory_call_createPoolETH') }}
        where call_success
        union all
        select
        output_pool
        ,get_json_object(params, '$.token') AS token_address
        from {{ source('collectionswap_ethereum','CollectionPoolFactory_call_createPoolERC20Filtered') }}
        where call_success
        union all
        select
        output_pool
        ,'0x0000000000000000000000000000000000000000' AS token_address
        from {{ source('collectionswap_ethereum','CollectionPoolFactory_call_createPoolETHFiltered') }}
        where call_success
    ) c
    on e.poolAddress = c.output_pool
)
