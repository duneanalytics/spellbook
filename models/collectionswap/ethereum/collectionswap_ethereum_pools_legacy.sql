{{ config(
	tags=['legacy'],
	
        schema='collectionswap_ethereum',
        alias = alias('pools', legacy_model=True),
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['pool_address']
        )
}}

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
    {% if is_incremental() %}
    and call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    union all
    select
    output_pool
    ,'0x0000000000000000000000000000000000000000' AS token_address
    from {{ source('collectionswap_ethereum','CollectionPoolFactory_call_createPoolETH') }}
    where call_success
    {% if is_incremental() %}
    and call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    union all
    select
    output_pool
    ,get_json_object(params, '$.token') AS token_address
    from {{ source('collectionswap_ethereum','CollectionPoolFactory_call_createPoolERC20Filtered') }}
    where call_success
    {% if is_incremental() %}
    and call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    union all
    select
    output_pool
    ,'0x0000000000000000000000000000000000000000' AS token_address
    from {{ source('collectionswap_ethereum','CollectionPoolFactory_call_createPoolETHFiltered') }}
    where call_success
    {% if is_incremental() %}
    and call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
) c
on e.poolAddress = c.output_pool
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}
