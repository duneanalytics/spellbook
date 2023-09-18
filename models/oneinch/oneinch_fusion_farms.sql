{{
    config(
        schema = 'oneinch',
        alias = alias('fusion_farms'),
        materialized = 'incremental',
        file_format = 'delta',
        unique_key = ['unique_farm_distributor_set_up_id'],
        tags = ['dunesql']
    )
}}



{% set project_start_date = "timestamp '2022-12-25'" %} 
{% set lookback_days = -7 %}



with

delegates as (
    select *
    from (
        select
            block_time as resolver_register_delegatee
            , tx_hash as resolver_register_delegatee_tx_hash
            , substr(data, 13, 20) as resolver_address
        from {{ source('ethereum', 'logs') }}
        where
            topic0 = 0xb2bd819aacce2076359caf6d49d9ac5252134cffdffe026bf4ad781dc3847790 -- RegisterDelegatee
            and contract_address = 0xaccfac2339e16dc80c50d2fa81b5c2b049b4f947 -- 1inch: Delegate Resolver
            {% if is_incremental() %}
                and block_time >= cast(date_add('day', {{ lookback_days }}, now()) as timestamp)
            {% else %}
                and block_time >= {{ project_start_date }}
            {% endif %}

    ) as registrations
    left join (
        select
            block_time as farm_ownership_transferred
            , tx_hash as farm_ownership_transferred_tx_hash
            , contract_address as farm_address
            , max_by(substr(topic2, 13, 20), index) as resolver_address
        from {{ source('ethereum', 'logs') }}
        where
            topic0 = 0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0 -- OwnershipTransferred
            {% if is_incremental() %}
                and block_time >= cast(date_add('day', {{ lookback_days }}, now()) as timestamp)
            {% else %}
                and block_time >= {{ project_start_date }}
            {% endif %}
        group by 1, 2, 3
    ) as ownerships using(resolver_address)
)

, farm_tokens as (
    select
        contract_address as farm_address
        , block_time as farm_created_at
        , substr(data, 45, 20) as default_token
        , tx_hash
    from {{ source('ethereum', 'logs') }}
    where
        topic0 = 0x6bff9ddd187ef283e9c7726f406ab27bcc3719a41b6bee3585c7447183cffcec -- FarmCreated (token, reward)
        {% if is_incremental() %}
            and block_time >= cast(date_add('day', {{ lookback_days }}, now()) as timestamp)
        {% else %}
            and block_time >= {{ project_start_date }}
        {% endif %}
)

, distributors as (
    select
        contract_address as farm_address
        , block_time as set_up
        , max_by(substr(data, 45, 20), index) as distributor
        , max(block_time) over(partition by contract_address) as last_set_up
    from {{ source('ethereum', 'logs') }}
    where
        topic0 = 0xa9f739537fc57540bed0a44e33e27baa63290d865cc15f0f16cf17d38c998a4d -- DistributorChanged
        {% if is_incremental() %}
            and block_time >= cast(date_add('day', {{ lookback_days }}, now()) as timestamp)
        {% else %}
            and block_time >= {{ project_start_date }}
        {% endif %}
    group by 1, 2
)


select 
    *
    , cast(farm_address as varchar)||cast(distributor as varchar)||cast(distributor_status as varchar)||coalesce(cast(farm_default_token as varchar), '')||coalesce(cast(distributor_set_up as varchar), '') as unique_farm_distributor_set_up_id
from (
    select
        resolver_address
        , resolver_name
        , resolver_status
        , last_changed_at
        , kyc
        , resolver_register_delegatee
        , farm_address
        , farm_ownership_transferred
        , farm_created_at
        , default_token as farm_default_token
        , coalesce(distributor, 0x) as distributor
        , if(set_up = last_set_up, 'Current', if(set_up <> last_set_up, 'Legacy', 'No')) as distributor_status
        , set_up as distributor_set_up
    from {{ ref('oneinch_fusion_resolvers') }}
    join delegates using(resolver_address)
    left join distributors using(farm_address)
    left join farm_tokens using(farm_address)
    order by resolver_status, resolver_name, resolver_address, set_up desc
)