{{
    config(
        schema = 'oneinch',
        alias = 'fusion_farms',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['resolver_address', 'farm_address'],
    )
}}



{% set project_start_date = "timestamp '2022-12-25'" %} 



with

delegates as (
    select *
    from (
        select
            block_time as resolver_registered_delegatee_at
            , tx_hash as resolver_register_delegatee_tx_hash
            , substr(data, 13, 20) as resolver_address
        from {{ source('ethereum', 'logs') }}
        where
            topic0 = 0xb2bd819aacce2076359caf6d49d9ac5252134cffdffe026bf4ad781dc3847790 -- RegisterDelegatee
            and contract_address = 0xaccfac2339e16dc80c50d2fa81b5c2b049b4f947 -- 1inch: Delegate Resolver
            {% if is_incremental() %}
                and {{ incremental_predicate('block_time') }}
            {% else %}
                and block_time >= {{ project_start_date }}
            {% endif %}
    ) as registrations
    left join (
        select
            block_time as farm_ownership_transferred_at
            , tx_hash as farm_ownership_transferred_tx_hash
            , contract_address as farm_address
            , max_by(substr(topic2, 13, 20), index) as resolver_address
        from {{ source('ethereum', 'logs') }}
        where
            topic0 = 0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0 -- OwnershipTransferred
            {% if is_incremental() %}
                and {{ incremental_predicate('block_time') }}
            {% else %}
                and block_time >= {{ project_start_date }}
            {% endif %}
        group by 1, 2, 3
    ) as ownerships using(resolver_address)
)

, farm_tokens as (
    select
        contract_address as farm_address
        , max(block_time) as farm_last_created_at
        , max_by(substr(data, 45, 20), block_time) as farm_last_default_token
        , max_by(tx_hash, block_time) as farm_last_creation_tx_hash
    from {{ source('ethereum', 'logs') }}
    where
        topic0 = 0x6bff9ddd187ef283e9c7726f406ab27bcc3719a41b6bee3585c7447183cffcec -- FarmCreated (token, reward)
        {% if is_incremental() %}
            and {{ incremental_predicate('block_time') }}
        {% else %}
            and block_time >= {{ project_start_date }}
        {% endif %}
    group by 1
)

, distributors as (
    select
        contract_address as farm_address
        , max_by(substr(data, 45, 20), (block_time, index)) as farm_last_distributor
        , max(block_time) as farm_last_distributor_set_up_at
    from {{ source('ethereum', 'logs') }}
    where
        topic0 = 0xa9f739537fc57540bed0a44e33e27baa63290d865cc15f0f16cf17d38c998a4d -- DistributorChanged
        {% if is_incremental() %}
            and {{ incremental_predicate('block_time') }}
        {% else %}
            and block_time >= {{ project_start_date }}
        {% endif %}
    group by 1
)


select
    fr.address as resolver_address
    , fr.name as resolver_name
    , fr.status as resolver_status
    , fr.last_changed_at as resolver_last_changed_at
    , fr.kyc as resolver_kyc
    , resolver_registered_delegatee_at
    , farm_address
    , farm_ownership_transferred_at
    , farm_last_created_at
    , farm_last_default_token
    , farm_last_distributor
    , farm_last_distributor_set_up_at
from {{ ref('oneinch_fusion_resolvers') }} as fr
join delegates on fr.address = delegates.resolver_address
left join distributors using(farm_address)
left join farm_tokens using(farm_address)
order by resolver_status, resolver_name, resolver_address