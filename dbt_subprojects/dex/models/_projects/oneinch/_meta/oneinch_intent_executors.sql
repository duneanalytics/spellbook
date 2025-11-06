{#- it is not incremental, as the source tables are very light and small -#}

{{-
    config(
        schema = 'oneinch',
        alias = 'intent_executors',
        materialized = 'table',
        unique_key = ['blockchain', 'executor_address'],
    )
-}}



with

promotions as (
    select
        chainId as chain_id
        , promoter as resolver_address
        , promotee as executor_address
        , mode as promotion_mode
        , min(evt_block_time) as first_promoted_at
        , max(evt_block_time) as last_promoted_at
    from (
        select promoter, promotee, chainId, evt_block_time, 'intra-chain' as mode
        from {{ source('oneinch_ethereum', 'FusionWhitelistRegistryV1_evt_Promotion') }}
        union all
        select promoter, promotee, chainId, evt_block_time, 'intra-chain' as mode
        from {{ source('oneinch_ethereum', 'FusionWhitelistRegistryV2_evt_Promotion') }}
        union all
        select promoter, promotee, chainId, evt_block_time, 'cross-chain' as mode
        from {{ source('oneinch_ethereum', 'CrosschainWhitelistRegistryV1_evt_Promotion') }}
    )
    group by 1, 2, 3, 4
)

, accesses as (
    select
        blockchain
        , owner as executor_address
        , try(cast(tokenId as bigint)) as token_id
        , mode as access_mode
        , max_by("to", evt_block_time) as last_recipient
        , min(evt_block_time) as first_access_transfer_at
        , max(evt_block_time) as last_access_transfer_at
    from (
        
        {%- for blockchain in oneinch_blockchains_cfg_macro() if blockchain.contracts %}
            -- {{ blockchain.name }} --
            {% for contract, contract_data in blockchain.contracts.items() if contract_data.get('type', '') == 'AccessToken' %}
                -- {{ contract }} --
                select *
                    , '{{ blockchain }}' as blockchain
                    , array["from", "to"] as owners
                    , '{{ contract_data.get("mode", "null") }}' as mode
                from {{ source('oneinch_' + blockchain.name, contract + '_evt_transfer') }}
                {% if not loop.last %}union all{% endif -%}
            {%- endfor -%}
            {%- if not loop.last %}union all{% endif %}
        {% endfor %}
    ), unnest(owners) as o(owner)
    group by 1, 2, 3, 4
)

, unioned as (
    select
        blockchain
        , coalesce(resolver_address, 0x) as address
        , executor_address
        , promotion_mode
        , first_promoted_at
        , last_promoted_at
        , token_id
        , access_mode
        , last_recipient
        , first_access_transfer_at
        , last_access_transfer_at
        , coalesce(last_promoted_at, last_access_transfer_at) as last_time
    from promotions
    left join {{ ref('oneinch_blockchains') }} as meta using(chain_id)
    full join accesses using(blockchain, executor_address)
)

-- output --

select
    blockchain
    , executor_address
    , max_by(address, last_time) as resolver_address
    , max_by(name, last_time) as resolver_name
    , array_agg(distinct name) as resolver_names
    , array_agg(distinct promotion_mode) filter(where promotion_mode is not null) as promotion_modes
    , min(first_promoted_at) as first_promoted_at
    , max(last_promoted_at) as last_promoted_at
    , array_agg(distinct token_id) filter(where executor_address = last_recipient) as access_tokens
    , array_agg(distinct access_mode) filter(where executor_address = last_recipient) as access_modes
    , array_agg(distinct token_id) filter(where executor_address <> last_recipient) as access_tokens_legacy
    , array_agg(distinct access_mode) filter(where executor_address <> last_recipient) as access_modes_legacy
    , min(first_access_transfer_at) as first_access_transfer_at
    , max(last_access_transfer_at) as last_access_transfer_at
from unioned
left join {{ ref('oneinch_intent_resolvers') }} using(address)
where if(address = 0x, array_position(access_token_ids, token_id) > 0, true)
group by 1, 2