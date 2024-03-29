{{
    config(
        schema='boost',
        alias='deployed',
        materialized='incremental',
        file_format='delta',
        incremental_strategy='merge',
        unique_key=['boost_address', 'boost_id'],
        incremental_predicates=[incremental_predicate('DBT_INTERNAL_DEST.creation_time')]
    )
}}

{% set boost_deployed_models = [
    ref('boost_arbitrum_deployed'),
    ref('boost_base_deployed'),
    ref('boost_ethereum_deployed'),
    ref('boost_optimism_deployed'),
    ref('boost_polygon_deployed'),
] %}

select
    reward_network,
    boost_address,
    boost_id,
    boost_name,
    action_type,
    action_network,
    project_name,
    boost_type,
    TRY(from_unixtime(start_time)) as start_time,
    TRY(from_unixtime(end_time)) as end_time,
    reward_amount_raw,
    reward_token_address,
    cast(max_participants as int) as max_participants,
    creation_time,
    creator as creator_address
from
  (
    {% for model in boost_deployed_models %}
    select *
    from {{ model }}
    {% if is_incremental() %}
    where
        {{ incremental_predicate('creation_time') }}
    {% endif %}
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)
where 
    creator <> 0xa4c8bb4658bc44bac430699c8b7b13dab28e0f4e
    and start_time > 0
    and end_time < 1e11
