{{
  config(
    schema = 'polymarket_polygon',
    alias = 'positions_enriched',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['day'],
    unique_key = ['day', 'address', 'token_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

select
    p.day,
    p.address,
    mm.unique_key,
    p.token_id,
    mm.token_outcome,
    mm.token_outcome_name,
    p.balance_raw,
    p.balance,
    mm.question_id,
    mm.question as market_question,
    mm.market_description,
    mm.event_market_name,
    mm.event_market_description,
    mm.polymarket_link
from {{ ref('polymarket_polygon_positions_raw') }} p
inner join {{ ref('polymarket_polygon_market_details') }} mm on p.token_id = mm.token_id
{% if is_incremental() %}
where {{ incremental_predicate('p.day') }}
{% endif %}
