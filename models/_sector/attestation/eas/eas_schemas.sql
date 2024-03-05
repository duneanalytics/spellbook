{{
  config(
    schema = 'eas',
    alias = 'schemas',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'project', 'version', 'schema_uid'],
    post_hook = '{{ expose_spells(\'["arbitrum", "base", "ethereum", "optimism", "polygon", "scroll"]\',
                                "sector",
                                "attestation",
                                \'["tomfutago"]\') }}'
  )
}}

{%
  set models = [
    ref('eas_arbitrum_schemas'),
    ref('eas_base_schemas'),
    ref('eas_ethereum_schemas'),
    ref('eas_optimism_schemas'),
    ref('eas_polygon_schemas'),
    ref('eas_scroll_schemas')
  ]
%}

{% for model in models %}
select
  blockchain,
  project,
  version,
  schema_uid,
  registerer,
  resolver,
  schema,
  schema_array,
  is_revocable,
  contract_address,
  block_number,
  block_time,
  tx_hash,
  evt_index
from {{ model }}
{% if is_incremental() %}
where {{ incremental_predicate('block_time') }}
{% endif %}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
