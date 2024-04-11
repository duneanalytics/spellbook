{{
  config(
    schema = 'eas',
    alias = 'schema_details',
    materialized = 'view',
    unique_key = ['blockchain', 'project', 'version', 'schema_uid', 'ordinality'],
    post_hook = '{{ expose_spells(\'["arbitrum", "base", "ethereum", "optimism", "polygon", "scroll", "celo"]\',
                                "sector",
                                "attestation",
                                \'["tomfutago"]\') }}'
  )
}}

{%
  set models = [
    ref('eas_arbitrum_schema_details'),
    ref('eas_base_schema_details'),
    ref('eas_ethereum_schema_details'),
    ref('eas_optimism_schema_details'),
    ref('eas_polygon_schema_details'),
    ref('eas_scroll_schema_details'),
    ref('eas_celo_schema_details')
  ]
%}

{% for model in models %}
select
  blockchain,
  project,
  version,
  schema_uid,
  ordinality,
  data_type,
  field_name,
  block_number,
  block_time,
  tx_hash,
  evt_index
from {{ model }}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
