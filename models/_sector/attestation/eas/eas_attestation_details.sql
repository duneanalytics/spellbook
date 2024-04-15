{{
  config(
    schema = 'eas',
    alias = 'attestation_details',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'project', 'version', 'schema_uid', 'attestation_uid', 'ordinality'],
    post_hook = '{{ expose_spells(\'["arbitrum", "base", "ethereum", "optimism", "polygon", "scroll", "celo"]\',
                                "sector",
                                "attestation",
                                \'["tomfutago"]\') }}'
  )
}}

{%
  set models = [
    ref('eas_arbitrum_attestation_details'),
    ref('eas_base_attestation_details'),
    ref('eas_ethereum_attestation_details'),
    ref('eas_optimism_attestation_details'),
    ref('eas_polygon_attestation_details'),
    ref('eas_scroll_attestation_details'),
    ref('eas_celo_attestation_details')
  ]
%}

{% for model in models %}
select
  blockchain,
  project,
  version,
  schema_uid,
  attestation_uid,
  ordinality,
  data_type,
  field_name,
  decoded_data,
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
