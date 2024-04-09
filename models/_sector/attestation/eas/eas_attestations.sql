{{
  config(
    schema = 'eas',
    alias = 'attestations',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'project', 'version', 'schema_uid', 'attestation_uid'],
    post_hook = '{{ expose_spells(\'["arbitrum", "base", "ethereum", "optimism", "polygon", "scroll", "celo"]\',
                                "sector",
                                "attestation",
                                \'["tomfutago"]\') }}'
  )
}}

{%
  set models = [
    ref('eas_arbitrum_attestations'),
    ref('eas_base_attestations'),
    ref('eas_ethereum_attestations'),
    ref('eas_optimism_attestations'),
    ref('eas_polygon_attestations'),
    ref('eas_scroll_attestations'),
    ref('eas_celo_attestations')
  ]
%}

{% for model in models %}
select
  blockchain,
  project,
  version,
  schema_uid,
  attestation_uid,
  attester,
  recipient,
  request,
  is_revocable,
  ref_uid,
  raw_data,
  raw_value,
  expiration_time,
  revocation_time,
  attestation_state,
  is_revoked,
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
