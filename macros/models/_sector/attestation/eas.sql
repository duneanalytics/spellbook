{%
  macro eas_schemas(
    blockchain = '',
    project = 'eas',
    version = '',
    decoded_project_name = '',
    uid_column_name = 'uid'
  )
%}

{% set decoded_project_name = project if decoded_project_name == '' else decoded_project_name %}

with

src_SchemaRegistry_evt_Registered as (
  select *
  from {{ source(decoded_project_name ~ '_' ~ blockchain, 'SchemaRegistry_evt_Registered') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

src_SchemaRegistry_call_register as (
  select *
  from {{ source(decoded_project_name ~ '_' ~ blockchain, 'SchemaRegistry_call_register') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('call_block_time') }}
  {% endif %}
)

select
  '{{ blockchain }}' as blockchain,
  '{{ project }}' as project,
  '{{ version }}' as version,
  er.{{ uid_column_name }} as schema_uid,
  er.registerer,
  cr.resolver,
  cr.schema,
  transform(split(cr.schema, ','), x -> split(trim(x), ' ')) as schema_array, -- array of 2-element arrays [[data_type,field_name],[...]]
  cr.revocable as is_revocable,
  er.contract_address,
  er.evt_block_number as block_number,
  er.evt_block_time as block_time,
  er.evt_tx_hash as tx_hash,
  er.evt_index
from src_SchemaRegistry_evt_Registered er
  join src_SchemaRegistry_call_register cr on er.evt_tx_hash = cr.call_tx_hash
where cr.call_success

{% endmacro %}

{# ######################################################################### #}

{%
  macro eas_schema_details(
    blockchain = '',
    project = 'eas',
    version = ''
  )
%}

select
  sr.blockchain,
  sr.project,
  sr.version,
  sr.schema_uid,
  se.ordinality_id,
  se.element[1] as data_type,
  se.element[2] as field_name
from {{ ref(project ~ '_' ~ blockchain ~ '_schemas') }} sr
  cross join unnest(sr.schema_array) with ordinality as se (element, ordinality_id)
where cardinality(se.element) = 2 -- only inlcude valid schemas

{% endmacro %}

{# ######################################################################### #}

{%
  macro eas_attestations(
    blockchain = '',
    project = 'eas',
    version = '',
    decoded_project_name = '',
    schema_column_name = 'schema'
  )
%}

{% set decoded_project_name = project if decoded_project_name == '' else decoded_project_name %}

with

src_EAS_evt_Attested as (
  select *
  from {{ source(decoded_project_name ~ '_' ~ blockchain, 'EAS_evt_Attested') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

src_EAS_call_attest as (
  select
    *,
    replace(replace(replace(request, '\"', '"'), '"{', '{'), '}"', '}') as clean_request
  from {{ source(decoded_project_name ~ '_' ~ blockchain, 'EAS_call_attest') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('call_block_time') }}
  {% endif %}
),

src_EAS_evt_Revoked as (
  select *
  from {{ source(decoded_project_name ~ '_' ~ blockchain, 'EAS_evt_Revoked') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
)

select
  '{{ blockchain }}' as blockchain,
  '{{ project }}' as project,
  '{{ version }}' as version,
  ea.{{ schema_column_name }} as schema_uid,
  ea.uid as attestation_uid,
  ea.attester,
  ea.recipient,
  ca.request,
  json_query(ca.clean_request, 'lax $.data[*].revocable' omit quotes) as is_revocable,
  json_query(ca.clean_request, 'lax $.data[*].refUID' omit quotes) as ref_uid,
  json_query(ca.clean_request, 'lax $.data[*].data' omit quotes) as raw_data,
  json_query(ca.clean_request, 'lax $.data[*].value' omit quotes) as raw_value,
  json_query(ca.clean_request, 'lax $.data[*].expirationTime' omit quotes) as expiration_time,
  cast(er.evt_block_time as timestamp) as revocation_time,
  if(er.evt_block_time is not null, 'revoked', 'valid') as attestation_state,
  if(er.evt_block_time is not null, true, false) as is_revoked,
  ea.contract_address,
  ea.evt_block_number as block_number,
  ea.evt_block_time as block_time, -- attestation created
  ea.evt_tx_hash as tx_hash,
  ea.evt_index
from src_EAS_evt_Attested ea
  join src_EAS_call_attest ca on ea.evt_tx_hash = ca.call_tx_hash and ea.uid = ca.output_0
  left join src_EAS_evt_Revoked er on ea.{{ schema_column_name }} = er.{{ schema_column_name }} and ea.uid = er.uid
where ca.call_success

{% if is_incremental() %}
union all

select
  a.blockchain,
  a.project,
  a.version,
  a.schema_uid,
  a.attestation_uid,
  a.attester,
  a.recipient,
  a.request,
  a.is_revocable,
  a.ref_uid,
  a.raw_data,
  a.raw_value,
  a.expiration_time,
  er.evt_block_time as revocation_time,
  'revoked' as attestation_state,
  true as is_revoked,
  a.contract_address,
  a.block_number,
  a.block_time, -- attestation created
  a.tx_hash,
  a.evt_index
from src_EAS_evt_Revoked er
  join {{ this }} a on er.{{ schema_column_name }} = a.schema_uid and er.uid = a.attestation_uid -- checking against main model to backfill data
  left join src_EAS_evt_Attested ea on er.evt_tx_hash = ea.evt_tx_hash -- skip records included in this load increment (top select in union all)
where ea.evt_tx_hash is null
{% endif %}

{% endmacro %}

{# ######################################################################### #}

{%
  macro eas_attestation_details(
    blockchain = '',
    project = 'eas',
    version = ''
  )
%}

select
  sd.blockchain,
  sd.project,
  sd.version,
  sd.schema_uid,
  sd.ordinality_id,
  sd.data_type,
  sd.field_name,
  a.attestation_uid,
  a.raw_data,
  a.block_number,
  a.block_time,
  a.tx_hash,
  a.evt_index
from {{ ref(project ~ '_' ~ blockchain ~ '_schema_details') }} sd
  join {{ ref(project ~ '_' ~ blockchain ~ '_attestations') }} a on sd.schema_uid = a.schema_uid
{% if is_incremental() %}
where {{ incremental_predicate('block_time') }}
{% endif %}

{% endmacro %}
