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
  cross join unnest(split(sr.schema, ',')) with ordinality as se (element, ordinality_id)

{% endmacro %}

{# ######################################################################### #}
