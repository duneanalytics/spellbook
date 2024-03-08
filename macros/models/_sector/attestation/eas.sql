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
  se.ordinality,
  se.element[1] as data_type,
  se.element[2] as field_name,
  block_number,
  block_time,
  tx_hash,
  evt_index
from {{ ref(project ~ '_' ~ blockchain ~ '_schemas') }} sr
  cross join unnest(sr.schema_array) with ordinality as se (element, ordinality)
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
),

src_EAS_call_attest as (
  select
    *,
    replace(replace(replace(request, '\"', '"'), '"{', '{'), '}"', '}') as clean_request
  from {{ source(decoded_project_name ~ '_' ~ blockchain, 'EAS_call_attest') }}
),

src_EAS_evt_Revoked as (
  select *
  from {{ source(decoded_project_name ~ '_' ~ blockchain, 'EAS_evt_Revoked') }}
)

select distinct
  '{{ blockchain }}' as blockchain,
  '{{ project }}' as project,
  '{{ version }}' as version,
  ea.{{ schema_column_name }} as schema_uid,
  ea.uid as attestation_uid,
  ea.attester,
  ea.recipient,
  ca.request,
  try_cast(json_query(ca.clean_request, 'lax $.data[*].revocable' omit quotes) as boolean) as is_revocable,
  from_hex(json_query(ca.clean_request, 'lax $.data[*].refUID' omit quotes)) as ref_uid,
  json_query(ca.clean_request, 'lax $.data[*].data' omit quotes) as raw_data,
  try_cast(json_query(ca.clean_request, 'lax $.data[*].value' omit quotes) as uint256) as raw_value,
  cast(
    if(
      json_query(ca.clean_request, 'lax $.data[*].expirationTime' omit quotes) <> '0',
      from_unixtime(try_cast(json_query(ca.clean_request, 'lax $.data[*].expirationTime' omit quotes) as bigint))
    ) as timestamp
  ) as expiration_time,
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
  and ( {{ incremental_predicate('ea.evt_block_time') }}
     or {{ incremental_predicate('er.evt_block_time') }} )
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

with

schema_details as (
  select
    *,
    concat(
      case
        when data_type like '%int%' then 'int' -- simplify
        else data_type
      end,
      '-',
      cast(row_number() over (partition by schema_uid, data_type order by ordinality) as varchar)
    ) as data_type_join
  from {{ ref(project ~ '_' ~ blockchain ~ '_schema_details') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('block_time') }}
  {% endif %}
),

attestations as (
  select *
  from {{ ref(project ~ '_' ~ blockchain ~ '_attestations') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('block_time') }}
  {% endif %}
),

attestations_unnested as (
  select
    schema_uid,
    attestation_uid,
    ordinality,
    substring(raw_data from 3 + (64 * (ordinality - 1)) for 64) as chunk
  from attestations a
    cross join unnest(sequence(1, floor(length(raw_data) / 64))) as t(ordinality)
),

attestations_decoded as (
  select
    schema_uid,
    attestation_uid,
    ordinality,
    --chunk,
    case
      when try_cast(bytearray_to_uint256(from_hex(chunk)) as int) in (0, 1) then 'bool'
      when try_cast(bytearray_to_uint256(from_hex(chunk)) as uint256) <= 999999999999999999 then 'int'
      -- '[[:print:]]+[^\x00-\x7F]+[[:print:]]*' --> contains non-human readable characters?
      when regexp_like(from_utf8(from_hex(chunk)), '[[:print:]]+[^\x00-\x7F]+[[:print:]]*') then
        case
          when length(regexp_replace(chunk, '^0+', '')) <= 40 then 'address'
          else 'bytes32'
        end
      when regexp_like(from_utf8(from_hex(chunk)), '[[:print:]]+[^\x00-\x7F]+[[:print:]]*') = false then 'string' -- or 'bytes32'
      else 'unknown'
    end as data_type,
    case
      when try_cast(bytearray_to_uint256(from_hex(chunk)) as int) in (0, 1)
        then cast(try_cast(try_cast(bytearray_to_uint256(from_hex(chunk)) as int) as boolean) as varchar) -- boolean
      when try_cast(bytearray_to_uint256(from_hex(chunk)) as uint256) <= 999999999999999999
        then try_cast(bytearray_to_uint256(from_hex(chunk)) as varchar) -- int
      -- '[[:print:]]+[^\x00-\x7F]+[[:print:]]*' --> contains non-human readable characters?
      when regexp_like(from_utf8(from_hex(chunk)), '[[:print:]]+[^\x00-\x7F]+[[:print:]]*') then
        case
          when length(regexp_replace(chunk, '^0+', '')) <= 40 then try_cast(concat('0x', regexp_replace(chunk, '^0{1,24}', '')) as varchar) -- address
          else try_cast(concat('0x', chunk) as varchar) -- bytes32
        end
      when regexp_like(from_utf8(from_hex(chunk)), '[[:print:]]+[^\x00-\x7F]+[[:print:]]*') = false
        then try_cast(from_utf8(from_hex(chunk)) as varchar) -- string
      else cast(chunk as varchar) -- anything else
    end as decoded_data
  from attestations_unnested
),

attestations_ext as (
  select
    *,
    lag(data_type) over (partition by schema_uid, attestation_uid order by ordinality) as prev_data_type,
    lead(data_type) over (partition by schema_uid, attestation_uid order by ordinality) as next_data_type
  from attestations_decoded
),

attestations_sorted as (
  select
    *,
    sum(case when data_type != prev_data_type then 1 else 0 end ) over (partition by schema_uid, attestation_uid order by ordinality) as group_id
  from attestations_ext
),

attestations_grouped as (
  select
    schema_uid,
    attestation_uid,
    data_type,
    group_id,
    array_join(array_agg(decoded_data order by ordinality), '') as decoded_data,
    concat(
      case
        when data_type like '%int%' then 'int' -- simply as non-trivial to "guess" int size based on decoded data chunk
        else data_type
      end,
      '-',
      cast(dense_rank() over (partition by schema_uid, attestation_uid, data_type order by group_id) as varchar)
    ) as data_type_join
  from attestations_sorted
  where 1 = 1
    and not (data_type = 'int' and cast(decoded_data as bigint) % 64 = 0) -- exclude bytesize markers
    and not (data_type = 'int' and next_data_type = 'string') -- exclude string length markers
  group by 1,2,3,4
)

select
  sd.blockchain,
  sd.project,
  sd.version,
  sd.schema_uid,
  a.attestation_uid,
  sd.ordinality,
  sd.data_type,
  sd.field_name,
  ag.decoded_data,
  a.is_revoked,
  a.block_number,
  a.block_time,
  a.tx_hash,
  a.evt_index
from schema_details sd
  join attestations a on sd.schema_uid = a.schema_uid -- only schemas with online attestations
  left join attestations_grouped ag
     on a.schema_uid = ag.schema_uid
    and a.attestation_uid = ag.attestation_uid
    and sd.data_type_join = ag.data_type_join

{% endmacro %}
