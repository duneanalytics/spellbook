{{
  config(
    schema = 'eas_optimism',
    alias = 'schemas',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['schema_uid'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

with eas as (
  {{
    eas_schemas(
      blockchain = 'optimism',
      project = 'eas',
      version = '1',
      decoded_project_name = 'attestationstation_v1'
    )
  }}
)

select *
from eas
where schema_uid not in (
  0x026b0f2bfcb9d0e91cb3c2f6c8e84872615c6d5028edec1c9f360a54ae6595d8
  ,0x655a5addcf762e79f17ed35afc9e985a9912d57bd02835d529154954dd07a03d
  ,0x5d2ec81b1de9d7919b34b41de87073a6f97c914b6c143df0aa15d84d5c5ba391
  ,0xed7c89d83e631fc93115ac5cb92260b859dc70ff3ba8d686cd5e5b54800d0137
  ,0x11bd7f94b4ce942bd11d1e02e94a3ab386993845211dc937fd0076e38f0c084e
  ,0x45ca2f188d07f561fa0c4c83ffe204a86780c59cd39b1dcefcea6e64540adde4
)