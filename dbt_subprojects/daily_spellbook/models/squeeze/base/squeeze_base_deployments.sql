{{ config(
    alias = 'deployments',
    schema = 'squeeze_base',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = 'token'
) }}

{% set project_start_date = '2025-01-01' %}
{% set blockchain = 'base' %}
{% set airlock = '0x660eAaEdEBc968f8f3694354FA8EC0b4c5Ba8D12' %}
{% set integrator = '0x6C61feE73584670AbEd65101946734006DAB12d6' %}

-- Prerequisite: Doppler Airlock ABI decoded on Dune.
-- Confirm actual source schema/table names after submit, then update source().
-- Integrator is on the create() call, NOT on Create event args.

SELECT
    c.call_block_time AS block_time
    , date_trunc('day', c.call_block_time) AS block_date
    , date_trunc('month', c.call_block_time) AS block_month
    , '{{ blockchain }}' AS blockchain
    , 'squeeze' AS project
    , c.output_asset AS token
    , c.output_pool AS pool
    , c.integrator AS integrator
    , c.call_tx_from AS deployer
    , c.call_tx_hash AS tx_hash
FROM {{ source('doppler_base', 'Airlock_call_create') }} c
WHERE c.call_success = true
  AND c.contract_address = {{ airlock }}
  AND c.integrator = {{ integrator }}
  {% if is_incremental() %}
  AND {{ incremental_predicate('c.call_block_time') }}
  {% else %}
  AND c.call_block_time >= TIMESTAMP '{{ project_start_date }}'
  {% endif %}
