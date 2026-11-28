{{ config(
    schema = 'swapwizard_polygon',
    alias = 'trades',
    materialized = 'incremental',
    partition_by = ['block_month'],
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook='{{ expose_spells(
        blockchains = \'["polygon"]\',
        spell_type = "project",
        spell_name = "swapwizard",
        contributors = \'["cmayorga"]\'
    ) }}'
) }}

{{ swapwizard_trades('polygon', '0xc1409502815C4274e2e4F0c5EE3a32d0ce76f2c9') }}
