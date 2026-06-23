{{ config(
    schema = 'swapwizard_arbitrum',
    alias = 'trades',
    materialized = 'incremental',
    partition_by = ['block_month'],
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook='{{ expose_spells(
        blockchains = \'["arbitrum"]\',
        spell_type = "project",
        spell_name = "swapwizard",
        contributors = \'["cmayorga"]\'
    ) }}'
) }}

{{ swapwizard_trades('arbitrum', '0xA8E28f3c117B922867c78039adD25f07D23C5f6E') }}
